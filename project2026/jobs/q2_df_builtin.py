from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from common import (
    Timer,
    build_personalization,
    create_spark,
    join_hdfs_path,
    write_text_hdfs,
)

#Earth radius for Haversine formula
EARTH_RADIUS_KM = 6371.0

#km and miles conversion
KM_TO_MILES = 0.621371
MILES_TO_KM = 1.609344

#Bounding box for NYC area (step 3 of Q2).
#limits to cover all 5 boroughs + airports while filtering out wrong GPS coordinates
MIN_LON = -74.30
MAX_LON = -73.60
MIN_LAT = 40.45
MAX_LAT = 40.95


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Q2 duration/speed/congestion analysis using DataFrame built-in functions."
    )
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "errorifexists"])
    return parser.parse_args()


def make_json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, Row):
        return {
            key: make_json_safe(val)
            for key, val in value.asDict(recursive=True).items()
        }

    if isinstance(value, list):
        return [make_json_safe(item) for item in value]

    if isinstance(value, dict):
        return {
            key: make_json_safe(val)
            for key, val in value.items()
        }

    return value


def rows_to_dicts(rows: list[Row]) -> list[dict[str, Any]]:
    return [
        make_json_safe(row.asDict(recursive=True))
        for row in rows
    ]


def write_csv(df: DataFrame, output_path: str, mode: str) -> None:
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", "true")
        .csv(output_path)
    )


def load_trips(spark, input_base: str) -> DataFrame:
    #load the 2015 yellow taxi data from HDFS parquet
    parquet_path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2015")
    return spark.read.parquet(parquet_path)


def filter_nyc_coords(df: DataFrame) -> DataFrame:
    #Keep only rows where both pickup AND dropoff fall inside of NYC bounding box
    return df.filter(
        (F.col("pickup_longitude").between(MIN_LON, MAX_LON)) &
        (F.col("dropoff_longitude").between(MIN_LON, MAX_LON)) &
        (F.col("pickup_latitude").between(MIN_LAT, MAX_LAT)) &
        (F.col("dropoff_latitude").between(MIN_LAT, MAX_LAT))
    )


def add_haversine(df: DataFrame) -> DataFrame:
    #Haversine formula using Spark builtin functions
    #a = sin^2(Δlat/2) + cos(lat1)*cos(lat2)*sin²(Δlon/2)
    #c = 2 * atan2(ριζα[a], ριζα[(1-a)])
    #d = R * c

    #Step 1: convert degrees to radians
    lat1 = F.radians(F.col("pickup_latitude"))
    lon1 = F.radians(F.col("pickup_longitude"))
    lat2 = F.radians(F.col("dropoff_latitude"))
    lon2 = F.radians(F.col("dropoff_longitude"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    #Step 2: compute 'a' (the square of half the chord length)
    a = (
        F.pow(F.sin(dlat / F.lit(2.0)), 2.0) +
        F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / F.lit(2.0)), 2.0)
    )

    #Clamp to [0, 1] to guard against tiny floating point errors
    a_safe = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))

    # tep 3: angular distance c, then multiply by Earth radius
    c = F.lit(2.0) * F.atan2(F.sqrt(a_safe), F.sqrt(F.lit(1.0) - a_safe))

    return (
        df
        .withColumn("haversine_km", F.lit(EARTH_RADIUS_KM) * c)
        .withColumn("haversine_mi", F.col("haversine_km") * F.lit(KM_TO_MILES))
    )


def prepare_trips(df: DataFrame, hours: list[int]) -> DataFrame:
    #Step 1-3: add derived time/distance columns
    base = (
        df
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        #Trip duration in minutes and hours
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
        .withColumn("duration_hours", F.col("duration_minutes") / F.lit(60.0))
        #Raw trip_distance is in miles then convert to km for metric output
        .withColumn("trip_distance_km", F.col("trip_distance") * F.lit(MILES_TO_KM))
    )

    #Step 1 and 2: drop wrong records
    clean = (
        base
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("pickup_ts") <= F.col("dropoff_ts"))   #dropoff after pickup
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("pickup_hour").isin(hours))            #personalized hours
        .filter(F.col("pickup_longitude").isNotNull())
        .filter(F.col("pickup_latitude").isNotNull())
        .filter(F.col("dropoff_longitude").isNotNull())
        .filter(F.col("dropoff_latitude").isNotNull())
    )

    #Step 3: remove coordinates outside the NYC area
    clean = filter_nyc_coords(clean)

    #Step 4: compute Haversine distance using builtin Spark functions
    with_haversine = add_haversine(clean)

    #Step 4 + Step 5: derived metrics
    return (
        with_haversine
        #Speed in km/h: distance_km / duration_hours
        .withColumn("speed_kmh", F.col("trip_distance_km") / F.col("duration_hours"))
        .withColumn("distance_gap_km", F.col("trip_distance_km") - F.col("haversine_km"))
        #Detour ratio > 1 means the driver took a longer route than the straight line
        .withColumn(
            "detour_ratio",
            F.when(F.col("haversine_km") > 0, F.col("trip_distance_km") / F.col("haversine_km"))
             .otherwise(F.lit(None).cast("double")),
        )
        #Minutes spent per km — higher = slower trip
        .withColumn("duration_per_km", F.col("duration_minutes") / F.col("trip_distance_km"))
        #Step 5: congestion flag (speed < 10 mph = ~16.09 km/h AND distance >= 1 mile = 1.609 km)
        .withColumn(
            "congestion_candidate",
            ((F.col("speed_kmh") < 16.09344) & (F.col("trip_distance_km") >= 1.609344)).cast("int"),
        )
    )


def aggregate_by_hour(trips: DataFrame) -> DataFrame:
    #Step 6: group by pickup hour and compute all required statistics
    return (
        trips
        .groupBy("pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("duration_minutes"), 4).alias("avg_duration_minutes"),
            #Median and p90 use percentile_approx
            F.round(F.expr("percentile_approx(duration_minutes, 0.5)"), 4).alias("median_duration_minutes"),
            F.round(F.expr("percentile_approx(duration_minutes, 0.9)"), 4).alias("p90_duration_minutes"),
            #Average speed per trip
            F.round(F.avg("speed_kmh"), 4).alias("avg_speed_kmh"),
            #Aggregate speed = total km / total hours (by distance, no per trip avg)
            F.round((F.sum("trip_distance_km") / F.sum("duration_hours")), 4).alias("agg_speed_kmh"),
            F.round(F.avg("haversine_km"), 4).alias("avg_haversine_km"),
            F.round(F.avg("distance_gap_km"), 4).alias("avg_distance_gap_km"),
            F.round(F.avg("detour_ratio"), 4).alias("avg_detour_ratio"),
            #Share of trips flagged as congestion candidates (0 and 1)
            F.round(F.avg("congestion_candidate"), 6).alias("congestion_candidate_share"),
        )
        .orderBy("pickup_hour")
    )


def get_top5_slowest(trips: DataFrame) -> DataFrame:
    #Step 7: top 5 trips with the highest duration per km
    return (
        trips
        .select(
            "pickup_ts", "dropoff_ts", "pickup_hour",
            "trip_distance_km", "duration_minutes", "duration_per_km",
            "speed_kmh", "haversine_km", "distance_gap_km", "detour_ratio",
            "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
        )
        .orderBy(F.desc("duration_per_km"))
        .limit(5)
    )


def get_top5_fastest(trips: DataFrame) -> DataFrame:
    #Step 7: top 5 trips with the highest speed in km/h
    return (
        trips
        .select(
            "pickup_ts", "dropoff_ts", "pickup_hour",
            "trip_distance_km", "duration_minutes", "duration_per_km",
            "speed_kmh", "haversine_km", "distance_gap_km", "detour_ratio",
            "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
        )
        .orderBy(F.desc("speed_kmh"))
        .limit(5)
    )


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark("q2_df_builtin")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        #Load raw data and count before filtering
        raw_df = load_trips(spark, args.input_base)
        raw_count = raw_df.count()

        #Apply all filters and compute columns and cache cause query it multiple times
        trips = prepare_trips(raw_df, personalization.hours).cache()
        filtered_count = trips.count()

        #Compute the three result tables
        by_hour      = aggregate_by_hour(trips)
        top5_slowest = get_top5_slowest(trips)
        top5_fastest = get_top5_fastest(trips)

        #Output paths
        by_hour_path = join_hdfs_path(args.output_base, "results", "tables", "q2", "df_builtin_by_hour")
        slowest_path = join_hdfs_path(args.output_base, "results", "tables", "q2", "df_builtin_slowest_per_km")
        fastest_path = join_hdfs_path(args.output_base, "results", "tables", "q2", "df_builtin_fastest")
        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q2_df_builtin_metrics.json")
        plan_path    = join_hdfs_path(args.output_base, "results", "plans",   "q2_df_builtin_plan.txt")

        #Write CSVs
        write_csv(by_hour,       by_hour_path, args.mode)
        write_csv(top5_slowest,  slowest_path, args.mode)
        write_csv(top5_fastest,  fastest_path, args.mode)

        #Save the execution plan for the report
        plan_text = by_hour._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        #Build a metrics dict with everything for report
        metrics = {
            "job": "q2_df_builtin",
            "implementation": "DataFrame API without UDF",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "hours": personalization.hours,
                "top_k": personalization.top_k,
            },
            "nyc_bounding_box": {
                "min_lon": MIN_LON,
                "max_lon": MAX_LON,
                "min_lat": MIN_LAT,
                "max_lat": MAX_LAT,
            },
            "raw_row_count": raw_count,
            "filtered_row_count": filtered_count,
            "output_paths": {
                "by_hour": by_hour_path,
                "slowest_per_km": slowest_path,
                "fastest": fastest_path,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "by_hour_preview": rows_to_dicts(by_hour.collect()),
            "slowest_per_km_preview": rows_to_dicts(top5_slowest.collect()),
            "fastest_preview": rows_to_dicts(top5_fastest.collect()),
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

        trips.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()