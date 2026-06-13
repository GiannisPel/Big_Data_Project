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
    schema_2015_raw,
    write_text_hdfs,
)

#Constants used in the distance calculations
EARTH_RADIUS_KM = 6371.0
KM_TO_MILES = 0.621371
MILES_TO_KM = 1.609344

#Simple bounding box for the wider NYC area
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

    #needed because Q2 builtin must run once on CSV and once on Parquet
    parser.add_argument("--input-format", default="parquet", choices=["csv", "parquet"])

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


def normalize_2015_csv(raw: DataFrame) -> DataFrame:

    #rename them to the same names used in the prepared Parquet dataset
    return (
        raw
        .select(
            F.col("VendorID").cast("int").alias("vendor_id"),
            F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss").alias("pickup_ts"),
            F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss").alias("dropoff_ts"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("pickup_longitude").cast("double").alias("pickup_longitude"),
            F.col("pickup_latitude").cast("double").alias("pickup_latitude"),
            F.col("RateCodeID").cast("int").alias("rate_code_id"),
            F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
            F.col("dropoff_longitude").cast("double").alias("dropoff_longitude"),
            F.col("dropoff_latitude").cast("double").alias("dropoff_latitude"),
            F.col("payment_type").cast("int").alias("payment_type"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("extra").cast("double").alias("extra"),
            F.col("mta_tax").cast("double").alias("mta_tax"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("tolls_amount").cast("double").alias("tolls_amount"),
            F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
            F.col("total_amount").cast("double").alias("total_amount"),
        )
    )


def load_trips(spark, input_base: str, input_format: str) -> DataFrame:
    #For Parquet version, input_base is the project HDFS base
    #For  CSV version, input_base is the raw /data HDFS base
    if input_format == "parquet":
        parquet_path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2015")
        return spark.read.parquet(parquet_path)

    csv_path = join_hdfs_path(input_base, "yellow_tripdata_2015.csv")

    raw = (
        spark.read
        .option("header", "true")
        .schema(schema_2015_raw())
        .csv(csv_path)
    )

    return normalize_2015_csv(raw)


def filter_nyc_coords(df: DataFrame) -> DataFrame:
    #keep only trips where pickup and dropoff coordinates are inside the NYC area
    return df.filter(
        (F.col("pickup_longitude").between(MIN_LON, MAX_LON)) &
        (F.col("dropoff_longitude").between(MIN_LON, MAX_LON)) &
        (F.col("pickup_latitude").between(MIN_LAT, MAX_LAT)) &
        (F.col("dropoff_latitude").between(MIN_LAT, MAX_LAT))
    )


def add_haversine(df: DataFrame) -> DataFrame:
    lat1 = F.radians(F.col("pickup_latitude"))
    lon1 = F.radians(F.col("pickup_longitude"))
    lat2 = F.radians(F.col("dropoff_latitude"))
    lon2 = F.radians(F.col("dropoff_longitude"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    #Haversine
    #a = sin^2(dlat/2) + cos(lat1) * cos(lat2) * sin^2(dlon/2)
    a = (
        F.pow(F.sin(dlat / F.lit(2.0)), 2.0) +
        F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / F.lit(2.0)), 2.0)
    )

   
    a_safe = F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))

    #c is angular distance. Then multiply by Earth radius
    c = F.lit(2.0) * F.atan2(F.sqrt(a_safe), F.sqrt(F.lit(1.0) - a_safe))

    return (
        df
        .withColumn("haversine_km", F.lit(EARTH_RADIUS_KM) * c)
        .withColumn("haversine_mi", F.col("haversine_km") * F.lit(KM_TO_MILES))
    )


def prepare_trips(df: DataFrame, hours: list[int]) -> DataFrame:
    #create the time and distance columns
    base = (
        df
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
        .withColumn("duration_hours", F.col("duration_minutes") / F.lit(60.0))
        .withColumn("trip_distance_km", F.col("trip_distance") * F.lit(MILES_TO_KM))
    )

    # basic filters and the personal hours here
    clean = (
        base
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.year("pickup_ts") == 2015)
        .filter(F.col("pickup_ts") <= F.col("dropoff_ts"))
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("pickup_hour").isin(hours))
        .filter(F.col("pickup_longitude").isNotNull())
        .filter(F.col("pickup_latitude").isNotNull())
        .filter(F.col("dropoff_longitude").isNotNull())
        .filter(F.col("dropoff_latitude").isNotNull())
    )

    #Remove trips with coordinates outside the NYC bounding box
    clean = filter_nyc_coords(clean)

    #Add Haversine distance using builtin expressions
    with_haversine = add_haversine(clean)

    return (
        with_haversine
        #Average speed based on taxi distance
        .withColumn("speed_kmh", F.col("trip_distance_km") / F.col("duration_hours"))

        #Difference between taxi distance and straightnline distance
        .withColumn("distance_gap_km", F.col("trip_distance_km") - F.col("haversine_km"))

        .withColumn(
            "detour_ratio",
            F.when(
                F.col("haversine_km") > 0.2,
                F.col("trip_distance_km") / F.col("haversine_km"),
            ).otherwise(F.lit(None).cast("double")),
        )

        # helps find very slow trips in the topn5 table
        .withColumn("duration_per_km", F.col("duration_minutes") / F.col("trip_distance_km"))

        #use 10 mph as the slow trip threshold, converted to km
        .withColumn(
            "congestion_candidate",
            (
                (F.col("speed_kmh") < 16.09344) &
                (F.col("trip_distance_km") >= 1.609344)
            ).cast("int"),
        )
    )


def aggregate_by_hour(trips: DataFrame) -> DataFrame:
    #main hourly summary
    return (
        trips
        .groupBy("pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("duration_minutes"), 4).alias("avg_duration_minutes"),
            F.round(F.expr("percentile_approx(duration_minutes, 0.5)"), 4).alias("median_duration_minutes"),
            F.round(F.expr("percentile_approx(duration_minutes, 0.9)"), 4).alias("p90_duration_minutes"),
            F.round(F.avg("speed_kmh"), 4).alias("avg_speed_kmh"),
            F.round((F.sum("trip_distance_km") / F.sum("duration_hours")), 4).alias("agg_speed_kmh"),
            F.round(F.avg("haversine_km"), 4).alias("avg_haversine_km"),
            F.round(F.avg("distance_gap_km"), 4).alias("avg_distance_gap_km"),
            F.round(F.avg("detour_ratio"), 4).alias("avg_detour_ratio"),
            F.round(F.avg("congestion_candidate"), 6).alias("congestion_candidate_share"),
        )
        .orderBy("pickup_hour")
    )


def get_top5_slowest(trips: DataFrame) -> DataFrame:
    #5 trips with the largest minutes per km
    return (
        trips
        .select(
            "vendor_id",
            "pickup_ts",
            "dropoff_ts",
            "pickup_hour",
            "trip_distance",
            "trip_distance_km",
            "duration_minutes",
            "duration_per_km",
            "speed_kmh",
            "haversine_km",
            "distance_gap_km",
            "detour_ratio",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
        )
        .orderBy(F.desc("duration_per_km"))
        .limit(5)
    )


def get_top5_fastest(trips: DataFrame) -> DataFrame:
    #5 trips with the highest speed
    return (
        trips
        .select(
            "vendor_id",
            "pickup_ts",
            "dropoff_ts",
            "pickup_hour",
            "trip_distance",
            "trip_distance_km",
            "duration_minutes",
            "duration_per_km",
            "speed_kmh",
            "haversine_km",
            "distance_gap_km",
            "detour_ratio",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
        )
        .orderBy(F.desc("speed_kmh"))
        .limit(5)
    )


def output_names(input_format: str) -> tuple[str, str]:
    #CSV run gets separate names so no overwrite the Parquet results
    if input_format == "csv":
        return "df_builtin_csv", "_csv"

    return "df_builtin", ""


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark(f"q2_df_builtin_{args.input_format}")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        raw_df = load_trips(spark, args.input_base, args.input_format)
        raw_count = raw_df.count()

        #cache
        trips = prepare_trips(raw_df, personalization.hours).cache()
        filtered_count = trips.count()

        by_hour = aggregate_by_hour(trips)
        top5_slowest = get_top5_slowest(trips)
        top5_fastest = get_top5_fastest(trips)

        table_prefix, file_suffix = output_names(args.input_format)

        by_hour_path = join_hdfs_path(
            args.output_base,
            "results",
            "tables",
            "q2",
            f"{table_prefix}_by_hour",
        )
        slowest_path = join_hdfs_path(
            args.output_base,
            "results",
            "tables",
            "q2",
            f"{table_prefix}_slowest_per_km",
        )
        fastest_path = join_hdfs_path(
            args.output_base,
            "results",
            "tables",
            "q2",
            f"{table_prefix}_fastest",
        )
        metrics_path = join_hdfs_path(
            args.output_base,
            "results",
            "metrics",
            f"q2_df_builtin{file_suffix}_metrics.json",
        )
        plan_path = join_hdfs_path(
            args.output_base,
            "results",
            "plans",
            f"q2_df_builtin{file_suffix}_plan.txt",
        )

        write_csv(by_hour, by_hour_path, args.mode)
        write_csv(top5_slowest, slowest_path, args.mode)
        write_csv(top5_fastest, fastest_path, args.mode)

        #Save plan so I can compare CSV scan vs Parquet
        plan_text = by_hour._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": f"q2_df_builtin{file_suffix}",
            "implementation": "DataFrame API without UDF",
            "input_format": args.input_format,
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
            "filters": {
                "year": 2015,
                "duration_minutes": "> 0",
                "trip_distance": "> 0",
                "coordinate_bounds": "pickup and dropoff coordinates inside NYC-area bounding box",
                "personalized_hours": personalization.hours,
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