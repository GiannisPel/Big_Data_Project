from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import Row

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
        description="Q2 duration/speed/congestion analysis using Spark SQL."
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


def write_csv(df, output_path: str, mode: str) -> None:
    #Merge to a single file so the output is one clean CSV
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", "true")
        .csv(output_path)
    )


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark("q2_sql")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        #Load the 2015 yellow taxi parquet and register it as a SQL view
        input_path = join_hdfs_path(args.input_base, "data", "parquet", "yellow_tripdata_2015")
        raw_df = spark.read.parquet(input_path)
        raw_count = raw_df.count()
        raw_df.createOrReplaceTempView("yellow_2015")

        #Build the IN() list for the personalized pickup hours
        hours_sql = ", ".join(str(h) for h in personalization.hours)

        #Build the prepared view (steps 1-5 all in one CTE chain) (used CTEs to keep each transformation step separate and readable)
        prepare_view_sql = f"""
            CREATE OR REPLACE TEMP VIEW q2_prepared AS

            -- Step 1-2: basic column derivation + initial validity filters
            WITH base AS (
                SELECT
                    pickup_ts,
                    dropoff_ts,
                    to_date(pickup_ts)  AS pickup_date,
                    hour(pickup_ts)     AS pickup_hour,
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude,
                    trip_distance,
                    -- Duration in minutes and hours
                    ((unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0)         AS duration_minutes,
                    (((unix_timestamp(dropoff_ts) - unix_timestamp(pickup_ts)) / 60.0) / 60.0) AS duration_hours,
                    -- Convert miles to km for all metric output
                    trip_distance * {MILES_TO_KM} AS trip_distance_km
                FROM yellow_2015
                WHERE pickup_ts IS NOT NULL
                  AND dropoff_ts IS NOT NULL
                  AND pickup_ts <= dropoff_ts          -- dropoff must come after pickup
                  AND trip_distance > 0
                  AND hour(pickup_ts) IN ({hours_sql}) -- only the personalized hours
                  AND pickup_longitude  IS NOT NULL
                  AND pickup_latitude   IS NOT NULL
                  AND dropoff_longitude IS NOT NULL
                  AND dropoff_latitude  IS NOT NULL
                  -- Step 3: NYC bounding box filter (covers all 5 boroughs + airports)
                  AND pickup_longitude  BETWEEN {MIN_LON} AND {MAX_LON}
                  AND dropoff_longitude BETWEEN {MIN_LON} AND {MAX_LON}
                  AND pickup_latitude   BETWEEN {MIN_LAT} AND {MAX_LAT}
                  AND dropoff_latitude  BETWEEN {MIN_LAT} AND {MAX_LAT}
            ),

            -- Remove any remaining rows where duration ended up as 0
            valid AS (
                SELECT * FROM base WHERE duration_minutes > 0
            ),

            -- Step 4: convert coordinates to radians (need for Haversine)
            radians_added AS (
                SELECT
                    *,
                    radians(pickup_latitude)   AS pickup_lat_rad,
                    radians(pickup_longitude)  AS pickup_lon_rad,
                    radians(dropoff_latitude)  AS dropoff_lat_rad,
                    radians(dropoff_longitude) AS dropoff_lon_rad
                FROM valid
            ),

            -- Step 4: compute Haversine intermediate value 'a'
            -- a = sin^2(Δlat/2) + cos(lat1)*cos(lat2)*sin²(Δlon/2)
            haversine_a AS (
                SELECT
                    *,
                    -- Clamp to [0,1] to guard against tiny floating-point errors
                    least(1.0, greatest(0.0,
                        pow(sin((dropoff_lat_rad - pickup_lat_rad) / 2.0), 2.0)
                        + cos(pickup_lat_rad) * cos(dropoff_lat_rad)
                        * pow(sin((dropoff_lon_rad - pickup_lon_rad) / 2.0), 2.0)
                    )) AS a
                FROM radians_added
            ),

            -- Step 4: angular distance c then multiply by Earth radius
            -- c = 2 * atan2(ριζα[a], ριζα[(1-a)]) :  d = R * c
            distances AS (
                SELECT
                    *,
                    {EARTH_RADIUS_KM} * 2.0 * atan2(sqrt(a), sqrt(1.0 - a)) AS haversine_km
                FROM haversine_a
            )

            -- Final SELECT: add all remaining columns
            SELECT
                *,
                haversine_km * {KM_TO_MILES}                        AS haversine_mi,
                -- Speed in km/h
                trip_distance_km / duration_hours                    AS speed_kmh,
                -- How much longer the actual route was vs the straight-line distance
                trip_distance_km - haversine_km                      AS distance_gap_km,
                -- Detour ratio > 1 means driver took a longer path than necessary
                CASE
                    WHEN haversine_km > 0 THEN trip_distance_km / haversine_km
                    ELSE NULL
                END AS detour_ratio,
                -- Minutes per km; higher = slower trip
                duration_minutes / trip_distance_km                  AS duration_per_km,
                -- Step 5: congestion flag (speed < 10 mph = ~16.09 km/h AND trip >= 1 mile = ~1.609 km)
                CASE
                    WHEN trip_distance_km / duration_hours < 16.09344
                     AND trip_distance_km >= 1.609344 THEN 1
                    ELSE 0
                END AS congestion_candidate
            FROM distances
        """

        spark.sql(prepare_view_sql)

        filtered_count = spark.sql("SELECT COUNT(*) AS cnt FROM q2_prepared").collect()[0]["cnt"]

        #Step 6: aggregate stats per pickup hour
        by_hour_sql = """
            SELECT
                pickup_hour,
                COUNT(*)                                              AS trips,
                ROUND(AVG(duration_minutes), 4)                      AS avg_duration_minutes,
                -- Median and p90 for each percentile_approx
                ROUND(percentile_approx(duration_minutes, 0.5), 4)   AS median_duration_minutes,
                ROUND(percentile_approx(duration_minutes, 0.9), 4)   AS p90_duration_minutes,
                -- Average speed per trip
                ROUND(AVG(speed_kmh), 4)                             AS avg_speed_kmh,
                -- Aggregate speed = total km / total hours
                ROUND(SUM(trip_distance_km) / SUM(duration_hours), 4) AS agg_speed_kmh,
                ROUND(AVG(haversine_km), 4)                          AS avg_haversine_km,
                ROUND(AVG(distance_gap_km), 4)                       AS avg_distance_gap_km,
                ROUND(AVG(detour_ratio), 4)                          AS avg_detour_ratio,
                -- Share of congestion-flagged trips (between 0 and 1)
                ROUND(AVG(congestion_candidate), 6)                  AS congestion_candidate_share
            FROM q2_prepared
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """

        #Step 7: top 5 slowest (highest duration per km)
        slowest_sql = """
            SELECT
                pickup_ts, dropoff_ts, pickup_hour,
                trip_distance_km, duration_minutes, duration_per_km,
                speed_kmh, haversine_km, distance_gap_km, detour_ratio,
                pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude
            FROM q2_prepared
            ORDER BY duration_per_km DESC
            LIMIT 5
        """

        #Step 7: top 5 fastest (highest speed in km/h)
        fastest_sql = """
            SELECT
                pickup_ts, dropoff_ts, pickup_hour,
                trip_distance_km, duration_minutes, duration_per_km,
                speed_kmh, haversine_km, distance_gap_km, detour_ratio,
                pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude
            FROM q2_prepared
            ORDER BY speed_kmh DESC
            LIMIT 5
        """

        by_hour      = spark.sql(by_hour_sql)
        top5_slowest = spark.sql(slowest_sql)
        top5_fastest = spark.sql(fastest_sql)

        #Output paths
        by_hour_path = join_hdfs_path(args.output_base, "results", "tables", "q2", "sql_by_hour")
        slowest_path = join_hdfs_path(args.output_base, "results", "tables", "q2", "sql_slowest_per_km")
        fastest_path = join_hdfs_path(args.output_base, "results", "tables", "q2", "sql_fastest")
        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q2_sql_metrics.json")
        plan_path    = join_hdfs_path(args.output_base, "results", "plans",   "q2_sql_plan.txt")

        #Write CSVs
        write_csv(by_hour,       by_hour_path, args.mode)
        write_csv(top5_slowest,  slowest_path, args.mode)
        write_csv(top5_fastest,  fastest_path, args.mode)

        #Save the query execution plan for the report
        plan_text = by_hour._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        #Build a metrics dict with everything for the report
        metrics = {
            "job": "q2_sql",
            "implementation": "Spark SQL",
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
            "sql": {
                "prepare_view_sql": prepare_view_sql,
                "by_hour_sql": by_hour_sql,
                "slowest_sql": slowest_sql,
                "fastest_sql": fastest_sql,
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

    spark.stop()


if __name__ == "__main__":
    main()