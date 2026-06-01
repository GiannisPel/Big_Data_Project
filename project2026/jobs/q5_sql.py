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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q5 airport and borough flow analysis using Spark SQL.")
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
        return {k: make_json_safe(v) for k, v in value.asDict(recursive=True).items()}
    if isinstance(value, list):
        return [make_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    return value


def rows_to_dicts(rows: list[Row]) -> list[dict[str, Any]]:
    return [make_json_safe(row.asDict(recursive=True)) for row in rows]


def write_small_csv_table(df, output_path: str, mode: str) -> None:
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

    spark = create_spark("q5_sql_parquet")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        trips_path = join_hdfs_path(args.input_base, "data", "parquet", "yellow_tripdata_2024")
        zones_path = join_hdfs_path(args.input_base, "data", "parquet", "taxi_zone_lookup")

        trips_df = spark.read.parquet(trips_path)
        zones_df = spark.read.parquet(zones_path)

        trips_df.createOrReplaceTempView("yellow_2024")
        zones_df.createOrReplaceTempView("taxi_zones")

        #Build IN clause values from personal
        days_sql = ", ".join(str(d) for d in personalization.days_2024)
        hours_sql = ", ".join(str(h) for h in personalization.hours)
        top_k = personalization.top_k

        #Create TempView with filtered trips + zone info + airport flags
        #Split into CTE for readability
        prepare_query = f"""
            CREATE OR REPLACE TEMP VIEW q5_enriched AS
            WITH filtered AS (
                SELECT
                    *
                FROM yellow_2024
                WHERE pickup_ts IS NOT NULL
                  AND dropoff_ts IS NOT NULL
                  AND pickup_ts <= dropoff_ts
                  AND duration_minutes > 0
                  AND trip_distance > 0
                  AND total_amount > 0
                  AND pickup_day IN ({days_sql})
                  AND pickup_hour IN ({hours_sql})
            ),
            joined AS (
                SELECT
                    f.*,
                    coalesce(pu.borough, 'Unknown') AS pu_borough,
                    coalesce(pu.zone, 'Unknown') AS pu_zone,
                    coalesce(pu.service_zone, 'Unknown') AS pu_service_zone,
                    coalesce(doz.borough, 'Unknown') AS do_borough,
                    coalesce(doz.zone, 'Unknown') AS do_zone,
                    coalesce(doz.service_zone, 'Unknown') AS do_service_zone
                FROM filtered f
                LEFT JOIN taxi_zones pu
                  ON f.pu_location_id = pu.location_id
                LEFT JOIN taxi_zones doz
                  ON f.do_location_id = doz.location_id
            )
            SELECT
                *,
                -- pu_is_airport: airport zone/borough match OR airport_fee > 0 as pickup signal
                CASE
                    WHEN lower(pu_zone) RLIKE 'jfk|laguardia|newark|airport'
                      OR lower(pu_borough) = 'ewr'
                      OR coalesce(airport_fee, 0.0) > 0
                    THEN 1 ELSE 0
                END AS pu_is_airport,
                -- do_is_airport: zone/borough only, NOT airport_fee (fee is charged at trip origin)
                CASE
                    WHEN lower(do_zone) RLIKE 'jfk|laguardia|newark|airport'
                      OR lower(do_borough) = 'ewr'
                    THEN 1 ELSE 0
                END AS do_is_airport,
                -- airport_trip: 1 if either endpoint is an airport
                CASE
                    WHEN lower(pu_zone) RLIKE 'jfk|laguardia|newark|airport'
                      OR lower(pu_borough) = 'ewr'
                      OR coalesce(airport_fee, 0.0) > 0
                      OR lower(do_zone) RLIKE 'jfk|laguardia|newark|airport'
                      OR lower(do_borough) = 'ewr'
                    THEN 1 ELSE 0
                END AS airport_trip
            FROM joined
        """

        spark.sql(prepare_query)

        #Need filtered_count for trip_share
        filtered_count = spark.sql("SELECT COUNT(*) AS cnt FROM q5_enriched").collect()[0]["cnt"]

        #borough to borough flows with metrics
        #trips DESC first, then revenue if there is a tie
        borough_flows_query = f"""
            SELECT
                pu_borough,
                do_borough,
                COUNT(*) AS trips,
                ROUND(COUNT(*) / CAST({filtered_count} AS DOUBLE), 6) AS trip_share,
                ROUND(SUM(total_amount), 4) AS total_revenue,
                ROUND(AVG(total_amount), 4) AS avg_total_amount,
                ROUND(AVG(trip_distance), 4) AS avg_trip_distance,
                ROUND(AVG(duration_minutes), 4) AS avg_duration_minutes,
                ROUND(AVG(airport_trip), 6) AS airport_trip_share
            FROM q5_enriched
            GROUP BY pu_borough, do_borough
            ORDER BY trips DESC, total_revenue DESC
            LIMIT {top_k}
        """

        #Airport routes
        airport_routes_query = f"""
            SELECT
                pu_zone,
                do_zone,
                COUNT(*) AS trips,
                ROUND(AVG(coalesce(airport_fee, 0.0)), 4) AS avg_airport_fee,
                ROUND(AVG(total_amount), 4) AS avg_total_amount,
                ROUND(AVG(duration_minutes), 4) AS avg_duration_minutes,
                ROUND(AVG(trip_distance), 4) AS avg_trip_distance
            FROM q5_enriched
            WHERE airport_trip = 1
            GROUP BY pu_zone, do_zone
            ORDER BY trips DESC, avg_total_amount DESC
            LIMIT {top_k}
        """

        #zones classified as airports (for doc)
        airport_examples_query = """
            SELECT
                location_id,
                borough,
                zone,
                service_zone
            FROM taxi_zones
            WHERE lower(zone) RLIKE 'jfk|laguardia|newark|airport'
               OR lower(borough) = 'ewr'
            ORDER BY borough, zone
            LIMIT 10
        """

        borough_flows = spark.sql(borough_flows_query)
        airport_routes = spark.sql(airport_routes_query)
        airport_examples = spark.sql(airport_examples_query)

        table_base = join_hdfs_path(args.output_base, "results", "tables", "q5")

        paths = {
            "borough_flows": join_hdfs_path(table_base, "sql_parquet_borough_flows"),
            "airport_routes": join_hdfs_path(table_base, "sql_parquet_airport_routes"),
            "airport_zone_examples": join_hdfs_path(table_base, "sql_parquet_airport_zone_examples"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q5_sql_parquet_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", "q5_sql_parquet_plan.txt")

        write_small_csv_table(borough_flows, paths["borough_flows"], args.mode)
        write_small_csv_table(airport_routes, paths["airport_routes"], args.mode)
        write_small_csv_table(airport_examples, paths["airport_zone_examples"], args.mode)

        #Save plan
        plan_text = borough_flows._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q5_sql",
            "input_format": "parquet",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "days_2024": personalization.days_2024,
                "hours": personalization.hours,
                "top_k": personalization.top_k,
            },
            "filters": {
                "duration_minutes_gt_0": True,
                "trip_distance_gt_0": True,
                "total_amount_gt_0": True,
            },
            "airport_detection_policy": {
                "zone_name_terms": ["JFK", "LaGuardia", "Newark", "Airport"],
                "borough_ewr": "EWR",
                "airport_fee_gt_0_used_as_pickup_airport_signal": True,
            },
            "filtered_row_count": filtered_count,
            "output_paths": {
                **paths,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "sql": {
                "prepare_query": prepare_query,
                "borough_flows_query": borough_flows_query,
                "airport_routes_query": airport_routes_query,
                "airport_examples_query": airport_examples_query,
            },
            "borough_flows_preview": rows_to_dicts(borough_flows.collect()),
            "airport_routes_preview": rows_to_dicts(airport_routes.collect()),
            "airport_zone_examples_preview": rows_to_dicts(airport_examples.collect()),
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