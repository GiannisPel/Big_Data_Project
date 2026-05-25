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
    parser = argparse.ArgumentParser(description="Q3 revenue and high-value zones using Spark SQL.")
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


def choose_support_threshold(total_trips: int) -> int:
    # Support threshold for efficiency rankings.
    # Without this, a zone with only a few trips could appear at the top just
    # because of one unusually expensive trip.
    one_percent = int(total_trips * 0.01)
    return max(10, min(50, one_percent))


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark("q3_sql_parquet")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        #Q3 SQL uses prepared Parquet datasets: yellow taxi trips for 2024 and taxi zone lookup for zone names or boroughs
        trips_path = join_hdfs_path(args.input_base, "data", "parquet", "yellow_tripdata_2024")
        zones_path = join_hdfs_path(args.input_base, "data", "parquet", "taxi_zone_lookup")

        trips_df = spark.read.parquet(trips_path)
        zones_df = spark.read.parquet(zones_path)

        trips_df.createOrReplaceTempView("yellow_2024")
        zones_df.createOrReplaceTempView("taxi_zones")

        #personal filters from student ID.
        days_sql = ", ".join(str(d) for d in personalization.days_2024)
        hours_sql = ", ".join(str(h) for h in personalization.hours)
        top_k = personalization.top_k

        #Count valid personal subset first cause depends on the size of personal subset
        filtered_count_query = f"""
            SELECT COUNT(*) AS cnt
            FROM yellow_2024
            WHERE pickup_ts IS NOT NULL
              AND dropoff_ts IS NOT NULL
              AND pickup_ts <= dropoff_ts
              AND duration_minutes > 0
              AND trip_distance > 0
              AND fare_amount > 0
              AND total_amount > 0
              AND pickup_day IN ({days_sql})
              AND pickup_hour IN ({hours_sql})
        """

        filtered_count = spark.sql(filtered_count_query).collect()[0]["cnt"]
        support_threshold = choose_support_threshold(filtered_count)

        #Filter valid trips in the personal window
        #Create distance buckets
        #Join with taxi_zone_lookup using pickup location.
        #Aggregate revenue and efficiency metrics by: pickup_borough, pickup_zone, distance_bucket.
        grouped_query = f"""
            CREATE OR REPLACE TEMP VIEW q3_grouped AS
            WITH filtered AS (
                SELECT
                    *,
                    CASE
                        WHEN trip_distance < 1 THEN 'very_short'
                        WHEN trip_distance >= 1 AND trip_distance < 3 THEN 'short'
                        WHEN trip_distance >= 3 AND trip_distance < 10 THEN 'medium'
                        ELSE 'long'
                    END AS distance_bucket,

                    -- Add surcharge-related components together.
                    -- coalesce is used because some columns may contain nulls.
                    (
                        coalesce(extra, 0.0)
                        + coalesce(mta_tax, 0.0)
                        + coalesce(tolls_amount, 0.0)
                        + coalesce(improvement_surcharge, 0.0)
                        + coalesce(congestion_surcharge, 0.0)
                        + coalesce(airport_fee, 0.0)
                    ) AS surcharge_components
                FROM yellow_2024
                WHERE pickup_ts IS NOT NULL
                  AND dropoff_ts IS NOT NULL
                  AND pickup_ts <= dropoff_ts
                  AND duration_minutes > 0
                  AND trip_distance > 0
                  AND fare_amount > 0
                  AND total_amount > 0
                  AND pickup_day IN ({days_sql})
                  AND pickup_hour IN ({hours_sql})
            ),
            joined AS (
                SELECT
                    -- If a pickup location does not match the lookup table,
                    -- keep it as Unknown instead of dropping the trip.
                    coalesce(z.borough, 'Unknown') AS pickup_borough,
                    coalesce(z.zone, 'Unknown') AS pickup_zone,
                    f.distance_bucket,
                    f.total_amount,
                    f.trip_distance,
                    f.duration_minutes,
                    f.fare_amount,
                    f.tip_amount,
                    f.surcharge_components
                FROM filtered f
                LEFT JOIN taxi_zones z
                  ON f.pu_location_id = z.location_id
            )
            SELECT
                pickup_borough,
                pickup_zone,
                distance_bucket,
                COUNT(*) AS trips,

                -- Main revenue metric.
                ROUND(SUM(total_amount), 4) AS total_revenue,

                -- Average money per trip.
                ROUND(AVG(total_amount), 4) AS avg_revenue_per_trip,

                -- Efficiency metrics.
                ROUND(SUM(total_amount) / SUM(trip_distance), 4) AS revenue_per_mile,
                ROUND(SUM(total_amount) / SUM(duration_minutes), 4) AS revenue_per_minute,

                -- Extra supporting metrics for interpretation.
                ROUND(AVG(fare_amount), 4) AS avg_fare_amount,
                ROUND(AVG(tip_amount), 4) AS avg_tip_amount,
                ROUND(SUM(surcharge_components) / SUM(total_amount), 6) AS surcharge_share
            FROM joined
            GROUP BY pickup_borough, pickup_zone, distance_bucket
        """

        spark.sql(grouped_query)

        #Rank 1:
        #Top k zones or buckets by total revenue - no support threshold
        top_total_revenue_query = f"""
            SELECT *
            FROM q3_grouped
            ORDER BY total_revenue DESC, trips DESC
            LIMIT {top_k}
        """

        #Rank 2: Top k zones or buckets by revenue per mile with support theshold
        top_revenue_per_mile_query = f"""
            SELECT *
            FROM q3_grouped
            WHERE trips >= {support_threshold}
            ORDER BY revenue_per_mile DESC, trips DESC
            LIMIT {top_k}
        """

        #Rank 3: Top k zonesor buckets by revenue per minute with support theshold
        top_revenue_per_minute_query = f"""
            SELECT *
            FROM q3_grouped
            WHERE trips >= {support_threshold}
            ORDER BY revenue_per_minute DESC, trips DESC
            LIMIT {top_k}
        """

        top_total_revenue = spark.sql(top_total_revenue_query)
        top_revenue_per_mile = spark.sql(top_revenue_per_mile_query)
        top_revenue_per_minute = spark.sql(top_revenue_per_minute_query)

        #HDFS output paths for result tables, metrics and plans
        table_base = join_hdfs_path(args.output_base, "results", "tables", "q3")
        paths = {
            "top_total_revenue": join_hdfs_path(table_base, "sql_parquet_top_total_revenue"),
            "top_revenue_per_mile": join_hdfs_path(table_base, "sql_parquet_top_revenue_per_mile"),
            "top_revenue_per_minute": join_hdfs_path(table_base, "sql_parquet_top_revenue_per_minute"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q3_sql_parquet_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", "q3_sql_parquet_plan.txt")

        #Write three small result tables
        write_small_csv_table(top_total_revenue, paths["top_total_revenue"], args.mode)
        write_small_csv_table(top_revenue_per_mile, paths["top_revenue_per_mile"], args.mode)
        write_small_csv_table(top_revenue_per_minute, paths["top_revenue_per_minute"], args.mode)

        #Save the Spark plan
        plan_text = top_revenue_per_mile._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        #Store all important evidence in one JSON file: Application id, filters, output paths, preview rows
        metrics = {
            "job": "q3_sql",
            "input_format": "parquet",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "days_2024": personalization.days_2024,
                "hours": personalization.hours,
                "top_k": personalization.top_k,
            },
            "support_threshold": support_threshold,
            "support_threshold_policy": "max(10, min(50, floor(personal_subset_trips * 0.01)))",
            "filtered_row_count": filtered_count,
            "output_paths": {
                **paths,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "sql": {
                "grouped_query": grouped_query,
                "top_total_revenue_query": top_total_revenue_query,
                "top_revenue_per_mile_query": top_revenue_per_mile_query,
                "top_revenue_per_minute_query": top_revenue_per_minute_query,
            },
            "top_total_revenue_preview": rows_to_dicts(top_total_revenue.collect()),
            "top_revenue_per_mile_preview": rows_to_dicts(top_revenue_per_mile.collect()),
            "top_revenue_per_minute_preview": rows_to_dicts(top_revenue_per_minute.collect()),
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        #Also print metrics in driver logs for easier debugging
        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

    spark.stop()


if __name__ == "__main__":
    main()