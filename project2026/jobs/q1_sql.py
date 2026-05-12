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
    #SQL version only reads parquet
    parser = argparse.ArgumentParser(description="Q1 Demand analysis using Spark SQL.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "errorifexists"])
    return parser.parse_args()


def make_json_safe(value):
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


def write_small_csv_table(df, output_path: str, mode: str) -> None:
    #coalesce to 1 partition so output is a single file not a bunch of part-xxxxx files
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

    spark = create_spark("q1_sql_parquet")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        input_path = join_hdfs_path(args.input_base, "data", "parquet", "yellow_tripdata_2024")

        #load the parquet data
        df_2024 = spark.read.parquet(input_path)
        df_2024.createOrReplaceTempView("yellow_2024")

        #Only touches relevant rows
        hours_sql = ", ".join(str(h) for h in personalization.hours)
        days_sql = ", ".join(str(d) for d in personalization.days_2024)
        top_k = personalization.top_k

        filtered_sql = f"""
            CREATE OR REPLACE TEMP VIEW q1_filtered AS
            SELECT
                *,
                date_format(pickup_ts, 'E') AS weekday,
                CASE
                    WHEN dayofweek(pickup_ts) IN (1, 7) THEN true
                    ELSE false
                END AS is_weekend,
                -- bucket pickup hour into time-of-day bands
                CASE
                    WHEN pickup_hour BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN pickup_hour BETWEEN 12 AND 16 THEN 'Afternoon'
                    WHEN pickup_hour BETWEEN 17 AND 21 THEN 'Evening'
                    ELSE 'Late'
                END AS time_band
            FROM yellow_2024
            WHERE pickup_ts IS NOT NULL
              AND dropoff_ts IS NOT NULL
              AND pickup_ts <= dropoff_ts
              AND duration_minutes > 0
              AND trip_distance > 0
              AND total_amount > 0
              AND pickup_day IN ({days_sql})
              AND pickup_hour IN ({hours_sql})
        """

        spark.sql(filtered_sql)

        #count how many rows survived the filter (metrics)
        filtered_count = spark.sql("SELECT COUNT(*) AS cnt FROM q1_filtered").collect()[0]["cnt"]

        #group by date+hour, compute stats
        top_hours_query = f"""
            WITH hourly AS (
                SELECT
                    pickup_date,
                    pickup_hour,
                    weekday,
                    is_weekend,
                    time_band,
                    COUNT(*) AS trips,
                    COUNT(DISTINCT pu_location_id) AS unique_pickup_zones,
                    AVG(passenger_count) AS avg_passenger_count,
                    AVG(duration_minutes) AS avg_duration_minutes,
                    AVG(trip_distance) AS avg_trip_distance,
                    AVG(total_amount) AS avg_total_amount,
                    SUM(total_amount) AS total_revenue
                FROM q1_filtered
                GROUP BY pickup_date, pickup_hour, weekday, is_weekend, time_band
            ),
            with_share AS (
                SELECT
                    *,
                    -- SUM with no PARTITION BY = sum over entire result set = global total
                    SUM(trips) OVER () AS total_trips_in_personal_window
                FROM hourly
            )
            SELECT
                pickup_date,
                pickup_hour,
                weekday,
                is_weekend,
                time_band,
                trips,
                unique_pickup_zones,
                ROUND(avg_passenger_count, 4) AS avg_passenger_count,
                ROUND(avg_duration_minutes, 4) AS avg_duration_minutes,
                ROUND(avg_trip_distance, 4) AS avg_trip_distance,
                ROUND(avg_total_amount, 4) AS avg_total_amount,
                ROUND(total_revenue, 4) AS total_revenue,
                ROUND(trips / total_trips_in_personal_window, 6) AS trips_share_in_personal_window
            FROM with_share
            ORDER BY trips DESC, total_revenue DESC, pickup_date ASC, pickup_hour ASC
            LIMIT {top_k}
        """

        time_band_query = """
            WITH by_band AS (
                SELECT
                    time_band,
                    COUNT(*) AS trips,
                    COUNT(DISTINCT pu_location_id) AS unique_pickup_zones,
                    AVG(passenger_count) AS avg_passenger_count,
                    AVG(duration_minutes) AS avg_duration_minutes,
                    AVG(trip_distance) AS avg_trip_distance,
                    AVG(total_amount) AS avg_total_amount,
                    SUM(total_amount) AS total_revenue
                FROM q1_filtered
                GROUP BY time_band
            ),
            with_share AS (
                SELECT
                    *,
                    SUM(trips) OVER () AS total_trips_in_personal_window
                FROM by_band
            )
            SELECT
                time_band,
                trips,
                unique_pickup_zones,
                ROUND(avg_passenger_count, 4) AS avg_passenger_count,
                ROUND(avg_duration_minutes, 4) AS avg_duration_minutes,
                ROUND(avg_trip_distance, 4) AS avg_trip_distance,
                ROUND(avg_total_amount, 4) AS avg_total_amount,
                ROUND(total_revenue, 4) AS total_revenue,
                ROUND(trips / total_trips_in_personal_window, 6) AS trips_share_in_personal_window
            FROM with_share
            ORDER BY trips DESC
        """

        top_hours_df = spark.sql(top_hours_query)
        time_band_df = spark.sql(time_band_query)
        result_dir = join_hdfs_path(args.output_base, "results", "tables", "q1", "sql_parquet")
        time_band_dir = join_hdfs_path(args.output_base, "results", "tables", "q1", "sql_parquet_time_band")
        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q1_sql_parquet_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", "q1_sql_parquet_plan.txt")
        #write to HDFS
        write_small_csv_table(top_hours_df, result_dir, args.mode)
        write_small_csv_table(time_band_df, time_band_dir, args.mode)
        plan_text = top_hours_df._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)
        #collect to driver for the metrics JSON preview
        top_rows = [make_json_safe(row) for row in top_hours_df.collect()]
        time_band_rows = [make_json_safe(row) for row in time_band_df.collect()]

        metrics = {
            "job": "q1_sql",
            "input_format": "parquet",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "hours": personalization.hours,
                "days_2024": personalization.days_2024,
                "top_k": personalization.top_k,
            },
            "filtered_row_count": filtered_count,
            "output_paths": {
                "top_hours": result_dir,
                "time_band_summary": time_band_dir,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            #include the actual SQL in the metrics for debug or rerun easily
            "top_hours_query": top_hours_query,
            "time_band_query": time_band_query,
            "top_hours_preview": top_rows,
            "time_band_summary_preview": time_band_rows,
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        #print so we can see output in the terminal
        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

    spark.stop()


if __name__ == "__main__":
    main()