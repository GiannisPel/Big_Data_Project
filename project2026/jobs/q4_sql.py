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
    schema_2024_raw,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q4 payment type and tipping behavior using Spark SQL.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-format", required=True, choices=["csv", "parquet"])
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


def write_small_csv_table(df: DataFrame, output_path: str, mode: str) -> None:
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", "true")
        .csv(output_path)
    )


def read_2024_csv(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "yellow_tripdata_2024.csv")

    raw_df = (
        spark.read
        .option("header", "true")
        .schema(schema_2024_raw())
        .csv(path)
    )

    # rename columns to match the parquet schema so the rest of the code is the same
    return raw_df.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("pickup_ts"),
        F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("dropoff_ts"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )


def read_2024_parquet(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2024")
    return spark.read.parquet(path).select(
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "trip_distance",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
    )


def add_time_columns(df: DataFrame) -> DataFrame:
    #compute day/hour/duration for all
    return (
        df
        .withColumn("pickup_day", F.dayofmonth("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
    )


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark(f"q4_sql_{args.input_format}")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        if args.input_format == "csv":
            trips_df = read_2024_csv(spark, args.input_base)
        else:
            trips_df = read_2024_parquet(spark, args.input_base)

        trips_df = add_time_columns(trips_df)
        trips_df.createOrReplaceTempView("yellow_2024_q4")

        #build the IN lists from personal
        days_sql = ", ".join(str(d) for d in personalization.days_2024)
        hours_sql = ", ".join(str(h) for h in personalization.hours)

        #filtered view
        #fare_amount > 0 is already required by the filter so tip_amount/fare_amount
        prepare_query = f"""
            CREATE OR REPLACE TEMP VIEW q4_filtered AS
            SELECT
                vendor_id,
                pickup_day,
                pickup_hour,
                payment_type,
                CASE
                    WHEN payment_type = 1 THEN 'card'
                    WHEN payment_type = 2 THEN 'cash'
                    ELSE 'other'
                END AS payment_label,
                trip_distance,
                duration_minutes,
                fare_amount,
                tip_amount,
                total_amount
            FROM yellow_2024_q4
            WHERE pickup_ts IS NOT NULL
              AND dropoff_ts IS NOT NULL
              AND pickup_ts <= dropoff_ts
              AND duration_minutes > 0
              AND trip_distance > 0
              AND fare_amount > 0
              AND total_amount > 0
              AND payment_type IN (1, 2)
              AND pickup_day IN ({days_sql})
              AND pickup_hour IN ({hours_sql})
        """

        spark.sql(prepare_query)

        #get filtered row count from the view 
        filtered_count = spark.sql("SELECT COUNT(*) AS cnt FROM q4_filtered").collect()[0]["cnt"]

        #table 1: trips broken down by hour and payment type
        by_hour_payment_query = """
            WITH grouped AS (
                SELECT
                    pickup_hour,
                    payment_type,
                    payment_label,
                    COUNT(*) AS trips,
                    AVG(fare_amount) AS avg_fare_amount,
                    AVG(total_amount) AS avg_total_amount,
                    AVG(tip_amount) AS avg_tip_amount,
                    AVG(tip_amount / fare_amount) AS tip_rate,
                    AVG(CASE WHEN tip_amount = 0 THEN 1.0 ELSE 0.0 END) AS zero_tip_share,
                    AVG(trip_distance) AS avg_trip_distance,
                    AVG(duration_minutes) AS avg_duration_minutes
                FROM q4_filtered
                GROUP BY pickup_hour, payment_type, payment_label
            )
            SELECT
                pickup_hour,
                payment_type,
                payment_label,
                trips,
                ROUND(trips / SUM(trips) OVER (PARTITION BY pickup_hour), 6) AS payment_share_in_hour,
                ROUND(avg_fare_amount, 4) AS avg_fare_amount,
                ROUND(avg_total_amount, 4) AS avg_total_amount,
                ROUND(avg_tip_amount, 4) AS avg_tip_amount,
                ROUND(tip_rate, 6) AS tip_rate,
                ROUND(zero_tip_share, 6) AS zero_tip_share,
                ROUND(avg_trip_distance, 4) AS avg_trip_distance,
                ROUND(avg_duration_minutes, 4) AS avg_duration_minutes
            FROM grouped
            ORDER BY pickup_hour, payment_type
        """

        #table 2: check if the two vendors have different payment distributions
        vendor_payment_query = """
            WITH grouped AS (
                SELECT
                    vendor_id,
                    payment_type,
                    payment_label,
                    COUNT(*) AS trips,
                    AVG(total_amount) AS avg_total_amount,
                    AVG(fare_amount) AS avg_fare_amount,
                    AVG(tip_amount) AS avg_tip_amount,
                    AVG(tip_amount / fare_amount) AS tip_rate,
                    AVG(CASE WHEN tip_amount = 0 THEN 1.0 ELSE 0.0 END) AS zero_tip_share
                FROM q4_filtered
                GROUP BY vendor_id, payment_type, payment_label
            )
            SELECT
                vendor_id,
                payment_type,
                payment_label,
                trips,
                ROUND(trips / SUM(trips) OVER (PARTITION BY vendor_id), 6) AS payment_share_in_vendor,
                ROUND(avg_total_amount, 4) AS avg_total_amount,
                ROUND(avg_fare_amount, 4) AS avg_fare_amount,
                ROUND(avg_tip_amount, 4) AS avg_tip_amount,
                ROUND(tip_rate, 6) AS tip_rate,
                ROUND(zero_tip_share, 6) AS zero_tip_share
            FROM grouped
            ORDER BY vendor_id, payment_type
        """

        #table 3: card vs cash summary per hour
        card_vs_cash_query = """
            SELECT
                pickup_hour,
                SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS card_trips,
                SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
                ROUND(
                    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / COUNT(*),
                    6
                ) AS card_share,
                ROUND(AVG(CASE WHEN payment_type = 1 THEN total_amount END), 4) AS avg_total_card,
                ROUND(AVG(CASE WHEN payment_type = 2 THEN total_amount END), 4) AS avg_total_cash,
                ROUND(AVG(CASE WHEN payment_type = 1 THEN fare_amount END), 4) AS avg_fare_card,
                ROUND(AVG(CASE WHEN payment_type = 2 THEN fare_amount END), 4) AS avg_fare_cash,
                ROUND(AVG(CASE WHEN payment_type = 1 THEN tip_amount / fare_amount END), 6) AS avg_tip_rate_card
            FROM q4_filtered
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """

        by_hour_payment = spark.sql(by_hour_payment_query)
        vendor_payment = spark.sql(vendor_payment_query)
        card_vs_cash = spark.sql(card_vs_cash_query)

        label = f"sql_{args.input_format}"
        table_base = join_hdfs_path(args.output_base, "results", "tables", "q4")

        paths = {
            "by_hour_payment": join_hdfs_path(table_base, f"{label}_by_hour_payment"),
            "vendor_payment": join_hdfs_path(table_base, f"{label}_vendor_payment"),
            "card_vs_cash": join_hdfs_path(table_base, f"{label}_card_vs_cash"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", f"q4_{label}_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", f"q4_{label}_plan.txt")

        write_small_csv_table(by_hour_payment, paths["by_hour_payment"], args.mode)
        write_small_csv_table(vendor_payment, paths["vendor_payment"], args.mode)
        write_small_csv_table(card_vs_cash, paths["card_vs_cash"], args.mode)

        #save plan for card_vs_cash
        plan_text = card_vs_cash._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q4_sql",
            "input_format": args.input_format,
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
                "fare_amount_gt_0": True,
                "total_amount_gt_0": True,
                "payment_type_in": [1, 2],
            },
            "filtered_row_count": filtered_count,
            "output_paths": {
                **paths,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "sql": {
                "prepare_query": prepare_query,
                "by_hour_payment_query": by_hour_payment_query,
                "vendor_payment_query": vendor_payment_query,
                "card_vs_cash_query": card_vs_cash_query,
            },
            "by_hour_payment_preview": rows_to_dicts(by_hour_payment.collect()),
            "vendor_payment_preview": rows_to_dicts(vendor_payment.collect()),
            "card_vs_cash_preview": rows_to_dicts(card_vs_cash.collect()),
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