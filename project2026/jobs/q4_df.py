from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import (
    Timer,
    build_personalization,
    create_spark,
    join_hdfs_path,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q4 payment type and tipping behavior using DataFrame API.")
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


def write_small_csv_table(df: DataFrame, output_path: str, mode: str) -> None:
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", "true")
        .csv(output_path)
    )


def read_2024_parquet(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2024")
    return spark.read.parquet(path)


def prepare_q4_df(df: DataFrame, days: list[int], hours: list[int]) -> DataFrame:
    #add columns then apply all quality filters in one go
    return (
        df
        .withColumn("pickup_day", F.dayofmonth("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
        .withColumn(
            "payment_label",
            F.when(F.col("payment_type") == 1, F.lit("card"))
            .when(F.col("payment_type") == 2, F.lit("cash"))
            .otherwise(F.lit("other")),
        )
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("pickup_ts") <= F.col("dropoff_ts"))
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("payment_type").isin([1, 2]))
        .filter(F.col("pickup_day").isin(days))
        .filter(F.col("pickup_hour").isin(hours))
    )


def calculate_by_hour_payment(df: DataFrame) -> DataFrame:
    #group by hour and payment type, then use a window to get the share within each hour
    grouped = (
        df
        .groupBy("pickup_hour", "payment_type", "payment_label")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            # tip_rate: avg of tip/fare per trip (fare_amount > 0 guaranteed by filter above)
            F.avg(F.col("tip_amount") / F.col("fare_amount")).alias("tip_rate"),
            # zero_tip_share: fraction of trips where no tip was given
            F.avg(F.when(F.col("tip_amount") == 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("zero_tip_share"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
        )
    )

    #window over all rows with same hour to compute the payment share
    hour_window = Window.partitionBy("pickup_hour")

    return (
        grouped
        .withColumn(
            "payment_share_in_hour",
            F.round(F.col("trips") / F.sum("trips").over(hour_window), 6),
        )
        .select(
            "pickup_hour",
            "payment_type",
            "payment_label",
            "trips",
            "payment_share_in_hour",
            F.round("avg_fare_amount", 4).alias("avg_fare_amount"),
            F.round("avg_total_amount", 4).alias("avg_total_amount"),
            F.round("avg_tip_amount", 4).alias("avg_tip_amount"),
            F.round("tip_rate", 6).alias("tip_rate"),
            F.round("zero_tip_share", 6).alias("zero_tip_share"),
            F.round("avg_trip_distance", 4).alias("avg_trip_distance"),
            F.round("avg_duration_minutes", 4).alias("avg_duration_minutes"),
        )
        .orderBy("pickup_hour", "payment_type")
    )


def calculate_vendor_payment(df: DataFrame) -> DataFrame:
    #similar to by_hour but grouped by vendor instead
    grouped = (
        df
        .groupBy("vendor_id", "payment_type", "payment_label")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg(F.col("tip_amount") / F.col("fare_amount")).alias("tip_rate"),
            F.avg(F.when(F.col("tip_amount") == 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("zero_tip_share"),
        )
    )

    vendor_window = Window.partitionBy("vendor_id")

    return (
        grouped
        .withColumn(
            "payment_share_in_vendor",
            F.round(F.col("trips") / F.sum("trips").over(vendor_window), 6),
        )
        .select(
            "vendor_id",
            "payment_type",
            "payment_label",
            "trips",
            "payment_share_in_vendor",
            F.round("avg_total_amount", 4).alias("avg_total_amount"),
            F.round("avg_fare_amount", 4).alias("avg_fare_amount"),
            F.round("avg_tip_amount", 4).alias("avg_tip_amount"),
            F.round("tip_rate", 6).alias("tip_rate"),
            F.round("zero_tip_share", 6).alias("zero_tip_share"),
        )
        .orderBy("vendor_id", "payment_type")
    )


def calculate_card_vs_cash(df: DataFrame) -> DataFrame:
    #card vs cash comparison per hour
    return (
        df
        .groupBy("pickup_hour")
        .agg(
            F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)).alias("card_trips"),
            F.sum(F.when(F.col("payment_type") == 2, 1).otherwise(0)).alias("cash_trips"),
            F.round(
                F.sum(F.when(F.col("payment_type") == 1, 1).otherwise(0)) / F.count(F.lit(1)),
                6,
            ).alias("card_share"),
            F.round(F.avg(F.when(F.col("payment_type") == 1, F.col("total_amount"))), 4).alias("avg_total_card"),
            F.round(F.avg(F.when(F.col("payment_type") == 2, F.col("total_amount"))), 4).alias("avg_total_cash"),
            F.round(F.avg(F.when(F.col("payment_type") == 1, F.col("fare_amount"))), 4).alias("avg_fare_card"),
            F.round(F.avg(F.when(F.col("payment_type") == 2, F.col("fare_amount"))), 4).alias("avg_fare_cash"),
            F.round(
                F.avg(
                    F.when(
                        F.col("payment_type") == 1,
                        F.col("tip_amount") / F.col("fare_amount"),
                    )
                ),
                6,
            ).alias("avg_tip_rate_card"),
        )
        .orderBy("pickup_hour")
    )


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark("q4_df_parquet")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        raw_df = read_2024_parquet(spark, args.input_base)

        #filter and cache because we run 3 different aggregations on the same data
        filtered_df = prepare_q4_df(
            raw_df,
            days=personalization.days_2024,
            hours=personalization.hours,
        ).cache()

        filtered_count = filtered_df.count()

        by_hour_payment = calculate_by_hour_payment(filtered_df)
        vendor_payment = calculate_vendor_payment(filtered_df)
        card_vs_cash = calculate_card_vs_cash(filtered_df)

        table_base = join_hdfs_path(args.output_base, "results", "tables", "q4")

        paths = {
            "by_hour_payment": join_hdfs_path(table_base, "df_parquet_by_hour_payment"),
            "vendor_payment": join_hdfs_path(table_base, "df_parquet_vendor_payment"),
            "card_vs_cash": join_hdfs_path(table_base, "df_parquet_card_vs_cash"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q4_df_parquet_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", "q4_df_parquet_plan.txt")

        write_small_csv_table(by_hour_payment, paths["by_hour_payment"], args.mode)
        write_small_csv_table(vendor_payment, paths["vendor_payment"], args.mode)
        write_small_csv_table(card_vs_cash, paths["card_vs_cash"], args.mode)

        #save plan for the card_vs_cash query
        plan_text = card_vs_cash._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q4_df",
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

        #release the cached data
        filtered_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()