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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q6 pickup/dropoff imbalance analysis using DataFrame API.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--activity-threshold", type=int, default=30,
                        help="Minimum activity (pickups+dropoffs) to include a zone (default 30).")
    parser.add_argument("--top-k", type=int, default=None,
                        help="Override personalization top_k.")
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


def read_zones_parquet(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "data", "parquet", "taxi_zone_lookup")
    return spark.read.parquet(path)


def filter_trips(df: DataFrame, days: list[int], hours: list[int]) -> DataFrame:
    
    #only valid trips in the personal window
    return (
        df
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("pickup_day").isin(days))
        .filter(F.col("pickup_hour").isin(hours))
    )


def build_pickups(df: DataFrame) -> DataFrame:
    
    #Count pickups per (hour, zone). Using pickup_hour key

    return (
        df
        .groupBy(
            F.col("pickup_hour").alias("hour"),
            F.col("pu_location_id").alias("location_id"),
        )
        .agg(F.count(F.lit(1)).alias("pickups"))
    )


def build_dropoffs(df: DataFrame) -> DataFrame:
    
    #Count dropoffs per (hour, zone). using pickup_hour key
    return (
        df
        .groupBy(
            F.col("pickup_hour").alias("hour"),
            F.col("do_location_id").alias("location_id"),
        )
        .agg(F.count(F.lit(1)).alias("dropoffs"))
    )


def build_imbalance(
    pickups_df: DataFrame,
    dropoffs_df: DataFrame,
    zones_df: DataFrame,
    activity_threshold: int,
    top_k: int,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Full outer join pickups and dropoffs
    Metrics:
    activity= pickups + dropoffs
    net_pickups =pickups - dropoffs
    imbalance_ratio   = net_pickups / activity  
    abs_imbalance_ratio = abs(imbalance_ratio)
    """
    joined = (
        pickups_df.alias("p")
        .join(dropoffs_df.alias("d"), on=["hour", "location_id"], how="full_outer")
        .select(
            F.coalesce(F.col("p.hour"),        F.col("d.hour")).alias("hour"),
            F.coalesce(F.col("p.location_id"), F.col("d.location_id")).alias("location_id"),
            F.coalesce(F.col("p.pickups"),  F.lit(0)).alias("pickups"),
            F.coalesce(F.col("d.dropoffs"), F.lit(0)).alias("dropoffs"),
        )
    )

    metrics = (
        joined
        .withColumn("activity",    F.col("pickups") + F.col("dropoffs"))
        .withColumn("net_pickups", F.col("pickups") - F.col("dropoffs"))
        .withColumn(
            "imbalance_ratio",
            F.when(
                F.col("activity") > 0,
                F.round((F.col("pickups") - F.col("dropoffs")) / F.col("activity"), 6),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("abs_imbalance_ratio", F.abs(F.col("imbalance_ratio")))
        .filter(F.col("activity") >= activity_threshold)
    )

    #Join with taxi_zone_lookup to get names
    enriched = (
        metrics
        .join(
            zones_df.select(
                F.col("location_id").alias("z_location_id"),
                "borough", "zone", "service_zone",
            ),
            metrics["location_id"] == F.col("z_location_id"),
            "left",
        )
        .drop("z_location_id")
        .fillna({"borough": "Unknown", "zone": "Unknown", "service_zone": "Unknown"})
    )

    result_cols = [
        "hour", "location_id", "borough", "zone", "service_zone",
        "pickups", "dropoffs", "activity", "net_pickups",
        F.round("imbalance_ratio", 6).alias("imbalance_ratio"),
        F.round("abs_imbalance_ratio", 6).alias("abs_imbalance_ratio"),
    ]

    base = enriched.select(*result_cols)

    #Top K zones with the most pickups
    top_positive = (
        base
        .filter(F.col("net_pickups") > 0)
        .orderBy(F.desc("net_pickups"))
        .limit(top_k)
    )

    #Top K zones with the most dropoffs
    top_negative = (
        base
        .filter(F.col("net_pickups") < 0)
        .orderBy(F.asc("net_pickups"))
        .limit(top_k)
    )

    #Top K zones with the highest absolute imbalance ratio
    top_abs = (
        base
        .filter(F.col("abs_imbalance_ratio").isNotNull())
        .orderBy(F.desc("abs_imbalance_ratio"))
        .limit(top_k)
    )

    return top_positive, top_negative, top_abs


def build_hourly_summary(
    pickups_df: DataFrame,
    dropoffs_df: DataFrame,
    activity_threshold: int,
) -> DataFrame:
    
    #Roll up by pickup_hour to find which hour has the biggest spatial imbalance across zones
    joined = (
        pickups_df.alias("p")
        .join(dropoffs_df.alias("d"), on=["hour", "location_id"], how="full_outer")
        .select(
            F.coalesce(F.col("p.hour"),    F.col("d.hour")).alias("hour"),
            F.coalesce(F.col("p.pickups"), F.lit(0)).alias("pickups"),
            F.coalesce(F.col("d.dropoffs"),F.lit(0)).alias("dropoffs"),
        )
        .withColumn("activity",    F.col("pickups") + F.col("dropoffs"))
        .withColumn("net_pickups", F.col("pickups") - F.col("dropoffs"))
        .withColumn(
            "imbalance_ratio",
            F.when(F.col("activity") > 0,
                   (F.col("pickups") - F.col("dropoffs")) / F.col("activity"))
            .otherwise(None),
        )
        .withColumn("abs_imbalance_ratio", F.abs(F.col("imbalance_ratio")))
        .filter(F.col("activity") >= activity_threshold)
    )

    return (
        joined
        .groupBy("hour")
        .agg(
            F.sum(F.abs(F.col("net_pickups"))).alias("total_abs_net_pickups"),
            F.round(F.avg("abs_imbalance_ratio"), 6).alias("mean_abs_imbalance_ratio"),
        )
        .orderBy(F.desc("mean_abs_imbalance_ratio"))
    )


def main() -> None:
    args = parse_args()
    from common import build_personalization
    personalization = build_personalization(args.student_id)

    top_k             = args.top_k if args.top_k is not None else personalization.top_k
    activity_threshold = args.activity_threshold

    spark = create_spark("q6_df_parquet")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        trips_raw = read_2024_parquet(spark, args.input_base)
        zones_raw = read_zones_parquet(spark, args.input_base)

        filtered = filter_trips(
            trips_raw,
            days=personalization.days_2024,
            hours=personalization.hours,
        ).cache()

        #Cache
        pickups_df  = build_pickups(filtered).cache()
        dropoffs_df = build_dropoffs(filtered).cache()

        top_positive, top_negative, top_abs = build_imbalance(
            pickups_df, dropoffs_df, zones_raw,
            activity_threshold=activity_threshold,
            top_k=top_k,
        )

        hourly_summary = build_hourly_summary(
            pickups_df, dropoffs_df,
            activity_threshold=activity_threshold,
        )

        table_base = join_hdfs_path(args.output_base, "results", "tables", "q6")

        paths = {
            "top_positive_net_pickups": join_hdfs_path(table_base, "df_parquet_top_positive"),
            "top_negative_net_pickups": join_hdfs_path(table_base, "df_parquet_top_negative"),
            "top_abs_imbalance":        join_hdfs_path(table_base, "df_parquet_top_abs"),
            "hourly_summary":           join_hdfs_path(table_base, "df_parquet_hourly_summary"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q6_df_parquet_metrics.json")
        plan_path    = join_hdfs_path(args.output_base, "results", "plans",   "q6_df_parquet_plan.txt")

        write_small_csv_table(top_positive,   paths["top_positive_net_pickups"], args.mode)
        write_small_csv_table(top_negative,   paths["top_negative_net_pickups"], args.mode)
        write_small_csv_table(top_abs,        paths["top_abs_imbalance"],        args.mode)
        write_small_csv_table(hourly_summary, paths["hourly_summary"],           args.mode)

        #Save the plan
        plan_text = top_abs._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q6_df",
            "input_format": "parquet",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "days_2024": personalization.days_2024,
                "hours": personalization.hours,
                "top_k": top_k,
            },
            "activity_threshold": activity_threshold,
            "filters": {
                "duration_minutes_gt_0": True,
                "total_amount_gt_0": True,
            },
            "imbalance_policy": {
                "time_base": "pickup_hour",
                "join_type": "full_outer",
                "null_fill": 0,
                "imbalance_ratio_formula": "(pickups - dropoffs) / (pickups + dropoffs)",
                "imbalance_ratio_defined_when": "activity > 0",
            },
            "output_paths": {**paths, "metrics": metrics_path, "plan": plan_path},
            "top_positive_preview":   rows_to_dicts(top_positive.collect()),
            "top_negative_preview":   rows_to_dicts(top_negative.collect()),
            "top_abs_preview":        rows_to_dicts(top_abs.collect()),
            "hourly_summary_preview": rows_to_dicts(hourly_summary.collect()),
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

        pickups_df.unpersist()
        dropoffs_df.unpersist()
        filtered.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()