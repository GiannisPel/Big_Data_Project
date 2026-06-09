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
    create_spark,
    join_hdfs_path,
    write_text_hdfs,
)


MIN_TRIPS_PER_HALF = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q6 OD-Halves scaling experiment.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--config", required=True, choices=["A", "B", "C"])
    parser.add_argument("--run", required=True, choices=["cold", "warm"])
    parser.add_argument("--min-trips", type=int, default=MIN_TRIPS_PER_HALF)
    parser.add_argument("--top-k", type=int, default=20)
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


def read_full_2024(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2024")
    return spark.read.parquet(path)


def apply_validity_filters(df: DataFrame) -> DataFrame:
    #duration_minutes > 0, trip_distance > 0, fare_amount > 0.
    return (
        df
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("pickup_day").isNotNull())
        .filter(F.col("pickup_hour").isNotNull())
        .filter(F.col("pu_location_id").isNotNull())
        .filter(F.col("do_location_id").isNotNull())
    )


def aggregate_half(df: DataFrame, half_label: str) -> DataFrame:
    return (
        df
        .groupBy("pu_location_id", "do_location_id", "pickup_hour")
        .agg(
            F.count(F.lit(1)).alias(f"trips_{half_label}"),
            F.avg("fare_amount").alias(f"avg_fare_{half_label}"),
            F.avg("total_amount").alias(f"avg_total_{half_label}"),
        )
    )


def build_od_comparison(
    df: DataFrame,
    min_trips: int,
    top_k: int,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Split 2024 dataset into two halves:
    H1 = pickup_day <= 15
    H2 = pickup_day > 15
    Aggregate each half by pu_location_id, do_location_id, pickup_hour, join both halves, then compute change percentages
    """
    h1 = aggregate_half(df.filter(F.col("pickup_day") <= 15), "h1")
    h2 = aggregate_half(df.filter(F.col("pickup_day") > 15), "h2")

    join_keys = ["pu_location_id", "do_location_id", "pickup_hour"]

    compared = (
        h1
        .join(h2, on=join_keys, how="inner")
        .filter(F.col("trips_h1") >= min_trips)
        .filter(F.col("trips_h2") >= min_trips)
        .withColumn(
            "trips_change_pct",
            (F.col("trips_h2") - F.col("trips_h1")) * F.lit(100.0) / F.col("trips_h1"),
        )
        .withColumn(
            "fare_change_pct",
            F.when(
                F.col("avg_fare_h1") > 0,
                (F.col("avg_fare_h2") - F.col("avg_fare_h1")) * F.lit(100.0) / F.col("avg_fare_h1"),
            ),
        )
        .withColumn(
            "total_amount_change_pct",
            F.when(
                F.col("avg_total_h1") > 0,
                (F.col("avg_total_h2") - F.col("avg_total_h1")) * F.lit(100.0) / F.col("avg_total_h1"),
            ),
        )
        .select(
            *join_keys,
            "trips_h1",
            "trips_h2",
            F.round("trips_change_pct", 4).alias("trips_change_pct"),
            F.round("avg_fare_h1", 4).alias("avg_fare_h1"),
            F.round("avg_fare_h2", 4).alias("avg_fare_h2"),
            F.round("fare_change_pct", 4).alias("fare_change_pct"),
            F.round("avg_total_h1", 4).alias("avg_total_h1"),
            F.round("avg_total_h2", 4).alias("avg_total_h2"),
            F.round("total_amount_change_pct", 4).alias("total_amount_change_pct"),
        )
        .cache()
    )

    top_increase = (
        compared
        .orderBy(F.desc("trips_change_pct"), F.desc("trips_h2"))
        .limit(top_k)
    )

    top_decrease = (
        compared
        .orderBy(F.asc("trips_change_pct"), F.desc("trips_h1"))
        .limit(top_k)
    )

    return compared, top_increase, top_decrease


def main() -> None:
    args = parse_args()

    app_name = f"q6_od_halves_config{args.config}_{args.run}"
    spark = create_spark(app_name)
    spark.sparkContext.setLogLevel("WARN")

    spark_conf = spark.sparkContext.getConf()

    executor_configs = {
        "A": {"instances": 2, "cores": 2, "memory": "2G"},
        "B": {"instances": 4, "cores": 2, "memory": "2G"},
        "C": {"instances": 8, "cores": 2, "memory": "2G"},
    }

    with Timer() as timer:
        raw_df = read_full_2024(spark, args.input_base)
        valid_df = apply_validity_filters(raw_df).cache()

        valid_row_count = valid_df.count()

        compared, top_increase, top_decrease = build_od_comparison(
            df=valid_df,
            min_trips=args.min_trips,
            top_k=args.top_k,
        )

        compared_pair_count = compared.count()

        label = f"od_halves_config{args.config}_{args.run}"
        table_base = join_hdfs_path(args.output_base, "results", "tables", "q6")

        paths = {
            "top_increase": join_hdfs_path(table_base, f"{label}_top_increase"),
            "top_decrease": join_hdfs_path(table_base, f"{label}_top_decrease"),
        }

        metrics_path = join_hdfs_path(
            args.output_base,
            "results",
            "metrics",
            f"q6_{label}_metrics.json",
        )
        plan_path = join_hdfs_path(
            args.output_base,
            "results",
            "plans",
            f"q6_{label}_plan.txt",
        )

        write_small_csv_table(top_increase, paths["top_increase"], args.mode)
        write_small_csv_table(top_decrease, paths["top_decrease"], args.mode)

        plan_text = top_increase._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q6_od_halves",
            "student_id": args.student_id,
            "input_format": "parquet",
            "config": args.config,
            "run_type": args.run,
            "spark_application_id": spark.sparkContext.applicationId,
            "executor_config_expected": executor_configs[args.config],
            "executor_config_from_spark": {
                "spark.executor.instances": spark_conf.get("spark.executor.instances", "unknown"),
                "spark.executor.cores": spark_conf.get("spark.executor.cores", "unknown"),
                "spark.executor.memory": spark_conf.get("spark.executor.memory", "unknown"),
            },
            "halves": {
                "H1": "pickup_day <= 15",
                "H2": "pickup_day > 15",
            },
            "filters": {
                "personalization_applied": False,
                "duration_minutes_gt_0": True,
                "trip_distance_gt_0": True,
                "fare_amount_gt_0": True,
                "min_trips_h1": args.min_trips,
                "min_trips_h2": args.min_trips,
            },
            "grouping_keys": ["pu_location_id", "do_location_id", "pickup_hour"],
            "join_type": "inner",
            "valid_row_count": valid_row_count,
            "compared_pair_count": compared_pair_count,
            "output_paths": {
                **paths,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "top_increase_preview": rows_to_dicts(top_increase.collect()),
            "top_decrease_preview": rows_to_dicts(top_decrease.collect()),
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

        compared.unpersist()
        valid_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
