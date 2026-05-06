from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import (
    Timer,
    create_spark,
    hdfs_file_count,
    hdfs_size_bytes,
    join_hdfs_path,
    schema_2015_raw,
    schema_2024_raw,
    schema_zones_raw,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare taxi CSV datasets into Parquet datasets"
    )
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "errorifexists"])
    return parser.parse_args()


def read_csv(spark, path: str, schema) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def add_common_time_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_day", F.dayofmonth("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / F.lit(60.0),
        )
        .withColumn("trip_distance_km", F.col("trip_distance") * F.lit(1.609344))
    )


def prepare_2015(raw: DataFrame) -> DataFrame:
    prepared = (
        raw
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))
        .select(
            F.col("VendorID").cast("int").alias("vendor_id"),
            "pickup_ts",
            "dropoff_ts",
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
    prepared = add_common_time_columns(prepared)
    return prepared.filter(F.col("pickup_ts").isNotNull() & (F.year("pickup_ts") == 2015))


def prepare_2024(raw: DataFrame) -> DataFrame:
    prepared = (
        raw
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .select(
            F.col("VendorID").cast("int").alias("vendor_id"),
            "pickup_ts",
            "dropoff_ts",
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("trip_distance").cast("double").alias("trip_distance"),
            F.col("RatecodeID").cast("int").alias("rate_code_id"),
            F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
            F.col("PULocationID").cast("int").alias("pu_location_id"),
            F.col("DOLocationID").cast("int").alias("do_location_id"),
            F.col("payment_type").cast("int").alias("payment_type"),
            F.col("fare_amount").cast("double").alias("fare_amount"),
            F.col("extra").cast("double").alias("extra"),
            F.col("mta_tax").cast("double").alias("mta_tax"),
            F.col("tip_amount").cast("double").alias("tip_amount"),
            F.col("tolls_amount").cast("double").alias("tolls_amount"),
            F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
            F.col("total_amount").cast("double").alias("total_amount"),
            F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
            F.col("Airport_fee").cast("double").alias("airport_fee"),
        )
    )
    prepared = add_common_time_columns(prepared)
    return prepared.filter(F.col("pickup_ts").isNotNull() & (F.year("pickup_ts") == 2024))


def prepare_zones(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def timestamp_metrics(raw_with_ts: DataFrame, expected_year: int) -> dict[str, Any]:
    raw_count = raw_with_ts.count()

    bad_ts_count = raw_with_ts.filter(F.col("pickup_ts").isNull()).count()

    wrong_year_count = raw_with_ts.filter(
        F.col("pickup_ts").isNotNull() & (F.year("pickup_ts") != expected_year)
    ).count()

    min_max = raw_with_ts.filter(F.col("pickup_ts").isNotNull()).agg(
        F.min("pickup_ts").alias("min_pickup_ts"),
        F.max("pickup_ts").alias("max_pickup_ts"),
    ).collect()[0]

    return {
        "raw_row_count": raw_count,
        "bad_or_null_pickup_ts_count": bad_ts_count,
        "wrong_year_rejected_count": wrong_year_count,
        "min_pickup_ts": str(min_max["min_pickup_ts"]),
        "max_pickup_ts": str(min_max["max_pickup_ts"]),
    }


def prepare_and_write_trip_dataset(
    *,
    spark,
    dataset_name: str,
    input_path: str,
    output_path: str,
    schema,
    expected_year: int,
    timestamp_format: str,
    prepare_fn,
    partition_columns: list[str],
    mode: str,
) -> dict[str, Any]:
    with Timer() as timer:
        raw = read_csv(spark, input_path, schema)

        raw_with_ts = raw.withColumn(
            "pickup_ts",
            F.to_timestamp("tpep_pickup_datetime", timestamp_format),
        )

        base_metrics = timestamp_metrics(raw_with_ts, expected_year)

        prepared = prepare_fn(raw)
        prepared_count = prepared.count()

        writer = prepared.write.mode(mode).parquet
        if partition_columns:
            prepared.write.mode(mode).partitionBy(*partition_columns).parquet(output_path)
        else:
            prepared.write.mode(mode).parquet(output_path)

    return {
        "dataset": dataset_name,
        "input_path": input_path,
        "output_path": output_path,
        "timestamp_format": timestamp_format,
        "partition_columns": partition_columns,
        "prepared_row_count": prepared_count,
        "conversion_time_seconds": round(timer.elapsed_seconds, 3),
        "parquet_output_size_bytes": hdfs_size_bytes(spark, output_path),
        "parquet_file_count": hdfs_file_count(spark, output_path),
        **base_metrics,
    }


def prepare_and_write_zones(
    *,
    spark,
    input_path: str,
    output_path: str,
    mode: str,
) -> dict[str, Any]:
    with Timer() as timer:
        raw = read_csv(spark, input_path, schema_zones_raw())
        raw_count = raw.count()

        prepared = prepare_zones(raw)
        prepared_count = prepared.count()

        prepared.write.mode(mode).parquet(output_path)

    return {
        "dataset": "taxi_zone_lookup",
        "input_path": input_path,
        "output_path": output_path,
        "partition_columns": [],
        "raw_row_count": raw_count,
        "prepared_row_count": prepared_count,
        "conversion_time_seconds": round(timer.elapsed_seconds, 3),
        "parquet_output_size_bytes": hdfs_size_bytes(spark, output_path),
        "parquet_file_count": hdfs_file_count(spark, output_path),
    }


def main() -> None:
    args = parse_args()

    spark = create_spark("prepare_parquet")
    spark.sparkContext.setLogLevel("WARN")

    run_started_at = datetime.now(timezone.utc).isoformat()

    input_2015 = join_hdfs_path(args.input_base, "yellow_tripdata_2015.csv")
    input_2024 = join_hdfs_path(args.input_base, "yellow_tripdata_2024.csv")
    input_zones = join_hdfs_path(args.input_base, "taxi_zone_lookup.csv")

    parquet_base = join_hdfs_path(args.output_base, "data", "parquet")
    output_2015 = join_hdfs_path(parquet_base, "yellow_tripdata_2015")
    output_2024 = join_hdfs_path(parquet_base, "yellow_tripdata_2024")
    output_zones = join_hdfs_path(parquet_base, "taxi_zone_lookup")

    metrics_path = join_hdfs_path(
        args.output_base,
        "results",
        "metrics",
        "prepare_parquet_metrics.json",
    )

    datasets = []

    datasets.append(
        prepare_and_write_trip_dataset(
            spark=spark,
            dataset_name="yellow_tripdata_2015",
            input_path=input_2015,
            output_path=output_2015,
            schema=schema_2015_raw(),
            expected_year=2015,
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            prepare_fn=prepare_2015,
            partition_columns=["pickup_hour"],
            mode=args.mode,
        )
    )

    datasets.append(
        prepare_and_write_trip_dataset(
            spark=spark,
            dataset_name="yellow_tripdata_2024",
            input_path=input_2024,
            output_path=output_2024,
            schema=schema_2024_raw(),
            expected_year=2024,
            timestamp_format="yyyy-MM-dd'T'HH:mm:ss.SSS",
            prepare_fn=prepare_2024,
            partition_columns=["pickup_day"],
            mode=args.mode,
        )
    )

    datasets.append(
        prepare_and_write_zones(
            spark=spark,
            input_path=input_zones,
            output_path=output_zones,
            mode=args.mode,
        )
    )

    metrics = {
        "job": "prepare_parquet",
        "student_id": args.student_id,
        "spark_application_id": spark.sparkContext.applicationId,
        "run_started_at": run_started_at,
        "run_finished_at": datetime.now(timezone.utc).isoformat(),
        "input_base": args.input_base,
        "output_base": args.output_base,
        "metrics_path": metrics_path,
        "datasets": datasets,
    }

    write_text_hdfs(
        spark,
        metrics_path,
        json.dumps(metrics, indent=2, ensure_ascii=False),
        overwrite=True,
    )

    print(json.dumps(metrics, indent=2, ensure_ascii=False))

    spark.stop()


if __name__ == "__main__":
    main()