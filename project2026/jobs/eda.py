from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

#helpers from common.py
from common import (
    Timer,
    create_spark,
    hdfs_file_count,
    hdfs_size_bytes,
    join_hdfs_path,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:

    #keep student id and HDFS paths outside the code so the job is reusable
    parser = argparse.ArgumentParser(
        description="Exploratory Data Analysis on prepared NYC taxi Parquet datasets."
    )
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument(
        "--input-base",
        required=True,
        help=(
            "Project output base, i.e. the same value passed as --output-base to "
            "prepare_parquet.py (e.g. hdfs://.../user/$USER/project2026). "
            "The job appends data/parquet/ internally to locate the Parquet datasets."
        ),
    )
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "errorifexists"])
    return parser.parse_args()


def make_hdfs_dir_if_missing(spark, hdfs_dir: str) -> None:

    #Create an HDFS directory if it does not already exist

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    file_system = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = jvm.org.apache.hadoop.fs.Path(hdfs_dir)

    if not file_system.exists(path):
        file_system.mkdirs(path)


def make_json_safe(value: Any) -> Any:

    #Convert Spark and Python objects into values that json.dumps can save

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

    #Convert a list of Spark Row objects into a list of dictionaries (useful for saving small samples in JSON)
    return [
        make_json_safe(row.asDict(recursive=True))
        for row in rows
    ]


def write_small_csv_table(df: DataFrame, output_path: str, mode: str = "overwrite") -> None:

    #Save a small result table to HDFS as CSV so it is easy to inspect
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", "true")
        .csv(output_path)
    )


def get_schema_info(df: DataFrame) -> dict[str, Any]:

    #Store schema information in multiple formats (supports the EDA requirement for schema dumps)
    return {
        "tree_string": df._jdf.schema().treeString(),
        "simple_string": df.schema.simpleString(),
        "json": json.loads(df.schema.json()),
    }


def get_describe_output(df: DataFrame) -> list[dict[str, Any]]:
    return rows_to_dicts(df.describe().collect())


def calculate_null_percentages(df: DataFrame, dataset_name: str) -> DataFrame:

    #Calculate null count and null percentage for every column of a dataset
    total_rows = df.count()

    #Build one aggregation expression per column
    null_count_expressions = [
        F.sum(
            F.when(F.col(column_name).isNull(), F.lit(1)).otherwise(F.lit(0))
        ).alias(column_name)
        for column_name in df.columns
    ]

    null_counts_row = df.agg(*null_count_expressions).collect()[0].asDict()

    null_rows = []
    for column_name, null_count in null_counts_row.items():
        null_count = int(null_count or 0)
        null_percentage = (null_count / total_rows * 100.0) if total_rows else 0.0
        null_rows.append((dataset_name, column_name, null_count, null_percentage))

    spark = df.sparkSession

    return (
        spark.createDataFrame(
            null_rows,
            ["dataset", "column_name", "null_count", "null_percentage"],
        )
        .orderBy(
            F.desc("null_percentage"),
            F.desc("null_count"),
            F.asc("column_name"),
        )
    )


def calculate_pickup_hour_counts(df: DataFrame, dataset_name: str) -> DataFrame:

    #Count trips per pickup_hour for one dataset (for combined 2015 and 2024 pickup hour distribution)
    return (
        df.groupBy("pickup_hour")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .withColumn("dataset", F.lit(dataset_name))
        .select("dataset", "pickup_hour", "trip_count")
        .orderBy("pickup_hour")
    )


def calculate_pickup_day_counts_2024(df_2024: DataFrame) -> DataFrame:

    #Count trips per pickup day for the 2024 dataset

    return (
        df_2024.groupBy("pickup_day")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .orderBy("pickup_day")
    )


def calculate_numeric_summary(df: DataFrame, dataset_name: str) -> dict[str, Any]:

    #Calculate summary statistics for important numeric columns
    #For each available column we keep count, min, percentiles, max and average (helps detect anomalies)
    wanted_columns = [
        "trip_distance",
        "trip_distance_km",
        "duration_minutes",
        "fare_amount",
        "tip_amount",
        "total_amount",
    ]

    existing_columns = [
        column_name
        for column_name in wanted_columns
        if column_name in df.columns
    ]

    summary_expressions = []

    for column_name in existing_columns:
        summary_expressions.extend([
            F.count(F.col(column_name)).alias(f"{column_name}_non_null_count"),
            F.min(F.col(column_name)).alias(f"{column_name}_min"),
            F.expr(f"percentile_approx({column_name}, 0.25)").alias(f"{column_name}_p25"),
            F.expr(f"percentile_approx({column_name}, 0.50)").alias(f"{column_name}_median"),
            F.expr(f"percentile_approx({column_name}, 0.75)").alias(f"{column_name}_p75"),
            F.max(F.col(column_name)).alias(f"{column_name}_max"),
            F.avg(F.col(column_name)).alias(f"{column_name}_avg"),
        ])

    summary_row = df.agg(*summary_expressions).collect()[0].asDict()

    return {
        "dataset": dataset_name,
        "summary": make_json_safe(summary_row),
    }


def calculate_log_histogram(df: DataFrame, dataset_name: str, column_name: str) -> DataFrame:

    #Create histogram buckets using floor(log10(value)) where negative and zero values are excluded (log undefined) they are counted separately
    #with count_negative_zero_positive and appear in the metrics JSON and this aggregated table is consumed by plot_eda.py to produce the final PNG
    return (
        df.filter(F.col(column_name).isNotNull() & (F.col(column_name) > 0))
        .withColumn(
            "log10_floor_bucket",
            F.floor(F.log10(F.col(column_name))).cast("int"),
        )
        .groupBy("log10_floor_bucket")
        .agg(F.count(F.lit(1)).alias("count"))
        .withColumn("dataset", F.lit(dataset_name))
        .withColumn("column_name", F.lit(column_name))
        .select("dataset", "column_name", "log10_floor_bucket", "count")
        .orderBy("log10_floor_bucket")
    )


def count_negative_zero_positive(df: DataFrame, dataset_name: str, column_name: str) -> dict[str, Any]:

    #Count negative, zero and positive values for one numeric column
    # taxi data may contain negative or zero values that must be flagged in the EDA report

    counts_row = df.agg(
        F.sum(F.when(F.col(column_name) < 0, F.lit(1)).otherwise(F.lit(0))).alias("negative_count"),
        F.sum(F.when(F.col(column_name) == 0, F.lit(1)).otherwise(F.lit(0))).alias("zero_count"),
        F.sum(F.when(F.col(column_name) > 0, F.lit(1)).otherwise(F.lit(0))).alias("positive_count"),
        F.count(F.lit(1)).alias("total_rows"),
    ).collect()[0].asDict()

    return {
        "dataset": dataset_name,
        "column_name": column_name,
        "negative_count": int(counts_row["negative_count"] or 0),
        "zero_count": int(counts_row["zero_count"] or 0),
        "positive_count": int(counts_row["positive_count"] or 0),
        "total_rows": int(counts_row["total_rows"] or 0),
    }


def check_2024_zone_joinability(df_2024: DataFrame, zones_df: DataFrame) -> dict[str, Any]:

    #Check if pu_location_id in 2024 exists in taxi_zone_lookup.

    unique_zone_ids = (
        zones_df
        .select(F.col("location_id").alias("zone_location_id"))
        .dropDuplicates()
    )

    joined_df = df_2024.join(
        unique_zone_ids,
        df_2024["pu_location_id"] == unique_zone_ids["zone_location_id"],
        "left",
    )

    join_counts = joined_df.agg(
        F.count(F.lit(1)).alias("total_2024_rows"),
        F.sum(
            F.when(F.col("zone_location_id").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        ).alias("matched_rows"),
        F.sum(
            F.when(F.col("zone_location_id").isNull(), F.lit(1)).otherwise(F.lit(0))
        ).alias("unmatched_rows"),
    ).collect()[0].asDict()

    total_rows = int(join_counts["total_2024_rows"] or 0)
    matched_rows = int(join_counts["matched_rows"] or 0)
    unmatched_rows = int(join_counts["unmatched_rows"] or 0)
    matched_percentage = (matched_rows / total_rows * 100.0) if total_rows else 0.0

    return {
        "total_2024_rows": total_rows,
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "matched_percentage": matched_percentage,
    }


def calculate_top_10_pickup_zones(df_2024: DataFrame, zones_df: DataFrame) -> DataFrame:

    #Find the 10 most common pickup zones in 2024 and join with taxi_zone_lookup for borough and zone names

    return (
        df_2024
        .groupBy("pu_location_id")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .join(
            zones_df,
            F.col("pu_location_id") == F.col("location_id"),
            "left",
        )
        .select(
            "pu_location_id",
            "borough",
            "zone",
            "service_zone",
            "trip_count",
        )
        .orderBy(F.desc("trip_count"))
        .limit(10)
    )


def collect_anomaly_examples(df: DataFrame, dataset_name: str) -> dict[str, Any]:

    #Collect small samples of extreme and suspicious records for the EDA report section
    #Only 5 rows per category are collected so collect() is safe here

    useful_columns = [
        column_name
        for column_name in [
            "vendor_id",
            "pickup_ts",
            "dropoff_ts",
            "pickup_day",
            "pickup_hour",
            "trip_distance",
            "duration_minutes",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "pu_location_id",
            "do_location_id",
        ]
        if column_name in df.columns
    ]

    largest_distance_rows = (
        df.select(*useful_columns)
        .filter(F.col("trip_distance").isNotNull())
        .orderBy(F.desc("trip_distance"))
        .limit(5)
        .collect()
    )

    longest_duration_rows = (
        df.select(*useful_columns)
        .filter(F.col("duration_minutes").isNotNull())
        .orderBy(F.desc("duration_minutes"))
        .limit(5)
        .collect()
    )

    negative_fare_rows = (
        df.select(*useful_columns)
        .filter(F.col("fare_amount") < 0)
        .orderBy(F.asc("fare_amount"))
        .limit(5)
        .collect()
    )

    zero_or_negative_duration_rows = (
        df.select(*useful_columns)
        .filter(F.col("duration_minutes") <= 0)
        .orderBy(F.asc("duration_minutes"))
        .limit(5)
        .collect()
    )

    negative_total_amount_rows = (
        df.select(*useful_columns)
        .filter(F.col("total_amount") < 0)
        .orderBy(F.asc("total_amount"))
        .limit(5)
        .collect()
    )

    return {
        "dataset": dataset_name,
        "largest_trip_distance": rows_to_dicts(largest_distance_rows),
        "largest_duration_minutes": rows_to_dicts(longest_duration_rows),
        "negative_fare_amount": rows_to_dicts(negative_fare_rows),
        "zero_or_negative_duration": rows_to_dicts(zero_or_negative_duration_rows),
        "negative_total_amount": rows_to_dicts(negative_total_amount_rows),
    }


def main() -> None:
    args = parse_args()

    spark = create_spark("eda")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as total_timer:
        #Input paths: parquet_input_base sits inside the prepare_parquet output tree
        parquet_input_base = join_hdfs_path(args.input_base, "data", "parquet")

        path_2015 = join_hdfs_path(parquet_input_base, "yellow_tripdata_2015")
        path_2024 = join_hdfs_path(parquet_input_base, "yellow_tripdata_2024")
        path_zones = join_hdfs_path(parquet_input_base, "taxi_zone_lookup")

        #Output paths: metrics JSON and EDA result tables.
        metrics_dir    = join_hdfs_path(args.output_base, "results", "metrics")
        eda_tables_dir = join_hdfs_path(args.output_base, "results", "tables", "eda")
        metrics_output_path = join_hdfs_path(metrics_dir, "eda_metrics.json")

        #Ensure both output directories exist before any write
        make_hdfs_dir_if_missing(spark, metrics_dir)
        make_hdfs_dir_if_missing(spark, eda_tables_dir)

        print(f"[eda] Reading 2015 Parquet from: {path_2015}", flush=True)
        df_2015 = spark.read.parquet(path_2015)

        print(f"[eda] Reading 2024 Parquet from: {path_2024}", flush=True)
        df_2024 = spark.read.parquet(path_2024)

        print(f"[eda] Reading taxi zone lookup Parquet from: {path_zones}", flush=True)
        zones_df = spark.read.parquet(path_zones)

        #Row counts for the three prepared datasets
        print("[eda] Counting rows", flush=True)
        row_count_2015  = df_2015.count()
        row_count_2024  = df_2024.count()
        row_count_zones = zones_df.count()

        #Schema and describe outputs are required EDA evidence
        print("[eda] Saving schema and describe outputs", flush=True)
        schema_2015  = get_schema_info(df_2015)
        schema_2024  = get_schema_info(df_2024)
        schema_zones = get_schema_info(zones_df)

        describe_2015  = get_describe_output(df_2015)
        describe_2024  = get_describe_output(df_2024)
        describe_zones = get_describe_output(zones_df)

        #Null percentages for all three datasets
        print("[eda] Calculating null percentages", flush=True)
        nulls_2015  = calculate_null_percentages(df_2015,    "yellow_tripdata_2015")
        nulls_2024  = calculate_null_percentages(df_2024,    "yellow_tripdata_2024")
        nulls_zones = calculate_null_percentages(zones_df,   "taxi_zone_lookup")

        all_nulls = (
            nulls_2015
            .unionByName(nulls_2024)
            .unionByName(nulls_zones)
        )

        nulls_output_path = join_hdfs_path(eda_tables_dir, "null_percentages")
        write_small_csv_table(all_nulls, nulls_output_path, args.mode)

        #Hour distribution for 2015 and 2024
        print("[eda] Calculating pickup_hour distribution", flush=True)
        hour_counts_2015 = calculate_pickup_hour_counts(df_2015, "yellow_tripdata_2015")
        hour_counts_2024 = calculate_pickup_hour_counts(df_2024, "yellow_tripdata_2024")

        all_hour_counts = hour_counts_2015.unionByName(hour_counts_2024)

        hour_counts_output_path = join_hdfs_path(eda_tables_dir, "pickup_hour_distribution")
        write_small_csv_table(all_hour_counts, hour_counts_output_path, args.mode)

        #Day distribution for 2024
        print("[eda] Calculating pickup_day distribution for 2024", flush=True)
        day_counts_2024 = calculate_pickup_day_counts_2024(df_2024)

        day_counts_output_path = join_hdfs_path(eda_tables_dir, "pickup_day_distribution_2024")
        write_small_csv_table(day_counts_2024, day_counts_output_path, args.mode)

        #Basic statistics
        print("[eda] Calculating numeric summaries", flush=True)
        numeric_summary_2015 = calculate_numeric_summary(df_2015, "yellow_tripdata_2015")
        numeric_summary_2024 = calculate_numeric_summary(df_2024, "yellow_tripdata_2024")

        #Histogram bucket tables consumed later by plot_eda.py to produce PNGs
        print("[eda] Calculating log histogram buckets", flush=True)
        trip_distance_histogram = (
            calculate_log_histogram(df_2015, "yellow_tripdata_2015", "trip_distance")
            .unionByName(
                calculate_log_histogram(df_2024, "yellow_tripdata_2024", "trip_distance")
            )
        )

        trip_distance_histogram_path = join_hdfs_path(eda_tables_dir, "trip_distance_log_histogram")
        write_small_csv_table(trip_distance_histogram, trip_distance_histogram_path, args.mode)

        total_amount_histogram = (
            calculate_log_histogram(df_2015, "yellow_tripdata_2015", "total_amount")
            .unionByName(
                calculate_log_histogram(df_2024, "yellow_tripdata_2024", "total_amount")
            )
        )

        total_amount_histogram_path = join_hdfs_path(eda_tables_dir, "total_amount_log_histogram")
        write_small_csv_table(total_amount_histogram, total_amount_histogram_path, args.mode)

        #Count suspicious negative and zero values (feeds the total_amount histogram annotation)
        print("[eda] Counting negative, zero and positive values", flush=True)
        negative_zero_counts = [
            count_negative_zero_positive(df_2015, "yellow_tripdata_2015", "trip_distance"),
            count_negative_zero_positive(df_2024, "yellow_tripdata_2024", "trip_distance"),
            count_negative_zero_positive(df_2015, "yellow_tripdata_2015", "fare_amount"),
            count_negative_zero_positive(df_2024, "yellow_tripdata_2024", "fare_amount"),
            count_negative_zero_positive(df_2015, "yellow_tripdata_2015", "total_amount"),
            count_negative_zero_positive(df_2024, "yellow_tripdata_2024", "total_amount"),
        ]

        #Check if the 2024 pickup location IDs can join to the lookup table
        print("[eda] Checking 2024 pickup location joinability", flush=True)
        zone_joinability = check_2024_zone_joinability(df_2024, zones_df)

        #Find most common pickup zones in 2024
        print("[eda] Calculating top 10 pickup zones", flush=True)
        top_pickup_zones = calculate_top_10_pickup_zones(df_2024, zones_df)

        top_pickup_zones_path = join_hdfs_path(eda_tables_dir, "top_10_pickup_zones_2024")
        write_small_csv_table(top_pickup_zones, top_pickup_zones_path, args.mode)

        #Collect anomaly examples for the report
        print("[eda] Collecting anomaly examples", flush=True)
        anomalies_2015 = collect_anomaly_examples(df_2015, "yellow_tripdata_2015")
        anomalies_2024 = collect_anomaly_examples(df_2024, "yellow_tripdata_2024")

        #Final JSON metrics
        eda_metrics = {
            "job": "eda",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "input_base": args.input_base,
            "output_base": args.output_base,
            "paths": {
                "yellow_tripdata_2015": path_2015,
                "yellow_tripdata_2024": path_2024,
                "taxi_zone_lookup": path_zones,
                "metrics_output_path": metrics_output_path,
                "eda_tables_dir": eda_tables_dir,
            },
            "row_counts": {
                "yellow_tripdata_2015": row_count_2015,
                "yellow_tripdata_2024": row_count_2024,
                "taxi_zone_lookup": row_count_zones,
            },
            "schemas": {
                "yellow_tripdata_2015": schema_2015,
                "yellow_tripdata_2024": schema_2024,
                "taxi_zone_lookup": schema_zones,
            },
            "describe_outputs": {
                "yellow_tripdata_2015": describe_2015,
                "yellow_tripdata_2024": describe_2024,
                "taxi_zone_lookup": describe_zones,
            },
            "numeric_summaries": {
                "yellow_tripdata_2015": numeric_summary_2015,
                "yellow_tripdata_2024": numeric_summary_2024,
            },
            "negative_zero_counts": negative_zero_counts,
            "joinability_2024_pu_location_id": zone_joinability,
            "anomaly_samples": {
                "yellow_tripdata_2015": anomalies_2015,
                "yellow_tripdata_2024": anomalies_2024,
            },
            "table_outputs": {
                "null_percentages":              nulls_output_path,
                "pickup_hour_distribution":      hour_counts_output_path,
                "pickup_day_distribution_2024":  day_counts_output_path,
                "trip_distance_log_histogram":   trip_distance_histogram_path,
                "total_amount_log_histogram":    total_amount_histogram_path,
                "top_10_pickup_zones_2024":      top_pickup_zones_path,
            },
            "hdfs_output_sizes_bytes": {
                "eda_tables_dir": hdfs_size_bytes(spark, eda_tables_dir),
            },
            "hdfs_file_counts": {
                "eda_tables_dir": hdfs_file_count(spark, eda_tables_dir),
            },
            "elapsed_seconds": round(total_timer.elapsed_seconds, 3),
        }

        #Write EDA metrics JSON to HDFS
        write_text_hdfs(
            spark,
            metrics_output_path,
            json.dumps(make_json_safe(eda_metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        #Print metrics for debugging
        print(json.dumps(make_json_safe(eda_metrics), indent=2, ensure_ascii=False), flush=True)

    spark.stop()


if __name__ == "__main__":
    main()