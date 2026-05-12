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
    schema_2024_raw,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:
    #Set up CLI arguments so we can run this from the terminal
    parser = argparse.ArgumentParser(description="Q1 Demand analysis using DataFrame API.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-format", required=True, choices=["csv", "parquet"])
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "errorifexists"])
    return parser.parse_args()


def make_json_safe(value: Any) -> Any:
   
    #Convert them all to basic python types before serializing
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Row):
        #Convert it to a dict recursively
        return {k: make_json_safe(v) for k, v in value.asDict(recursive=True).items()}
    if isinstance(value, list):
        return [make_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    return value


def delete_hdfs_path_if_exists(spark, path: str) -> None:
    #This avoids errors when rerunning the job and the output dir already exists
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)

    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True) 


def write_small_csv_table(df: DataFrame, path: str, mode: str) -> None:
    #coalesce(1) merges everything into a single partition -> single output file
    (
        df.coalesce(1)
        .write
        .mode(mode)
        .option("header", "true")
        .csv(path)
    )


def read_2024_csv(spark, input_base: str) -> DataFrame:
    input_path = join_hdfs_path(input_base, "yellow_tripdata_2024.csv")

    raw_df = (
        spark.read
        .option("header", "true")
        .schema(schema_2024_raw())
        .csv(input_path)
    )

    #rename and cast everything here
    return raw_df.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("pickup_ts"),
        F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("dropoff_ts"),
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


def read_2024_parquet(spark, input_base: str) -> DataFrame:
    input_path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2024")
    return spark.read.parquet(input_path)


def add_q1_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_day", F.dayofmonth("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        #duration in minutes
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
        .withColumn("weekday", F.date_format("pickup_ts", "E"))  #"Mon", "Tue"
        #1 = Sunday and 7 = Saturday in Spark
        .withColumn("is_weekend", F.dayofweek("pickup_ts").isin([1, 7]))
        .withColumn(
            "time_band",
            F.when((F.col("pickup_hour") >= 6) & (F.col("pickup_hour") <= 11), F.lit("Morning"))
            .when((F.col("pickup_hour") >= 12) & (F.col("pickup_hour") <= 16), F.lit("Afternoon"))
            .when((F.col("pickup_hour") >= 17) & (F.col("pickup_hour") <= 21), F.lit("Evening"))
            .otherwise(F.lit("Late")),
        )
    )


def filter_q1_window(df: DataFrame, days_2024: list[int], hours: list[int]) -> DataFrame:
    #remove bad/invalid rows before doing any aggregation
    return (
        df
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("pickup_ts") <= F.col("dropoff_ts"))  #dropoff cant be before pickup
        .filter(F.col("duration_minutes") > 0)              #dont include negative durations
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("pickup_day").isin(days_2024))
        .filter(F.col("pickup_hour").isin(hours))
    )


def calculate_q1_top_hours(filtered_df: DataFrame, top_k: int) -> DataFrame:
    #group by date+hour and compute summary stats per group
    grouped_df = (
        filtered_df
        .groupBy("pickup_date", "pickup_hour", "weekday", "is_weekend", "time_band")
        .agg(
            F.count(F.lit(1)).alias("trips"),                            # total trip count
            F.countDistinct("pu_location_id").alias("unique_pickup_zones"),
            F.avg("passenger_count").alias("avg_passenger_count"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.sum("total_amount").alias("total_revenue"),
        )
    )

    total_window = Window.partitionBy()

    return (
        grouped_df
        .withColumn("total_trips_in_personal_window", F.sum("trips").over(total_window))
        .withColumn(
            "trips_share_in_personal_window",
            F.col("trips") / F.col("total_trips_in_personal_window"),  # fraction of all trips
        )
        .select(
            "pickup_date",
            "pickup_hour",
            "weekday",
            "is_weekend",
            "time_band",
            "trips",
            "unique_pickup_zones",
            F.round("avg_passenger_count", 4).alias("avg_passenger_count"),
            F.round("avg_duration_minutes", 4).alias("avg_duration_minutes"),
            F.round("avg_trip_distance", 4).alias("avg_trip_distance"),
            F.round("avg_total_amount", 4).alias("avg_total_amount"),
            F.round("total_revenue", 4).alias("total_revenue"),
            F.round("trips_share_in_personal_window", 6).alias("trips_share_in_personal_window"),
        )
        #sort by busiest hours
        .orderBy(
            F.desc("trips"),
            F.desc("total_revenue"),
            F.asc("pickup_date"),
            F.asc("pickup_hour"),
        )
        .limit(top_k)  #only keep the top K rows as specified in personalization
    )


def calculate_time_band_summary(filtered_df: DataFrame) -> DataFrame:
    #same idea as top hours but grouped by time band (Morning/Afternoon/Evening/Late)
    grouped_df = (
        filtered_df
        .groupBy("time_band")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.countDistinct("pu_location_id").alias("unique_pickup_zones"),
            F.avg("passenger_count").alias("avg_passenger_count"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.sum("total_amount").alias("total_revenue"),
        )
    )

    total_window = Window.partitionBy()

    return (
        grouped_df
        .withColumn("total_trips_in_personal_window", F.sum("trips").over(total_window))
        .withColumn(
            "trips_share_in_personal_window",
            F.col("trips") / F.col("total_trips_in_personal_window"),
        )
        .select(
            "time_band",
            "trips",
            "unique_pickup_zones",
            F.round("avg_passenger_count", 4).alias("avg_passenger_count"),
            F.round("avg_duration_minutes", 4).alias("avg_duration_minutes"),
            F.round("avg_trip_distance", 4).alias("avg_trip_distance"),
            F.round("avg_total_amount", 4).alias("avg_total_amount"),
            F.round("total_revenue", 4).alias("total_revenue"),
            F.round("trips_share_in_personal_window", 6).alias("trips_share_in_personal_window"),
        )
        .orderBy(F.desc("trips"))  #busiest time band first
    )


def main() -> None:
    args = parse_args()
    #personalization
    personalization = build_personalization(args.student_id)

    spark = create_spark(f"q1_df_{args.input_format}")
    spark.sparkContext.setLogLevel("WARN")  # reduce console noise during execution

    with Timer() as timer:
        #load data
        if args.input_format == "csv":
            input_df = read_2024_csv(spark, args.input_base)
        else:
            input_df = read_2024_parquet(spark, args.input_base)

        enriched_df = add_q1_columns(input_df)

        #Q1 filters using personalized days and hours
        filtered_df = filter_q1_window(
            enriched_df,
            days_2024=personalization.days_2024,
            hours=personalization.hours,
        ).cache()  #cache because theyre used for aggregation below

        filtered_count = filtered_df.count()

        top_hours_df = calculate_q1_top_hours(filtered_df, personalization.top_k)
        time_band_df = calculate_time_band_summary(filtered_df)

        result_dir = join_hdfs_path(args.output_base, "results", "tables", "q1", f"df_{args.input_format}")
        time_band_dir = join_hdfs_path(args.output_base, "results", "tables", "q1", f"df_{args.input_format}_time_band")
        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", f"q1_df_{args.input_format}_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", f"q1_df_{args.input_format}_plan.txt")

        #write results to HDFS as CSV
        write_small_csv_table(top_hours_df, result_dir, args.mode)
        write_small_csv_table(time_band_df, time_band_dir, args.mode)

        #save the query plan for the report
        plan_text = top_hours_df._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        #collect results to driver to include a preview in the metrics JSON
        top_rows = [make_json_safe(row) for row in top_hours_df.collect()]
        time_band_rows = [make_json_safe(row) for row in time_band_df.collect()]

        #build the metrics dict
        metrics = {
            "job": "q1_df",
            "input_format": args.input_format,
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "hours": personalization.hours,
                "days_2024": personalization.days_2024,
                "top_k": personalization.top_k,
            },
            "filters": {
                #document which filters were applied so results are reproducible
                "pickup_ts_lte_dropoff_ts": True,
                "duration_minutes_gt_0": True,
                "trip_distance_gt_0": True,
                "total_amount_gt_0": True,
            },
            "filtered_row_count": filtered_count,
            "output_paths": {
                "top_hours": result_dir,
                "time_band_summary": time_band_dir,
                "metrics": metrics_path,
                "plan": plan_path,
            },
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

        #print to stdout to see results without going to HDFS
        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

        #free the cached RDD now when process is done
        filtered_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()