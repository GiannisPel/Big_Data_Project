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
    schema_zones_raw,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Q3 revenue, trip performance and high-value pickup zones.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-format", required=True, choices=["csv", "parquet"])
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--variant", default="normal", choices=["normal", "no_pruning"])
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

    #make every column to the right type and give them  names
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


def read_zones_csv(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "taxi_zone_lookup.csv")

    raw_zones = (
        spark.read
        .option("header", "true")
        .schema(schema_zones_raw())
        .csv(path)
    )

    return raw_zones.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )


def read_2024_parquet(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "data", "parquet", "yellow_tripdata_2024")
    return spark.read.parquet(path)


def read_zones_parquet(spark, input_base: str) -> DataFrame:
    path = join_hdfs_path(input_base, "data", "parquet", "taxi_zone_lookup")
    return spark.read.parquet(path)


def add_q3_columns(df: DataFrame) -> DataFrame:
    #export the extra columns we need for filtering and grouping:
    #pickup_date
    #pickup_day - day-of-month (for partition pruning)
    #pickup_hour - hour of pickup (for personal hour filter)
    #duration_minutes - trip length in minutes (difference between drop off and pick up)
    #distance_bucket  - categorical bucketing of trip_distance per the Q3 spec: very_short < 1 mi, short 1-3, medium 3-10, long >= 10
    return (
        df
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_day", F.dayofmonth("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
        .withColumn(
            "distance_bucket",
            F.when(F.col("trip_distance") < 1, F.lit("very_short"))
            .when((F.col("trip_distance") >= 1) & (F.col("trip_distance") < 3), F.lit("short"))
            .when((F.col("trip_distance") >= 3) & (F.col("trip_distance") < 10), F.lit("medium"))
            .otherwise(F.lit("long")),
        )
    )


def filter_q3_window(df: DataFrame, days: list[int], hours: list[int], variant: str) -> DataFrame:
    #Apply all filters required:
    # both timestamps present and pickup <= dropoff (no negative trips)
    # duration, distance, fare, and total_amount all strictly positive
    # only the personal hours are kept
    base_filter = (
        df
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("pickup_ts") <= F.col("dropoff_ts"))
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("pickup_hour").isin(hours))
    )

    #no_pruning variant:
    #Using dayofmonth(pickup_ts) prevents Spark from using partition metadata to skip files -> lets us measure the pruning speedup
    if variant == "no_pruning":
        return base_filter.filter(F.dayofmonth(F.col("pickup_ts")).isin(days))

    #Normal variant: filter on the pre computed pickup_day column so Spark can push the predicate down to Parquet partition pruning
    return base_filter.filter(F.col("pickup_day").isin(days))


def join_pickup_zones(trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    #Left join trips with the zone lookup on pickup location ID
    #also rename zone columns to avoid collisions with any future dropoff zone join.
    clean_zones = zones_df.select(
        F.col("location_id").alias("zone_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    return (
        trips_df
        .join(clean_zones, trips_df["pu_location_id"] == clean_zones["zone_location_id"], "left")
        .drop("zone_location_id")
    )


def calculate_group_metrics(joined_df: DataFrame) -> DataFrame:
    #Pre compute sum per row
    #resulting share = total surcharges / total_amount gives the surcharge fraction per group
    surcharge_sum = (
        F.coalesce(F.col("extra"), F.lit(0.0)) +
        F.coalesce(F.col("mta_tax"), F.lit(0.0)) +
        F.coalesce(F.col("tolls_amount"), F.lit(0.0)) +
        F.coalesce(F.col("improvement_surcharge"), F.lit(0.0)) +
        F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)) +
        F.coalesce(F.col("airport_fee"), F.lit(0.0))
    )

    #Group by (pickup_borough, pickup_zone, distance_bucket) and compute all 8 required metrics
    return (
        joined_df
        #Fill nulls from the left join so Unknown zones form their own group
        .fillna({"pickup_borough": "Unknown", "pickup_zone": "Unknown", "pickup_service_zone": "Unknown"})
        .withColumn("surcharge_components", surcharge_sum)
        .groupBy("pickup_borough", "pickup_zone", "distance_bucket")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),
            (F.sum("total_amount") / F.sum("trip_distance")).alias("revenue_per_mile"),
            (F.sum("total_amount") / F.sum("duration_minutes")).alias("revenue_per_minute"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            (F.sum("surcharge_components") / F.sum("total_amount")).alias("surcharge_share"),
        )
        #Round to the precision
        .select(
            "pickup_borough",
            "pickup_zone",
            "distance_bucket",
            "trips",
            F.round("total_revenue", 4).alias("total_revenue"),
            F.round("avg_revenue_per_trip", 4).alias("avg_revenue_per_trip"),
            F.round("revenue_per_mile", 4).alias("revenue_per_mile"),
            F.round("revenue_per_minute", 4).alias("revenue_per_minute"),
            F.round("avg_fare_amount", 4).alias("avg_fare_amount"),
            F.round("avg_tip_amount", 4).alias("avg_tip_amount"),
            F.round("surcharge_share", 6).alias("surcharge_share"),
        )
    )


def choose_support_threshold(total_trips: int) -> int:
    #support threshold: 1% of the personalized trip count
    #clamped between 10 and 50 and this prevents tiny zones from dominating
    one_percent = int(total_trips * 0.01)
    return max(10, min(50, one_percent))


def main() -> None:
    args = parse_args()
    #Build the personalized day/hour/top-k parameters from student ID
    personalization = build_personalization(args.student_id)

    app_name = f"q3_df_{args.input_format}_{args.variant}"
    spark = create_spark(app_name)
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        #Read input data (CSV or Parquet)
        if args.input_format == "csv":
            trips_raw = read_2024_csv(spark, args.input_base)
            zones_raw = read_zones_csv(spark, args.input_base)
        else:
            trips_raw = read_2024_parquet(spark, args.input_base)
            zones_raw = read_zones_parquet(spark, args.input_base)

        #columns (hour, day, duration, distance bucket)
        enriched = add_q3_columns(trips_raw)

        #Clean the data with personal
        filtered = filter_q3_window(
            enriched,
            days=personalization.days_2024,
            hours=personalization.hours,
            variant=args.variant,
        ).cache()

        #Count once to compute the support threshold
        filtered_count = filtered.count()
        support_threshold = choose_support_threshold(filtered_count)

        #Join zone lookup and compute group metrics
        joined = join_pickup_zones(filtered, zones_raw)
        grouped = calculate_group_metrics(joined).cache()

        #Apply support threshold
        supported = grouped.filter(F.col("trips") >= support_threshold)

        #Build the three ranked output tables
        #Top K zones by total revenue without support filter
        top_total_revenue = grouped.orderBy(F.desc("total_revenue"), F.desc("trips")).limit(personalization.top_k)
        #Top K zones by revenue per mile with support filter 
        top_revenue_per_mile = supported.orderBy(F.desc("revenue_per_mile"), F.desc("trips")).limit(personalization.top_k)
        #Top K zones by revenue per minute with support filter 
        top_revenue_per_minute = supported.orderBy(F.desc("revenue_per_minute"), F.desc("trips")).limit(personalization.top_k)

        #Build output path label
        label = f"df_{args.input_format}" if args.variant == "normal" else f"df_{args.input_format}_no_pruning"

        table_base = join_hdfs_path(args.output_base, "results", "tables", "q3")
        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", f"q3_{label}_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", f"q3_{label}_plan.txt")

        paths = {
            "top_total_revenue": join_hdfs_path(table_base, f"{label}_top_total_revenue"),
            "top_revenue_per_mile": join_hdfs_path(table_base, f"{label}_top_revenue_per_mile"),
            "top_revenue_per_minute": join_hdfs_path(table_base, f"{label}_top_revenue_per_minute"),
        }

        #Write CSV result tables
        write_small_csv_table(top_total_revenue, paths["top_total_revenue"], args.mode)
        write_small_csv_table(top_revenue_per_mile, paths["top_revenue_per_mile"], args.mode)
        write_small_csv_table(top_revenue_per_minute, paths["top_revenue_per_minute"], args.mode)

        #Save the query execution plan for the per-mile query (useful for inspecting
        # whether partition pruning was actually applied in the normal variant)
        plan_text = top_revenue_per_mile._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        #write metrics JSON
        metrics = {
            "job": "q3_df",
            "input_format": args.input_format,
            "variant": args.variant,
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "days_2024": personalization.days_2024,
                "hours": personalization.hours,
                "top_k": personalization.top_k,
            },
            "filters": {
                "pickup_ts_lte_dropoff_ts": True,
                "duration_minutes_gt_0": True,
                "trip_distance_gt_0": True,
                "fare_amount_gt_0": True,
                "total_amount_gt_0": True,
                "partition_pruning_expected": args.input_format == "parquet" and args.variant == "normal",
            },
            "support_threshold": support_threshold,
            "support_threshold_policy": "max(10, min(50, floor(personal_subset_trips * 0.01)))",
            "filtered_row_count": filtered_count,
            "output_paths": {
                **paths,
                "metrics": metrics_path,
                "plan": plan_path,
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

        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

        #free executor memory
        grouped.unpersist()
        filtered.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()