from __future__ import annotations

# Q5 - Airport and Borough Flow Analysis (DataFrame API)
# Using the PySpark DataFrame API to analyze flows between boroughs
# and airport-related trips. Data sources: yellow_tripdata_2024 + taxi_zone_lookup.

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
    parser = argparse.ArgumentParser(description="Q5 airport and borough flow analysis using DataFrame API.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    # The no_broadcast variant is needed to observe the effect on join strategy
    parser.add_argument("--variant", default="normal", choices=["normal", "no_broadcast"])
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "errorifexists"])
    return parser.parse_args()


def make_json_safe(value: Any) -> Any:
    # Helper function to serialize the result to JSON.
    # Spark returns Decimal and datetime objects that are not JSON-serializable by default.
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
    # coalesce(1) because the results are small (top-K rows) so multiple partitions are unnecessary
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


def prepare_trips(df: DataFrame, days: list[int], hours: list[int]) -> DataFrame:
    """
    Filter trips based on the personalized window (days + hours of 2024).
    Keep only records with:
      - valid timestamps (pickup <= dropoff)
      - duration > 0, distance > 0, total_amount > 0
    This removes cancelled/erroneous trips that would skew the aggregates.
    """
    return (
        df
        .withColumn(
            "duration_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / F.lit(60.0),
        )
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("pickup_ts") <= F.col("dropoff_ts"))
        .filter(F.col("duration_minutes") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("pickup_day").isin(days))
        .filter(F.col("pickup_hour").isin(hours))
    )


def airport_zone_expr(zone_col: str, borough_col: str):
    """
    Airport detection logic based on zone/borough name.

    Accepted airport zones:
      - Zones containing: 'jfk', 'laguardia', 'newark', 'airport' (case-insensitive)
      - Borough = 'ewr' (Newark Airport has its own borough in the NYC taxi dataset)

    Note: airport_fee > 0 is used ONLY as an additional signal for pickup
    (see join_pickup_dropoff_zones). It is not included here because this function
    is also used for dropoff, where the fee is not meaningful as an airport indicator.
    """
    zone_lower = F.lower(F.coalesce(F.col(zone_col), F.lit("")))
    borough_lower = F.lower(F.coalesce(F.col(borough_col), F.lit("")))

    return (
        zone_lower.rlike("jfk|laguardia|newark|airport")
        | (borough_lower == F.lit("ewr"))
    )


def join_pickup_dropoff_zones(trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Double join with taxi_zone_lookup:
      1. one for pickup location (pu_location_id)
      2. one for dropoff location (do_location_id)

    Using aliases to avoid column name conflicts.
    LEFT JOIN to retain trips even when zone info is missing (Unknown).

    For pu_is_airport, airport_fee > 0 is used as an additional economic indicator.
    For do_is_airport it is NOT used because the fee is charged at the trip origin.
    """
    pickup_zones = zones_df.select(
        F.col("location_id").alias("pu_zone_location_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone").alias("pu_zone"),
        F.col("service_zone").alias("pu_service_zone"),
    )

    dropoff_zones = zones_df.select(
        F.col("location_id").alias("do_zone_location_id"),
        F.col("borough").alias("do_borough"),
        F.col("zone").alias("do_zone"),
        F.col("service_zone").alias("do_service_zone"),
    )

    joined = (
        trips_df
        .join(pickup_zones, trips_df["pu_location_id"] == pickup_zones["pu_zone_location_id"], "left")
        .join(dropoff_zones, trips_df["do_location_id"] == dropoff_zones["do_zone_location_id"], "left")
        .drop("pu_zone_location_id", "do_zone_location_id")
        .fillna(
            {
                "pu_borough": "Unknown",
                "pu_zone": "Unknown",
                "pu_service_zone": "Unknown",
                "do_borough": "Unknown",
                "do_zone": "Unknown",
                "do_service_zone": "Unknown",
            }
        )
    )

    return (
        joined
        .withColumn(
            "pu_is_airport",
            (
                airport_zone_expr("pu_zone", "pu_borough")
                | (F.coalesce(F.col("airport_fee"), F.lit(0.0)) > 0)  # economic signal for pickup
            ).cast("int"),
        )
        .withColumn(
            "do_is_airport",
            airport_zone_expr("do_zone", "do_borough").cast("int"),
        )
        .withColumn(
            # A trip is an "airport trip" if either pickup or dropoff is at an airport
            "airport_trip",
            ((F.col("pu_is_airport") == 1) | (F.col("do_is_airport") == 1)).cast("int"),
        )
    )


def calculate_borough_flows(df: DataFrame, filtered_count: int, top_k: int) -> DataFrame:
    """
    Main borough-to-borough flow table.
    trip_share is computed as trips / filtered_count (fraction of the total window).
    airport_trip_share shows what percentage of trips for each flow involves an airport.
    Ordering: trips DESC first, then total_revenue DESC as a tiebreaker.
    """
    grouped = (
        df
        .groupBy("pu_borough", "do_borough")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("airport_trip").alias("airport_trip_share"),
        )
    )

    return (
        grouped
        .withColumn("trip_share", F.round(F.col("trips") / F.lit(filtered_count), 6))
        .select(
            "pu_borough",
            "do_borough",
            "trips",
            "trip_share",
            F.round("total_revenue", 4).alias("total_revenue"),
            F.round("avg_total_amount", 4).alias("avg_total_amount"),
            F.round("avg_trip_distance", 4).alias("avg_trip_distance"),
            F.round("avg_duration_minutes", 4).alias("avg_duration_minutes"),
            F.round("airport_trip_share", 6).alias("airport_trip_share"),
        )
        .orderBy(F.desc("trips"), F.desc("total_revenue"))
        .limit(top_k)
    )


def calculate_airport_routes(df: DataFrame, top_k: int) -> DataFrame:
    """
    Separate table for airport-related trips (pu_zone -> do_zone).
    Filtered to airport_trip == 1 and aggregated at zone level (more granular than borough).
    avg_airport_fee is included for comparison against non-airport trips.
    """
    return (
        df
        .filter(F.col("airport_trip") == 1)
        .groupBy("pu_zone", "do_zone")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.avg(F.coalesce(F.col("airport_fee"), F.lit(0.0))).alias("avg_airport_fee"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            F.avg("trip_distance").alias("avg_trip_distance"),
        )
        .select(
            "pu_zone",
            "do_zone",
            "trips",
            F.round("avg_airport_fee", 4).alias("avg_airport_fee"),
            F.round("avg_total_amount", 4).alias("avg_total_amount"),
            F.round("avg_duration_minutes", 4).alias("avg_duration_minutes"),
            F.round("avg_trip_distance", 4).alias("avg_trip_distance"),
        )
        .orderBy(F.desc("trips"), F.desc("avg_total_amount"))
        .limit(top_k)
    )


def calculate_airport_zone_examples(zones_df: DataFrame) -> DataFrame:
    """
    Small documentation table: shows which zones were detected as airports.
    Useful for verifying that the detection logic is working correctly.

    Expected zones include:
      - JFK Airport (Queens)
      - LaGuardia Airport (Queens)
      - Newark Airport (EWR borough)
      - Any zone with "Airport" in the name
    """
    return (
        zones_df
        .filter(airport_zone_expr("zone", "borough"))
        .select("location_id", "borough", "zone", "service_zone")
        .orderBy("borough", "zone")
        .limit(10)
    )


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark(f"q5_df_parquet_{args.variant}")
    spark.sparkContext.setLogLevel("WARN")

    if args.variant == "no_broadcast":
        # Disable broadcast join to see what strategy Catalyst picks
        # when it cannot use BroadcastHashJoin for taxi_zone_lookup.
        # Expecting sort-merge join, which is slower for small lookup tables.
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    with Timer() as timer:
        trips_raw = read_2024_parquet(spark, args.input_base)
        zones_raw = read_zones_parquet(spark, args.input_base)

        # Filter first and cache because it is used twice: count + enrichment
        filtered_trips = prepare_trips(
            trips_raw,
            days=personalization.days_2024,
            hours=personalization.hours,
        ).cache()

        filtered_count = filtered_trips.count()

        # Cache enriched too because it feeds both borough_flows AND airport_routes
        enriched = join_pickup_dropoff_zones(filtered_trips, zones_raw).cache()

        borough_flows = calculate_borough_flows(
            enriched,
            filtered_count=filtered_count,
            top_k=personalization.top_k,
        )
        airport_routes = calculate_airport_routes(
            enriched,
            top_k=personalization.top_k,
        )
        airport_examples = calculate_airport_zone_examples(zones_raw)

        label = "df_parquet" if args.variant == "normal" else "df_parquet_no_broadcast"
        table_base = join_hdfs_path(args.output_base, "results", "tables", "q5")

        paths = {
            "borough_flows": join_hdfs_path(table_base, f"{label}_borough_flows"),
            "airport_routes": join_hdfs_path(table_base, f"{label}_airport_routes"),
            "airport_zone_examples": join_hdfs_path(table_base, f"{label}_airport_zone_examples"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", f"q5_{label}_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", f"q5_{label}_plan.txt")

        write_small_csv_table(borough_flows, paths["borough_flows"], args.mode)
        write_small_csv_table(airport_routes, paths["airport_routes"], args.mode)
        write_small_csv_table(airport_examples, paths["airport_zone_examples"], args.mode)

        # Save the physical plan to analyze the join strategy in the report
        plan_text = borough_flows._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q5_df",
            "input_format": "parquet",
            "variant": args.variant,
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
                "total_amount_gt_0": True,
            },
            "airport_detection_policy": {
                "zone_name_terms": ["JFK", "LaGuardia", "Newark", "Airport"],
                "borough_ewr": "EWR",
                # airport_fee > 0 as an extra signal ONLY for pickup (not dropoff)
                "airport_fee_gt_0_used_as_pickup_airport_signal": True,
            },
            "join_strategy_variant": args.variant,
            "filtered_row_count": filtered_count,
            "output_paths": {
                **paths,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "borough_flows_preview": rows_to_dicts(borough_flows.collect()),
            "airport_routes_preview": rows_to_dicts(airport_routes.collect()),
            "airport_zone_examples_preview": rows_to_dicts(airport_examples.collect()),
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False),
            overwrite=True,
        )

        print(json.dumps(make_json_safe(metrics), indent=2, ensure_ascii=False), flush=True)

        # Unpersist to free memory
        enriched.unpersist()
        filtered_trips.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()