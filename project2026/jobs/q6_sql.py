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
    parser = argparse.ArgumentParser(description="Q6 pickup/dropoff imbalance analysis using Spark SQL.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    parser.add_argument("--activity-threshold", type=int, default=30)
    parser.add_argument("--top-k", type=int, default=None)
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


def write_small_csv_table(df, output_path: str, mode: str) -> None:
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

    top_k     = args.top_k if args.top_k is not None else personalization.top_k
    threshold = args.activity_threshold

    spark = create_spark("q6_sql_parquet")
    spark.sparkContext.setLogLevel("WARN")

    with Timer() as timer:
        trips_path = join_hdfs_path(args.input_base, "data", "parquet", "yellow_tripdata_2024")
        zones_path = join_hdfs_path(args.input_base, "data", "parquet", "taxi_zone_lookup")

        spark.read.parquet(trips_path).createOrReplaceTempView("yellow_2024")
        spark.read.parquet(zones_path).createOrReplaceTempView("taxi_zones")

        days_sql  = ", ".join(str(d) for d in personalization.days_2024)
        hours_sql = ", ".join(str(h) for h in personalization.hours)

        #Build the q6_imbalance view with CTEs:
        #filtered -> apply quality + personal filters
        #pickups -> count per pickup_hour, pu_location_id
        #dropoffs-> count per pickup_hour, do_location_id
        #combined     -> FULL OUTER JOIN, fill nulls with 0
        #with_metrics -> compute activity, net_pickups, imbalance_ratio
        #final SELECT -> join taxi_zones, apply activity threshold
        prepare_query = f"""
            CREATE OR REPLACE TEMP VIEW q6_imbalance AS
            WITH filtered AS (
                SELECT *
                FROM yellow_2024
                WHERE pickup_ts IS NOT NULL
                  AND duration_minutes > 0
                  AND total_amount > 0
                  AND pickup_day  IN ({days_sql})
                  AND pickup_hour IN ({hours_sql})
            ),
            pickups AS (
                SELECT
                    pickup_hour      AS hour,
                    pu_location_id   AS location_id,
                    COUNT(*)         AS pickups
                FROM filtered
                GROUP BY pickup_hour, pu_location_id
            ),
            dropoffs AS (
                -- using pickup_hour (not dropoff hour) as shared time base
                SELECT
                    pickup_hour      AS hour,
                    do_location_id   AS location_id,
                    COUNT(*)         AS dropoffs
                FROM filtered
                GROUP BY pickup_hour, do_location_id
            ),
            combined AS (
                -- FULL OUTER JOIN so zones that only appear on one side are kept
                SELECT
                    coalesce(p.hour,        d.hour)        AS hour,
                    coalesce(p.location_id, d.location_id) AS location_id,
                    coalesce(p.pickups,  0)                AS pickups,
                    coalesce(d.dropoffs, 0)                AS dropoffs
                FROM pickups p
                FULL OUTER JOIN dropoffs d
                  ON p.hour = d.hour AND p.location_id = d.location_id
            ),
            with_metrics AS (
                SELECT
                    hour,
                    location_id,
                    pickups,
                    dropoffs,
                    (pickups + dropoffs) AS activity,
                    (pickups - dropoffs) AS net_pickups,
                    -- imbalance_ratio is undefined when activity = 0
                    CASE WHEN (pickups + dropoffs) > 0
                         THEN ROUND(
                             (pickups - dropoffs) / CAST((pickups + dropoffs) AS DOUBLE), 6)
                         ELSE NULL
                    END AS imbalance_ratio
                FROM combined
            )
            SELECT
                m.hour,
                m.location_id,
                coalesce(z.borough,      'Unknown') AS borough,
                coalesce(z.zone,         'Unknown') AS zone,
                coalesce(z.service_zone, 'Unknown') AS service_zone,
                m.pickups,
                m.dropoffs,
                m.activity,
                m.net_pickups,
                m.imbalance_ratio,
                ABS(m.imbalance_ratio) AS abs_imbalance_ratio
            FROM with_metrics m
            LEFT JOIN taxi_zones z ON m.location_id = z.location_id
            -- drop low-traffic zones to keep results meaningful
            WHERE m.activity >= {threshold}
        """

        spark.sql(prepare_query)

        #Top K zones with the most pickups
        top_positive_query = f"""
            SELECT *
            FROM q6_imbalance
            WHERE net_pickups > 0
            ORDER BY net_pickups DESC
            LIMIT {top_k}
        """

        #Top K zones with the most dropoffs
        top_negative_query = f"""
            SELECT *
            FROM q6_imbalance
            WHERE net_pickups < 0
            ORDER BY net_pickups ASC
            LIMIT {top_k}
        """

        #Top K zones by how one sided they are, no matter of direction
        top_abs_query = f"""
            SELECT *
            FROM q6_imbalance
            WHERE abs_imbalance_ratio IS NOT NULL
            ORDER BY abs_imbalance_ratio DESC
            LIMIT {top_k}
        """

        #Per hour rollup
        hourly_summary_query = """
            SELECT
                hour,
                SUM(ABS(net_pickups))              AS total_abs_net_pickups,
                ROUND(AVG(abs_imbalance_ratio), 6) AS mean_abs_imbalance_ratio
            FROM q6_imbalance
            GROUP BY hour
            ORDER BY mean_abs_imbalance_ratio DESC
        """

        top_positive   = spark.sql(top_positive_query)
        top_negative   = spark.sql(top_negative_query)
        top_abs        = spark.sql(top_abs_query)
        hourly_summary = spark.sql(hourly_summary_query)

        table_base = join_hdfs_path(args.output_base, "results", "tables", "q6")

        paths = {
            "top_positive_net_pickups": join_hdfs_path(table_base, "sql_parquet_top_positive"),
            "top_negative_net_pickups": join_hdfs_path(table_base, "sql_parquet_top_negative"),
            "top_abs_imbalance":        join_hdfs_path(table_base, "sql_parquet_top_abs"),
            "hourly_summary":           join_hdfs_path(table_base, "sql_parquet_hourly_summary"),
        }

        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q6_sql_parquet_metrics.json")
        plan_path    = join_hdfs_path(args.output_base, "results", "plans",   "q6_sql_parquet_plan.txt")

        write_small_csv_table(top_positive,   paths["top_positive_net_pickups"], args.mode)
        write_small_csv_table(top_negative,   paths["top_negative_net_pickups"], args.mode)
        write_small_csv_table(top_abs,        paths["top_abs_imbalance"],        args.mode)
        write_small_csv_table(hourly_summary, paths["hourly_summary"],           args.mode)

        #Save  plan
        plan_text = top_abs._jdf.queryExecution().toString()
        write_text_hdfs(spark, plan_path, plan_text, overwrite=True)

        metrics = {
            "job": "q6_sql",
            "input_format": "parquet",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "days_2024": personalization.days_2024,
                "hours": personalization.hours,
                "top_k": top_k,
            },
            "activity_threshold": threshold,
            "imbalance_policy": {
                "time_base": "pickup_hour",
                "join_type": "FULL OUTER",
                "null_fill": 0,
                "imbalance_ratio_formula": "(pickups - dropoffs) / (pickups + dropoffs)",
            },
            "sql": {
                "prepare_query":        prepare_query,
                "top_positive_query":   top_positive_query,
                "top_negative_query":   top_negative_query,
                "top_abs_query":        top_abs_query,
                "hourly_summary_query": hourly_summary_query,
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

    spark.stop()


if __name__ == "__main__":
    main()