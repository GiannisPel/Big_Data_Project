from __future__ import annotations

import argparse
import csv
import io
import json
from datetime import datetime
from typing import Any

from common import (
    Timer,
    build_personalization,
    create_spark,
    join_hdfs_path,
    write_text_hdfs,
)


def parse_args() -> argparse.Namespace:
    #RDD version only works on CSV
    parser = argparse.ArgumentParser(description="Q1 Demand analysis using RDD API on CSV.")
    parser.add_argument("--student-id", required=True, type=int)
    parser.add_argument("--input-base", required=True)
    parser.add_argument("--output-base", required=True)
    return parser.parse_args()


def parse_float(value: str) -> float | None:
    #Returns None instead of crashing if the field is empty or useless
    try:
        if value is None or value == "":
            return None
        return float(value)
    except ValueError:
        return None


def parse_int(value: str) -> int | None:
    #cast float first because some int fields come in as "1.0" in the CSV
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except ValueError:
        return None


def parse_2024_timestamp(value: str) -> datetime | None:
    if not value:
        return None

    #the dataset has inconsistent timestamp formats
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None  #skip this row later


def parse_csv_line(line: str) -> list[str] | None:
    try:
        return next(csv.reader(io.StringIO(line)))
    except Exception:
        return None


def time_band_from_hour(hour: int) -> str:
    #same bucketing logic as the DataFrame version, just plain python
    if 6 <= hour <= 11:
        return "Morning"
    if 12 <= hour <= 16:
        return "Afternoon"
    if 17 <= hour <= 21:
        return "Evening"
    return "Late"


def is_weekend_from_datetime(ts: datetime) -> bool:
    #weekday() Monday=0, Sunday=6, ara 5 = Saturday, 6 = Sunday
    return ts.weekday() in (5, 6)


def parse_and_filter_q1(row: list[str], days_2024: set[int], hours: set[int]) -> tuple | None:
    #parsing and filtering to avoid extra RDD scans
    if row is None or len(row) < 19:
        return None  # weird row so skip

    if row[0] == "VendorID":
        return None  #header row so skip

    pickup_ts = parse_2024_timestamp(row[1])
    dropoff_ts = parse_2024_timestamp(row[2])

    #cant use the row if not valid timestamps
    if pickup_ts is None or dropoff_ts is None:
        return None

    if pickup_ts > dropoff_ts:
        return None  #error dropoff before pickup

    pickup_day = pickup_ts.day
    pickup_hour = pickup_ts.hour

    #apply personalization
    if pickup_day not in days_2024:
        return None
    if pickup_hour not in hours:
        return None

    passenger_count = parse_float(row[3])
    trip_distance = parse_float(row[4])
    pu_location_id = parse_int(row[7])
    total_amount = parse_float(row[16])

    #distance and amount required for analysis
    if trip_distance is None or total_amount is None:
        return None

    duration_minutes = (dropoff_ts - pickup_ts).total_seconds() / 60.0

    #filter out zero/negative values
    if duration_minutes <= 0 or trip_distance <= 0 or total_amount <= 0:
        return None

    pickup_date = pickup_ts.date().isoformat()
    weekday = pickup_ts.strftime("%a") 
    is_weekend = is_weekend_from_datetime(pickup_ts)
    time_band = time_band_from_hour(pickup_hour)

    #key is the grouping dimensions to aggregate over later
    key = (pickup_date, pickup_hour, weekday, is_weekend, time_band)

    #value is all the fields to compute aggregates in reduceByKey
    value = {
        "trips": 1,
        "pickup_zones": {pu_location_id} if pu_location_id is not None else set(),
        "passenger_sum": passenger_count or 0.0,
        "passenger_count": 1 if passenger_count is not None else 0,
        "duration_sum": duration_minutes,
        "duration_count": 1,
        "distance_sum": trip_distance,
        "distance_count": 1,
        "total_amount_sum": total_amount,
        "total_amount_count": 1,
        "total_revenue": total_amount,
    }

    return key, value


def merge_values(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    #merges two value dicts together
    return {
        "trips": left["trips"] + right["trips"],
        "pickup_zones": left["pickup_zones"].union(right["pickup_zones"]),
        "passenger_sum": left["passenger_sum"] + right["passenger_sum"],
        "passenger_count": left["passenger_count"] + right["passenger_count"],
        "duration_sum": left["duration_sum"] + right["duration_sum"],
        "duration_count": left["duration_count"] + right["duration_count"],
        "distance_sum": left["distance_sum"] + right["distance_sum"],
        "distance_count": left["distance_count"] + right["distance_count"],
        "total_amount_sum": left["total_amount_sum"] + right["total_amount_sum"],
        "total_amount_count": left["total_amount_count"] + right["total_amount_count"],
        "total_revenue": left["total_revenue"] + right["total_revenue"],
    }


def safe_avg(total: float, count: int) -> float | None:
    #avoids division by zero
    if count == 0:
        return None
    return total / count


def make_result_row(item: tuple, total_trips: int) -> dict[str, Any]:
    key, value = item
    pickup_date, pickup_hour, weekday, is_weekend, time_band = key

    trips = value["trips"]
    total_revenue = value["total_revenue"]

    return {
        "pickup_date": pickup_date,
        "pickup_hour": pickup_hour,
        "weekday": weekday,
        "is_weekend": is_weekend,
        "time_band": time_band,
        "trips": trips,
        "unique_pickup_zones": len(value["pickup_zones"]),  
        "avg_passenger_count": round(safe_avg(value["passenger_sum"], value["passenger_count"]) or 0.0, 4),
        "avg_duration_minutes": round(safe_avg(value["duration_sum"], value["duration_count"]) or 0.0, 4),
        "avg_trip_distance": round(safe_avg(value["distance_sum"], value["distance_count"]) or 0.0, 4),
        "avg_total_amount": round(safe_avg(value["total_amount_sum"], value["total_amount_count"]) or 0.0, 4),
        "total_revenue": round(total_revenue, 4),
        "trips_share_in_personal_window": round(trips / total_trips, 6) if total_trips else 0.0,
    }


def result_rows_to_csv_lines(rows: list[dict[str, Any]]) -> list[str]:
    #manually build CSV lines
    header = [
        "pickup_date",
        "pickup_hour",
        "weekday",
        "is_weekend",
        "time_band",
        "trips",
        "unique_pickup_zones",
        "avg_passenger_count",
        "avg_duration_minutes",
        "avg_trip_distance",
        "avg_total_amount",
        "total_revenue",
        "trips_share_in_personal_window",
    ]

    lines = [",".join(header)]

    for row in rows:
        lines.append(",".join(str(row[col]) for col in header))

    return lines


def band_rows_to_csv_lines(rows: list[dict[str, Any]]) -> list[str]:
    #same as above but for the time band summary table
    header = [
        "time_band",
        "trips",
        "unique_pickup_zones",
        "avg_passenger_count",
        "avg_duration_minutes",
        "avg_trip_distance",
        "avg_total_amount",
        "total_revenue",
        "trips_share_in_personal_window",
    ]

    lines = [",".join(header)]

    for row in rows:
        lines.append(",".join(str(row[col]) for col in header))

    return lines


def delete_hdfs_path_if_exists(spark, path: str) -> None:
    #same HDFS deletion helper as DataFrame v
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    hdfs_path = jvm.org.apache.hadoop.fs.Path(path)

    if fs.exists(hdfs_path):
        fs.delete(hdfs_path, True)


def save_lines_to_hdfs(spark, lines: list[str], path: str) -> None:
    delete_hdfs_path_if_exists(spark, path)
    spark.sparkContext.parallelize(lines, 1).saveAsTextFile(path)


def main() -> None:
    args = parse_args()
    personalization = build_personalization(args.student_id)

    spark = create_spark("q1_rdd_csv")
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    with Timer() as timer:
        input_path = join_hdfs_path(args.input_base, "yellow_tripdata_2024.csv")
        days_set = set(personalization.days_2024)
        hours_set = set(personalization.hours)
        raw_lines = sc.textFile(input_path)

        #parse each line then apply filters then cache for reuse
        filtered_pairs = (
            raw_lines
            .map(parse_csv_line)     #str -> list[str], parse
            .map(lambda row: parse_and_filter_q1(row, days_set, hours_set))  #list[str] -> (key, value), filter
            .filter(lambda item: item is not None)  #drop None (bad rows), cache
            .cache()    #cache                          
        )

        filtered_count = filtered_pairs.count()

        #aggregate per date, hour group
        reduced_by_hour = filtered_pairs.reduceByKey(merge_values)

        #build result dicts
        all_rows = (
            reduced_by_hour
            .map(lambda item: make_result_row(item, filtered_count))
            .collect()
        )

        #sort
        top_rows = sorted(
            all_rows,
            key=lambda r: (
                -r["trips"],          #most trips first
                -r["total_revenue"],  #highest revenue second
                r["pickup_date"],     #chronological third
                r["pickup_hour"],
            ),
        )[:personalization.top_k]

        band_pairs = filtered_pairs.map(
            lambda item: (
                item[0][4],  #time_band is the 5th element of the tuple
                item[1],
            )
        )

        band_reduced = band_pairs.reduceByKey(merge_values)

        #build band summary rows
        band_rows = []
        for band, value in band_reduced.collect():
            trips = value["trips"]
            band_rows.append({
                "time_band": band,
                "trips": trips,
                "unique_pickup_zones": len(value["pickup_zones"]),
                "avg_passenger_count": round(safe_avg(value["passenger_sum"], value["passenger_count"]) or 0.0, 4),
                "avg_duration_minutes": round(safe_avg(value["duration_sum"], value["duration_count"]) or 0.0, 4),
                "avg_trip_distance": round(safe_avg(value["distance_sum"], value["distance_count"]) or 0.0, 4),
                "avg_total_amount": round(safe_avg(value["total_amount_sum"], value["total_amount_count"]) or 0.0, 4),
                "total_revenue": round(value["total_revenue"], 4),
                "trips_share_in_personal_window": round(trips / filtered_count, 6) if filtered_count else 0.0,
            })

        band_rows = sorted(band_rows, key=lambda r: -r["trips"]) 
        result_dir = join_hdfs_path(args.output_base, "results", "tables", "q1", "rdd_csv")
        time_band_dir = join_hdfs_path(args.output_base, "results", "tables", "q1", "rdd_csv_time_band")
        metrics_path = join_hdfs_path(args.output_base, "results", "metrics", "q1_rdd_csv_metrics.json")
        plan_path = join_hdfs_path(args.output_base, "results", "plans", "q1_rdd_csv_plan.txt")
        save_lines_to_hdfs(spark, result_rows_to_csv_lines(top_rows), result_dir)
        save_lines_to_hdfs(spark, band_rows_to_csv_lines(band_rows), time_band_dir)

        #RDD jobs dont have a catalyst query plan
        write_text_hdfs(
            spark,
            plan_path,
            "RDD implementation: no Catalyst logical/physical plan is available. "
            "The job manually parses CSV lines and uses map/filter/reduceByKey.",
            overwrite=True,
        )

        metrics = {
            "job": "q1_rdd",
            "input_format": "csv",
            "student_id": args.student_id,
            "spark_application_id": spark.sparkContext.applicationId,
            "personalization": {
                "hours": personalization.hours,
                "days_2024": personalization.days_2024,
                "top_k": personalization.top_k,
            },
            "filtered_row_count": filtered_count,
            "output_paths": {
                "top_hours": result_dir,
                "time_band_summary": time_band_dir,
                "metrics": metrics_path,
                "plan": plan_path,
            },
            "top_hours_preview": top_rows,
            "time_band_summary_preview": band_rows,
            "elapsed_seconds": round(timer.elapsed_seconds, 3),
        }

        write_text_hdfs(
            spark,
            metrics_path,
            json.dumps(metrics, indent=2, ensure_ascii=False),
            overwrite=True,
        )

        print(json.dumps(metrics, indent=2, ensure_ascii=False), flush=True)

        #release the cached RDD
        filtered_pairs.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()