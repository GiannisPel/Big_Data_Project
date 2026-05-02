from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import time
from typing import Any

from pyspark.sql import SparkSession


HDFS_NAMENODE = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"


@dataclass(frozen=True)
class Personalization:
    student_id: int
    start_hour: int
    window_length: int
    hours: list[int]
    start_day_2024: int
    days_2024: list[int]
    top_k: int


def build_personalization(student_id: int) -> Personalization:
    start_hour = student_id % 24
    window_length = 4
    start_day_2024 = (student_id % 26) + 1
    top_k = (student_id % 11) + 10

    hours = [(start_hour + offset) % 24 for offset in range(window_length)]
    days_2024 = [start_day_2024, start_day_2024 + 1, start_day_2024 + 2]

    return Personalization(
        student_id=student_id,
        start_hour=start_hour,
        window_length=window_length,
        hours=hours,
        start_day_2024=start_day_2024,
        days_2024=days_2024,
        top_k=top_k,
    )


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def hdfs_path(*parts: str, namenode: str = HDFS_NAMENODE) -> str:
    cleaned = [part.strip("/") for part in parts if part]
    return f"{namenode}/{'/'.join(cleaned)}"


def write_json_local(path: str, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )


class Timer:
    def __enter__(self) -> "Timer":
        self.start = time.perf_counter()
        self.end = None
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.end = time.perf_counter()

    @property
    def elapsed_seconds(self) -> float:
        end = self.end if self.end is not None else time.perf_counter()
        return end - self.start

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)


def schema_2015_raw() -> StructType:
    return StructType([
        StructField("VendorID", StringType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", StringType(), True),
        StructField("trip_distance", StringType(), True),
        StructField("pickup_longitude", StringType(), True),
        StructField("pickup_latitude", StringType(), True),
        StructField("RateCodeID", StringType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", StringType(), True),
        StructField("dropoff_latitude", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("fare_amount", StringType(), True),
        StructField("extra", StringType(), True),
        StructField("mta_tax", StringType(), True),
        StructField("tip_amount", StringType(), True),
        StructField("tolls_amount", StringType(), True),
        StructField("improvement_surcharge", StringType(), True),
        StructField("total_amount", StringType(), True),
    ])


def schema_2024_raw() -> StructType:
    return StructType([
        StructField("VendorID", StringType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", StringType(), True),
        StructField("trip_distance", StringType(), True),
        StructField("RatecodeID", StringType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", StringType(), True),
        StructField("DOLocationID", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("fare_amount", StringType(), True),
        StructField("extra", StringType(), True),
        StructField("mta_tax", StringType(), True),
        StructField("tip_amount", StringType(), True),
        StructField("tolls_amount", StringType(), True),
        StructField("improvement_surcharge", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("congestion_surcharge", StringType(), True),
        StructField("Airport_fee", StringType(), True),
    ])


def schema_zones_raw() -> StructType:
    return StructType([
        StructField("LocationID", StringType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ])


def join_hdfs_path(base: str, *parts: str) -> str:
    cleaned_base = base.rstrip("/")
    cleaned_parts = [part.strip("/") for part in parts if part]
    return f"{cleaned_base}/{'/'.join(cleaned_parts)}"


def write_text_hdfs(spark: SparkSession, path: str, text: str, overwrite: bool = True) -> None:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    hdfs_path_obj = jvm.org.apache.hadoop.fs.Path(path)

    if overwrite and fs.exists(hdfs_path_obj):
        fs.delete(hdfs_path_obj, False)

    stream = fs.create(hdfs_path_obj, True)
    stream.write(bytearray(text, "utf-8"))
    stream.close()


def hdfs_size_bytes(spark: SparkSession, path: str) -> int:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    p = jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(p):
        return 0

    return int(fs.getContentSummary(p).getLength())


def hdfs_file_count(spark: SparkSession, path: str) -> int:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    p = jvm.org.apache.hadoop.fs.Path(path)

    if not fs.exists(p):
        return 0

    iterator = fs.listFiles(p, True)
    count = 0
    while iterator.hasNext():
        status = iterator.next()
        name = status.getPath().getName()
        if not name.startswith("_") and not name.startswith("."):
            count += 1

    return count