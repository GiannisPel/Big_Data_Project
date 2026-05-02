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