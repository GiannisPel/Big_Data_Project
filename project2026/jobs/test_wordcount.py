from __future__ import annotations

import argparse
from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ipelekanos-smoke-wordcount").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    counts = (
        sc.textFile(args.input)
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda left, right: left + right)
        .sortBy(lambda item: (-item[1], item[0]))
    )

    counts.coalesce(1).saveAsTextFile(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
