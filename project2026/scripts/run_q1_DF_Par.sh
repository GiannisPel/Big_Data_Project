#!/usr/bin/env bash
set -euo pipefail

source ~/bigdata-env.sh

RAW_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
PROJECT_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026"

spark-submit \
  --py-files project2026/jobs/common.py \
  project2026/jobs/q1_df.py \
  --student-id 2121261 \
  --input-format parquet \
  --input-base "$PROJECT_BASE" \
  --output-base "$PROJECT_BASE"