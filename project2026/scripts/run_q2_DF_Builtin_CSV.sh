#!/usr/bin/env bash
set -euo pipefail

source ~/bigdata-env.sh

RAW_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/data"
PROJECT_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026"

echo "[run_q2_DF_Builtin_CSV] Running Q2 DataFrame built-in implementation on raw CSV"

spark-submit \
  --name q2_df_builtin_csv \
  --py-files project2026/jobs/common.py \
  project2026/jobs/q2_df_builtin.py \
  --student-id 2121261 \
  --input-format csv \
  --input-base "$RAW_BASE" \
  --output-base "$PROJECT_BASE"

echo "[run_q2_DF_Builtin_CSV] Submitted/completed from client side."