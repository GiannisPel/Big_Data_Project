#!/usr/bin/env bash
set -euo pipefail

source ~/bigdata-env.sh

PROJECT_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026"

echo "[run_q2_SQL] Running Q2 Spark SQL implementation"

spark-submit \
  --py-files project2026/jobs/common.py \
  project2026/jobs/q2_sql.py \
  --student-id 2121261 \
  --input-base "$PROJECT_BASE" \
  --output-base "$PROJECT_BASE"

echo "[run_q2_SQL] Submitted/completed from client side."