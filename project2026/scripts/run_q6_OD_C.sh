#!/usr/bin/env bash
set -euo pipefail

source ~/bigdata-env.sh

RUN_LABEL="${1:-cold}"
PROJECT_BASE="hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026"

spark-submit \
  --name "q6_od_halves_configC_${RUN_LABEL}" \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.executor.instances=8 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g \
  --py-files project2026/jobs/common.py \
  project2026/jobs/q6_od_halves.py \
  --student-id 2121261 \
  --config C \
  --run "$RUN_LABEL" \
  --min-trips 30 \
  --input-base "$PROJECT_BASE" \
  --output-base "$PROJECT_BASE"