#!/usr/bin/env bash
set -euo pipefail

source ~/bigdata-env.sh

spark-submit \
  --py-files project2026/jobs/common.py \
  project2026/jobs/eda.py \
  --student-id 2121261 \
  --input-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026 \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026