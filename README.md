# Project 2026 Big Data

## Environment

- WSL2 Ubuntu
- Java 11
- Spark client 3.5.8
- Hadoop client 3.4.1
- kubectl
- OpenVPN connection to vdcloud
- Kubernetes namespace: `ipelekanos-priv`

## Info

ID: `2121261`

parameters:
- `h = 21`
- `L = 4`
- `hours = [21, 22, 23, 0]`
- `d = 26`
- `days_2024 = [26, 27, 28]`
- `K = 20`

## Layout


project2026/
  - jobs/
  - lib/
  - docs/
  - outputs/
    
README.md
RUNBOOK.md

## Current status

- Remote Spark/Kubernetes/HDFS test completed.
- CSV to Parquet preparation completed successfully.
- Prepared Parquet datasets are stored in HDFS under:

/user/ipelekanos/project2026/data/parquet/

AND preparation metrics are at:

/user/ipelekanos/project2026/results/metrics/prepare_parquet_metrics.json
