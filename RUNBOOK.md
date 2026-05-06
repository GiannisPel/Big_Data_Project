# RUNBOOK

## Test to check if environment works

App ID: spark-4bc269b040a54a9187388212666be3cc
Pod example: test-wordcount-py-786c2c9ddf7882d8-driver
Input: hdfs:///user/ipelekanos/project2026/tmp/test_text.txt
Output: hdfs:///user/ipelekanos/project2026/output/test_wordcount

Result:
- ('spark', 4)
- ('data', 2)
- ('big', 1)
- ('python', 1)

## Before working on anything

- Connect VPN
- Open WSL
- Load environment:


cd ~/project2026-bigdata
source ~/bigdata-env.sh

## CSV to Parquet

Εδω διαβασα τα 3 datasets, δημιουργησα βοηθητικες στηλες/διαμορφωσα τα ονοματα των στηλων οπως τα ζητησατε,
αναλυση χρονικων και αριθμητικων πεδιων, αναγραφη εξοδων parquet και αποηθηκευση μετρησεων προετοιμασιας.

### Prepare Parquate Commands

spark-submit \
  --py-files project2026/jobs/common.py \
  project2026/jobs/prepare_parquet.py \
  --student-id 2121261 \
  --input-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/data \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026 

## HDFS input

/data/yellow_tripdata_2015.csv
/data/yellow_tripdata_2024.csv
/data/taxi_zone_lookup.csv

## HDFS output 

/user/ipelekanos/project2026/data/parquet/yellow_tripdata_2015
/user/ipelekanos/project2026/data/parquet/yellow_tripdata_2024
/user/ipelekanos/project2026/data/parquet/taxi_zone_lookup
/user/ipelekanos/project2026/results/metrics/prepare_parquet_metrics.json

## Partitions

- yellow_tripdata_2015 χωριστηκε απο pickup_hour
- yellow_tripdata_2024 χωριστηκε απο pickup_day
- taxi_zone_lookup δεν χωριστηκε καπως

### Commands

hdfs dfs -ls /user/$VDCLOUD_USER/project2026/data/parquet
hdfs dfs -ls /user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2015 | head
hdfs dfs -ls /user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 | head
hdfs dfs -ls /user/$VDCLOUD_USER/project2026/data/parquet/taxi_zone_lookup
hdfs dfs -cat /user/$VDCLOUD_USER/project2026/results/metrics/prepare_parquet_metrics.json


## EDA Commands

spark-submit \
  --py-files project2026/jobs/common.py \
  project2026/jobs/eda.py \
  --student-id 2121261 \
  --input-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026 \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026


## EDA

```bash
./project2026/scripts/run_eda.sh

### EDA outputs

/user/ipelekanos/project2026/results/tables/eda/null_percentages
/user/ipelekanos/project2026/results/tables/eda/pickup_day_distribution_2024
/user/ipelekanos/project2026/results/tables/eda/pickup_hour_distribution
/user/ipelekanos/project2026/results/tables/eda/top_10_pickup_zones_2024
/user/ipelekanos/project2026/results/tables/eda/total_amount_log_histogram
/user/ipelekanos/project2026/results/tables/eda/trip_distance_log_histogram

### EDA Spark

Spark app ID: spark-7055d17eb1074174a68bf1a8e581be7a
Metrics path: /user/ipelekanos/project2026/results/metrics/eda_metrics.json

