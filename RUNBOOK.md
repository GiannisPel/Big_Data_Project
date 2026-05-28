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

yellow_tripdata_2015
yellow_tripdata_2024
taxi_zone_lookup
prepare_parquet_metrics.json

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

./project2026/scripts/run_eda.sh

### EDA outputs

null_percentages
pickup_day_distribution_2024
pickup_hour_distribution
top_10_pickup_zones_2024
total_amount_log_histogram
trip_distance_log_histogram

## Q1

./project2026/scripts/run_q1_RDD.sh
./project2026/scripts/run_q1_DF_CSV.sh
./project2026/scripts/run_q1_DF_Par.sh
./project2026/scripts/run_q1_SQL_Par.sh

### Q1 outputs

METRICS:

q1_rdd_metrics.json
q1_df_metrics.json
q1_df_parquet_metrics.json
q1_sql_parquet_metrics.json

PLANS:

q1_df_csv_plan.txt
q1_df_parquet_plan.txt
q1_sql_parquet_plan.txt

TABLES:

rdd_csv
rdd_csv_time_band
df_csv
df_csv_time_band
df_parquet
df_parquet_time_band
sql_parquet
sql_parquet_time_band

### Q1 outputs into local repo

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q1_rdd_csv_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q1_df_csv_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q1_df_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q1_sql_parquet_metrics.json project2026/results/metrics/

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q1_rdd_csv_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q1_df_csv_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q1_df_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q1_sql_parquet_plan.txt project2026/results/plans/

hdfs dfs -get /user/$VDCLOUD_USER/project2026/results/tables/q1 project2026/results/tables/q1


## Q2

./project2026/scripts/run_q2_DF_Builtin.sh
./project2026/scripts/run_q2_DF_UDF.sh
./project2026/scripts/run_q2_SQL.sh

### Q2 outputs

SPARK APP ID: spark-f963a23942c34629b49ee7e36e60f8d0

METRICS: 

q2_df_builtin_metrics.json
q2_df_udf_metrics.json
q2_sql_metrics.json

PLANS:

q2_df_builtin_plan.txt
q2_df_udf_plan.txt
q2_sql_plan.txt

TABLES:

df_builtin_by_hour
sql_by_hour
sql_fastest
sql_slowest_per_km

### Q2 outputs into local repo

METRICS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q2_df_builtin_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q2_df_udf_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q2_sql_metrics.json project2026/results/metrics/

PLANS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q2_df_builtin_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q2_df_udf_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q2_sql_plan.txt project2026/results/plans/

TABLES:

hdfs dfs -get /user/$VDCLOUD_USER/project2026/results/tables/q2 project2026/results/tables/q2

## Q3

./project2026/scripts/run_q3_DF_CSV.sh
./project2026/scripts/run_q3_DF_Par.sh
./project2026/scripts/run_q3_SQL_Par.sh
./project2026/scripts/run_q3_DF_NoPruning.sh

### Q3 outputs

Spark DF CSV metrics ID: spark-a7d53e51ee8e4813b567f158f63dbf33
Spark DF Parquate ID: spark-183e73870a1b4005896f12e7b8f615fd
Spark SQL Parquate ID: spark-abdf683bf4c34101935e3099c6d417d2
Spark DF No Pruning ID: spark-84bd661d42e1404e8c1f80b95b4daad1

METRICS:

q3_df_csv_metrics.json
q3_df_parquet_metrics.json
q3_df_parquet_no_pruning_metrics.json
q3_sql_parquet_metrics.json

PLANS:

q3_df_csv_plan.txt
q3_df_parquet_no_pruning_plan.txt
q3_df_parquet_plan.txt
q3_sql_parquet_plan.txt

TABLES:

df_csv_top_revenue_per_mile
df_csv_top_revenue_per_minute
df_csv_top_total_revenue
df_parquet_no_pruning_top_revenue_per_mile
df_parquet_no_pruning_top_revenue_per_minute
df_parquet_no_pruning_top_total_revenue
df_parquet_top_revenue_per_mile
df_parquet_top_revenue_per_minute
df_parquet_top_total_revenue
sql_parquet_top_revenue_per_mile
sql_parquet_top_revenue_per_minute
sql_parquet_top_total_revenue

PLOTS:

q3_top_zones_revenue_per_mile.png
q3_top_zones_revenue_per_minute.png
q3_top_zones_total_revenue.png

### Q3 outputs into local

METRICS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q3_df_csv_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q3_df_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q3_sql_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q3_df_parquet_no_pruning_metrics.json project2026/results/metrics/

PLANS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q3_df_csv_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q3_df_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q3_sql_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q3_df_parquet_no_pruning_plan.txt project2026/results/plans/

TABLES:

hdfs dfs -get /user/$VDCLOUD_USER/project2026/results/tables/q3 project2026/results/tables/q3

Generate PLOTS:

python project2026/scripts/plot_q3.py \
  --tables-dir project2026/results/tables/q3 \
  --plots-dir project2026/results/plots \
  --top-n 10

PLOTS:

project2026/results/plots/q3_top_zones_total_revenue.png
project2026/results/plots/q3_top_zones_revenue_per_mile.png
project2026/results/plots/q3_top_zones_revenue_per_minute.png

## Q4

./project2026/scripts/run_q4_SQL_Par.sh
./project2026/scripts/run_q4_DF_Par.sh
./project2026/scripts/run_q4_SQL_CSV.sh

### Q4 outputs

Spark SQL Parquate ID: spark-cdfe89bd318e48dc8a582a57f908342d
Spark DF Parquate ID: spark-20abfd23d91b4177a570daeb56b87d6a
Spark SQL CSV ID: spark-5d58d6e9690b4d67b3d8fb966073644b

METRICS:

q4_df_parquet_metrics.json
q4_sql_csv_metrics.json
q4_sql_parquet_metrics.json

PLANS:

q4_df_parquet_plan.txt
q4_sql_csv_plan.txt
q4_sql_parquet_plan.txt

TABLES:

df_parquet_by_hour_payment
df_parquet_card_vs_cash
df_parquet_vendor_payment
sql_csv_by_hour_payment
sql_csv_card_vs_cash
sql_csv_vendor_payment
sql_parquet_by_hour_payment
sql_parquet_card_vs_cash
sql_parquet_vendor_payment

PLOTS:
q4_avg_tip_rate_card_by_hour.png
q4_card_share_by_hour.png

### Q4 outputs into local

METRICS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q4_sql_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q4_df_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q4_sql_csv_metrics.json project2026/results/metrics/

PLANS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q4_sql_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q4_df_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q4_sql_csv_plan.txt project2026/results/plans/

TABLES:

hdfs dfs -get /user/$VDCLOUD_USER/project2026/results/tables/q4 project2026/results/tables/q4

PLOTS:

source .venv/bin/activate

python project2026/scripts/plot_q4.py \
  --tables-dir project2026/results/tables/q4 \
  --plots-dir project2026/results/plots