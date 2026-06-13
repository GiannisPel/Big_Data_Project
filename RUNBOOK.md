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

## FOR EACH JOB YOU CAN SEE SPARK UI

kubectl -n "$VDCLOUD_USER-priv" get pods -o wide

kubectl -n "$VDCLOUD_USER-priv" port-forward pod/"driver pod name" 4040:4040

Then open tab in browser on the 4040 ports

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

Spark EDA ID: spark-7055d17eb1074174a68bf1a8e581be7a
Elapsed seconds: 4011.758 seconds

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

Spark DF ID: spark-e55e23dc54854c969da2f5e5ba3a98a7
Elapsed seconds: 628.513 seconds

Spark DF Parqaute ID: spark-5c571a7967b8471588d7e7e0f63de27b
Elapsed seconds: 447.276 seconds

Spark RDD ID: spark-0cc020bd647741729e8f0467659d5cba
Elapsed seconds: 1410.911 seconds

SPARK SQL ID: spark-05e1d5eff3e443b88a2e790589b39224
Elapsed seconds: 122.192 seconds

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
./project2026/scripts/run_q2_DF_Builtin_CSV.sh
./project2026/scripts/run_q2_DF_UDF.sh
./project2026/scripts/run_q2_SQL.sh

### Q2 outputs

Spark DF built in ID: spark-f963a23942c34629b49ee7e36e60f8d0
Elapsed seconds: 190.714 seconds

Spark DF CSV built in ID: spark-dcb33cd1ca8c4ceca7f80a7663dc69d2

Spark DF UDF ID: spark-b552a3669292466782e2f242bd82b39c
Elapsed seconds: 140.939 seconds

Spark SQL ID: spark-f68dcb130ce54db5add75c78e7087a14
Elapsed seconds: 589.772 seconds

METRICS: 

q2_df_builtin_metrics.json
q2_df_builtin_csv_metrics.json
q2_df_udf_metrics.json
q2_sql_metrics.json

PLANS:

q2_df_builtin_plan.txt
q2_df_builtin_csv_plan.txt
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
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q2_df_builtin_csv_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q2_df_udf_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q2_sql_metrics.json project2026/results/metrics/

PLANS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q2_df_builtin_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q2_df_builtin_csv_plan.txt project2026/results/plans/
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
Elapsed seconds: 568.984 seconds

Spark DF Parquate ID: spark-183e73870a1b4005896f12e7b8f615fd
Elapsed seconds: 316.275 seconds

Spark SQL Parquate ID: spark-abdf683bf4c34101935e3099c6d417d2
Elapsed seconds: 107.972 seconds

Spark DF No Pruning ID: spark-84bd661d42e1404e8c1f80b95b4daad1
Elapsed seconds: 229.864 seconds

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

Generate PLOTS:

python project2026/scripts/plot_q3.py \
  --tables-dir project2026/results/tables/q3 \
  --plots-dir project2026/results/plots \
  --top-n 10

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

PLOTS:

project2026/results/plots/q3_top_zones_total_revenue.png
project2026/results/plots/q3_top_zones_revenue_per_mile.png
project2026/results/plots/q3_top_zones_revenue_per_minute.png

## Q4

./project2026/scripts/run_q4_SQL_Par.sh
./project2026/scripts/run_q4_DF_Par.sh
./project2026/scripts/run_q4_SQL_CSV.sh

Generate PLOTS:

source .venv/bin/activate

python project2026/scripts/plot_q4.py \
  --tables-dir project2026/results/tables/q4 \
  --plots-dir project2026/results/plots

### Q4 outputs

Spark SQL Parquate ID: spark-cdfe89bd318e48dc8a582a57f908342d
Elapsed seconds: 1000.471 seconds

Spark DF Parquate ID: spark-20abfd23d91b4177a570daeb56b87d6a
Elapsed seconds: 160.937 seconds

Spark SQL CSV ID: spark-5d58d6e9690b4d67b3d8fb966073644b
Elapsed seconds: 3313.016 seconds

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

## Q5 

./project2026/scripts/run_q5_DF_Par.sh
./project2026/scripts/run_q5_SQL_Par.sh
./project2026/scripts/run_q5_DF_NoBroadcast.sh

Generate PLOTS:

source .venv/bin/activate

python project2026/scripts/plot_q5.py \
  --tables-dir project2026/results/tables/q5 \
  --plots-dir project2026/results/plots \
  --top-n 10

### Q5 outputs

Spark DF parquate ID: spark-96c8586594b9460caeca2980c5152f78
Elapsed seconds: 54.649 seconds

Spark SQL Parquate ID: spark-9a871dee8c524e6f9d527516e52dd2aa
Elapsed seconds: 103.191 seconds

Spark DF no broad ID: spark-7a2b32a25a794a479140bfa93a18e8b0
Elapsed seconds: 65.593 seconds

METRICS:

q5_df_parquet_metrics.json
q5_df_parquet_no_broadcast_metrics.json
q5_sql_parquet_metrics.json

PLANS:

q5_df_parquet_no_broadcast_plan.txt
q5_df_parquet_plan.txt
q5_sql_parquet_plan.txt

PLOTS:

q5_top_airport_routes_by_avg_amount.png
q5_top_airport_routes_by_trips.png
q5_top_borough_flows_by_trips.png

TABLES:

df_parquet_airport_routes
df_parquet_airport_zone_examples
df_parquet_borough_flows
df_parquet_no_broadcast_airport_routes
df_parquet_no_broadcast_airport_zone_examples
df_parquet_no_broadcast_borough_flows
sql_parquet_airport_routes
sql_parquet_airport_zone_examples
sql_parquet_borough_flows

### Q5 outputs into local repo

METRICS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q5_df_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q5_sql_parquet_metrics.json project2026/results/metrics/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q5_df_parquet_no_broadcast_metrics.json project2026/results/metrics/

PLANS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q5_df_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q5_sql_parquet_plan.txt project2026/results/plans/
hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q5_df_parquet_no_broadcast_plan.txt project2026/results/plans/

TABLES:

hdfs dfs -get /user/$VDCLOUD_USER/project2026/results/tables/q5 project2026/results/tables/q5

## Q6

./project2026/scripts/run_q6_DF_Par.sh
./project2026/scripts/run_q6_SQL_Par.sh

./project2026/scripts/run_q6_OD_A.sh cold
./project2026/scripts/run_q6_OD_A.sh warm

./project2026/scripts/run_q6_OD_B.sh cold
./project2026/scripts/run_q6_OD_B.sh warm

./project2026/scripts/run_q6_OD_C.sh cold
./project2026/scripts/run_q6_OD_C.sh warm

Generate Plots:

source .venv/bin/activate

python project2026/scripts/plot_q6.py \
  --tables-dir project2026/results/tables/q6 \
  --od-metrics-dir project2026/results/metrics \
  --plots-dir project2026/results/plots \
  --top-n 15

### Q6 outputs

Spark DF Par ID: spark-fe2b06fb42ed49a69e37ef6698548fc8
Elapsed seconds: 81.587 seconds

Spark SQL ID: spark-49ed2828358e4097a6c16a65b1f4bfd6
Elapsed seconds: 186.761 seconds

Spark cold A Half ID: spark-59d4a6f6d1f741738160af228422652e
Elapsed seconds: 355.798 seconds
Spark warm A Half ID: spark-9546f228b19d4dac812ee6df71cc90eb
Elapsed seconds: 225.216 seconds

Spark cold B Half ID: spark-dddc36e3bf8645c8b3d3c0f5fb6e6aed
Elapsed seconds: 133.326 seconds
Spark warm B Half ID: spark-1dbd0e8ab4ad4be382b18a2493f235ae
Elapsed seconds: 120.434 seconds

Spark cold C Half ID: spark-0aba698d1b5f4cc88930b15b4dbd1e63
Elapsed seconds: 100.972 seconds
Spark warm C Half ID: spark-0c230232d06d4039981bdecc24f0aa4b
Elapsed seconds: 88.189 seconds

METRICS:

q6_df_parquet_metrics.json
q6_sql_parquet_metrics.json
q6_od_halves_configA_cold_metrics.json
q6_od_halves_configA_warm_metrics.json
q6_od_halves_configB_cold_metrics.json
q6_od_halves_configB_warm_metrics.json


PLANS:

q6_df_parquet_plan.txt
q6_sql_parquet_plan.txt
q6_od_halves_configA_cold_plan.txt
q6_od_halves_configA_warm_plan.txt
q6_od_halves_configB_cold_plan.txt
q6_od_halves_configB_warm_plan.txt

TABLES:

df_parquet_hourly_summary
df_parquet_top_abs
df_parquet_top_negative
df_parquet_top_positive

sql_parquet_hourly_summary
sql_parquet_top_abs
sql_parquet_top_negative
sql_parquet_top_positive

od_halves_configA_cold_top_decrease
od_halves_configA_cold_top_increase
od_halves_configA_warm_top_decrease
od_halves_configA_warm_top_increase

od_halves_configB_cold_top_decrease
od_halves_configB_cold_top_increase
od_halves_configB_warm_top_decrease
od_halves_configB_warm_top_increase

### Q6 outputs into local repo

METRICS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/metrics/q6_*.json project2026/results/metrics/

PLANS:

hdfs dfs -get -f /user/$VDCLOUD_USER/project2026/results/plans/q6_*.txt project2026/results/plans/

TABLES:

hdfs dfs -get /user/$VDCLOUD_USER/project2026/results/tables/q6 project2026/results/tables/q6