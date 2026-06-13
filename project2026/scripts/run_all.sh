#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

source ~/bigdata-env.sh

PROJECT_HDFS_BASE="/user/$VDCLOUD_USER/project2026"

RUN_Q6_SCALING="${RUN_Q6_SCALING:-1}"
RUN_PLOTS="${RUN_PLOTS:-1}"
TOP_N="${TOP_N:-15}"

mkdir -p project2026/results/logs
mkdir -p project2026/results/metrics
mkdir -p project2026/results/plans
mkdir -p project2026/results/plots
mkdir -p project2026/results/tables

log_msg() {
  echo
  echo "============================================================"
  echo "$1"
  echo "============================================================"
}

run_step() {
  local step_name="$1"
  shift

  local safe_name
  safe_name="$(echo "$step_name" | tr ' /' '__')"

  log_msg "START: $step_name"
  local start_ts
  start_ts="$(date +%s)"

  "$@" 2>&1 | tee "project2026/results/logs/${safe_name}.log"

  local end_ts
  end_ts="$(date +%s)"
  local elapsed
  elapsed=$((end_ts - start_ts))

  echo "END: $step_name (${elapsed}s)" | tee -a "project2026/results/logs/${safe_name}.log"
}

require_file() {
  local file_path="$1"
  if [[ ! -f "$file_path" ]]; then
    echo "Missing required file: $file_path"
    exit 1
  fi
}

run_script() {
  local script_path="$1"
  shift

  require_file "$script_path"
  bash "$script_path" "$@"
}

log_msg "Environment check"

echo "Repository root: $PROJECT_ROOT"
echo "VDCLOUD_USER: $VDCLOUD_USER"
echo "Kubernetes namespace: $VDCLOUD_USER-priv"
echo "HDFS project base: $PROJECT_HDFS_BASE"

kubectl -n "$VDCLOUD_USER-priv" get pods >/dev/null
hdfs dfs -ls "/user/$VDCLOUD_USER" >/dev/null

log_msg "Required script check"

required_scripts=(
  "project2026/scripts/run_eda.sh"

  "project2026/scripts/run_q1_RDD.sh"
  "project2026/scripts/run_q1_DF_CSV.sh"
  "project2026/scripts/run_q1_DF_Par.sh"
  "project2026/scripts/run_q1_SQL_Par.sh"

  "project2026/scripts/run_q2_DF_Builtin.sh"
  "project2026/scripts/run_q2_DF_UDF.sh"
  "project2026/scripts/run_q2_SQL.sh"

  "project2026/scripts/run_q3_DF_CSV.sh"
  "project2026/scripts/run_q3_DF_Par.sh"
  "project2026/scripts/run_q3_SQL_Par.sh"
  "project2026/scripts/run_q3_DF_NoPruning.sh"

  "project2026/scripts/run_q4_SQL_Par.sh"
  "project2026/scripts/run_q4_DF_Par.sh"
  "project2026/scripts/run_q4_SQL_CSV.sh"

  "project2026/scripts/run_q5_DF_Par.sh"
  "project2026/scripts/run_q5_SQL_Par.sh"
  "project2026/scripts/run_q5_DF_NoBroadcast.sh"

  "project2026/scripts/run_q6_DF_Par.sh"
  "project2026/scripts/run_q6_SQL_Par.sh"
  "project2026/scripts/run_q6_OD_A.sh"
  "project2026/scripts/run_q6_OD_B.sh"
  "project2026/scripts/run_q6_OD_C.sh"
)

for script in "${required_scripts[@]}"; do
  require_file "$script"
done

if [[ -f "project2026/scripts/run_prepare_parquet.sh" ]]; then
  run_step "prepare_parquet" run_script "project2026/scripts/run_prepare_parquet.sh"
else
  echo "NOTE: project2026/scripts/run_prepare_parquet.sh not found."
  echo "Assuming Parquet datasets already exist in HDFS."
fi

run_step "EDA" run_script "project2026/scripts/run_eda.sh"

run_step "Q1 RDD CSV" run_script "project2026/scripts/run_q1_RDD.sh"
run_step "Q1 DataFrame CSV" run_script "project2026/scripts/run_q1_DF_CSV.sh"
run_step "Q1 DataFrame Parquet" run_script "project2026/scripts/run_q1_DF_Par.sh"
run_step "Q1 Spark SQL Parquet" run_script "project2026/scripts/run_q1_SQL_Par.sh"

run_step "Q2 DF Builtin CSV" run_script project2026/scripts/run_q2_DF_Builtin_CSV.sh
run_step "Q2 DF Builtin Parquet" run_script project2026/scripts/run_q2_DF_Builtin.sh
run_step "Q2 DF UDF" run_script project2026/scripts/run_q2_DF_UDF.sh
run_step "Q2 SQL" run_script project2026/scripts/run_q2_SQL.sh

run_step "Q3 DataFrame CSV" run_script "project2026/scripts/run_q3_DF_CSV.sh"
run_step "Q3 DataFrame Parquet" run_script "project2026/scripts/run_q3_DF_Par.sh"
run_step "Q3 Spark SQL Parquet" run_script "project2026/scripts/run_q3_SQL_Par.sh"
run_step "Q3 DataFrame No Pruning" run_script "project2026/scripts/run_q3_DF_NoPruning.sh"

run_step "Q4 Spark SQL Parquet" run_script "project2026/scripts/run_q4_SQL_Par.sh"
run_step "Q4 DataFrame Parquet" run_script "project2026/scripts/run_q4_DF_Par.sh"
run_step "Q4 Spark SQL CSV" run_script "project2026/scripts/run_q4_SQL_CSV.sh"

run_step "Q5 DataFrame Parquet" run_script "project2026/scripts/run_q5_DF_Par.sh"
run_step "Q5 Spark SQL Parquet" run_script "project2026/scripts/run_q5_SQL_Par.sh"
run_step "Q5 DataFrame No Broadcast" run_script "project2026/scripts/run_q5_DF_NoBroadcast.sh"

run_step "Q6 DataFrame Parquet" run_script "project2026/scripts/run_q6_DF_Par.sh"
run_step "Q6 Spark SQL Parquet" run_script "project2026/scripts/run_q6_SQL_Par.sh"

if [[ "$RUN_Q6_SCALING" == "1" ]]; then
  run_step "Q6 OD-Halves A cold" run_script "project2026/scripts/run_q6_OD_A.sh" cold
  run_step "Q6 OD-Halves A warm" run_script "project2026/scripts/run_q6_OD_A.sh" warm

  run_step "Q6 OD-Halves B cold" run_script "project2026/scripts/run_q6_OD_B.sh" cold
  run_step "Q6 OD-Halves B warm" run_script "project2026/scripts/run_q6_OD_B.sh" warm

  run_step "Q6 OD-Halves C cold" run_script "project2026/scripts/run_q6_OD_C.sh" cold
  run_step "Q6 OD-Halves C warm" run_script "project2026/scripts/run_q6_OD_C.sh" warm
else
  echo "Skipping Q6 OD-Halves scaling because RUN_Q6_SCALING=$RUN_Q6_SCALING"
fi

log_msg "Copy HDFS outputs locally"

hdfs dfs -get -f "$PROJECT_HDFS_BASE/results/metrics/"*.json project2026/results/metrics/ || true
hdfs dfs -get -f "$PROJECT_HDFS_BASE/results/plans/"*.txt project2026/results/plans/ || true

rm -rf project2026/results/tables
hdfs dfs -get "$PROJECT_HDFS_BASE/results/tables" project2026/results/tables

if [[ "$RUN_PLOTS" == "1" ]]; then
  log_msg "Generate local plots"

  if [[ -f ".venv/bin/activate" ]]; then
    source .venv/bin/activate
  fi

  if [[ -f "project2026/scripts/plot_eda.py" && -d "project2026/results/tables/eda" ]]; then
    run_step "Plot EDA" python project2026/scripts/plot_eda.py \
      --tables-dir project2026/results/tables/eda \
      --plots-dir project2026/results/plots \
      --negative-counts-json project2026/results/metrics/eda_metrics.json
  fi

  if [[ -f "project2026/scripts/plot_q3.py" && -d "project2026/results/tables/q3" ]]; then
    run_step "Plot Q3" python project2026/scripts/plot_q3.py \
      --tables-dir project2026/results/tables/q3 \
      --plots-dir project2026/results/plots \
      --top-n 10
  fi

  if [[ -f "project2026/scripts/plot_q4.py" && -d "project2026/results/tables/q4" ]]; then
    run_step "Plot Q4" python project2026/scripts/plot_q4.py \
      --tables-dir project2026/results/tables/q4 \
      --plots-dir project2026/results/plots
  fi

  if [[ -f "project2026/scripts/plot_q5.py" && -d "project2026/results/tables/q5" ]]; then
    run_step "Plot Q5" python project2026/scripts/plot_q5.py \
      --tables-dir project2026/results/tables/q5 \
      --plots-dir project2026/results/plots \
      --top-n 10
  fi

  if [[ -f "project2026/scripts/plot_q6.py" && -d "project2026/results/tables/q6" ]]; then
    run_step "Plot Q6" python project2026/scripts/plot_q6.py \
      --tables-dir project2026/results/tables/q6 \
      --od-metrics-dir project2026/results/metrics \
      --plots-dir project2026/results/plots \
      --top-n "$TOP_N"
  fi
else
  echo "Skipping plots because RUN_PLOTS=$RUN_PLOTS"
fi

log_msg "Final local output check"

echo "Metrics:"
find project2026/results/metrics -maxdepth 1 -type f | sort

echo
echo "Plans:"
find project2026/results/plans -maxdepth 1 -type f | sort

echo
echo "Tables:"
find project2026/results/tables -maxdepth 3 -type f -name 'part-*' | sort

echo
echo "Plots:"
find project2026/results/plots -maxdepth 1 -type f | sort

log_msg "run_all.sh completed successfully"