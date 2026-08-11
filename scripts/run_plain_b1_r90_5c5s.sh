#!/bin/bash
# ================================================================
# HETERO-5 PLAIN STEADY-STATE RUN (EPaxos): NUM_SERVERS=5, NUM_CLIENTS=5,
# BATCHSIZE=1, MAX_INFLIGHT=5, INDEP_RATIO=90. No netem delay, no crash --
# single steady-state observation window, for meters.go's per-run summary
# (P50/P95/P99, fast/slow path latency).
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
CONFIG_PATH="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_plain_b1_r90_5c5s"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=5"
    "CONFIG_PATH=${CONFIG_PATH}"
    "THRESHOLD=2"
    "BATCHSIZE=1"
    "INDEP_RATIO=90.0"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "ENABLE_TIMESERIES=true"
    "LOG_LEVEL=info"
)

mkdir -p "$RUN_DIR"
MARKER="${RUN_DIR}/.run_start_marker"
touch "$MARKER"

archive_case() {
    local label=$1
    local case_dir="${RUN_DIR}/${label}"
    mkdir -p "$case_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$MARKER" -exec cp {} "${case_dir}/" \; 2>/dev/null || true
    fi
    find "${SCRIPT_DIR}/eval" -mindepth 2 -maxdepth 2 -name "tps_timeline_*.csv" -newer "$MARKER" -exec cp {} "${case_dir}/" \; 2>/dev/null || true
    # -newer "$MARKER" is essential here -- without it this greps every
    # historical eval CSV ever produced, not just this run's.
    find "${SCRIPT_DIR}/eval" -mindepth 2 -maxdepth 2 -name "*_eval_*.csv" -newer "$MARKER" -exec cp {} "${case_dir}/" \; 2>/dev/null || true
    find "${SCRIPT_DIR}/eval" -mindepth 2 -maxdepth 2 -name "s*_epaxos_*.csv" -newer "$MARKER" -exec cp {} "${case_dir}/" \; 2>/dev/null || true
    echo "  Archived results to: $case_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "================================================================"
echo " HETERO-5 PLAIN STEADY-STATE RUN (EPaxos): indep=90, batch=1, maxinflight=5, 5s/5c"
echo "================================================================"
echo "Result archive: $RUN_DIR"

CLUSTER_ACTIVE=true
env "${BASE_ENV[@]}" bash "$START_SCRIPT"

echo "  Running for ${RUNTIME_SECONDS}s..."
sleep "$RUNTIME_SECONDS"

CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
CLUSTER_ACTIVE=false

archive_case "plain_indep90_b1_5c5s"

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 plain steady-state run complete (EPaxos)"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
