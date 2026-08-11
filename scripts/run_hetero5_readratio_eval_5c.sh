#!/bin/bash
# ================================================================
# READ RATIO EVALUATION (HETERO-5, 5-CLIENT POOL) — EPaxos
#
# Fixed n=5 variant of run_hetero_readratio_eval.sh, using the 5-client
# pool (config/cluster_hetero_5n_10c.conf) instead of that script's
# 2-client hetero configs -- matches the 5-server/5-client convention
# used by the delay-sweep scripts (run_hetero5_ratio_delay_5c_b*.sh) so
# all four systems (woc, epaxos, raft, cabinet) run read-ratio at the
# same client/server count.
#
# Sweeps -readratio over 0,25,50,75,100 at fixed INDEP_RATIO=90,
# BATCHSIZE=1, MSG_SIZE=512.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_readratio_eval_5c"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

CONFIG_PATH="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"
NUM_SERVERS=5
NUM_CLIENTS=5
THRESHOLD=$(( (NUM_SERVERS - 1) / 2 ))
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
READ_RATIO_VALUES=(0 25 50 75 100)
CLUSTER_ACTIVE=false

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" "$USER@$host" "$*"
}

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale EPaxos processes..."
    echo "=================================================="
    mapfile -t all_ips < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH" | sort -u)
    for ip in "${all_ips[@]}"; do
        remote_exec "$ip" "pkill -9 epaxos 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    local marker="${RUN_DIR}/.last_archive_ts"
    local find_args=()
    if [ -f "$marker" ]; then
        find_args=(-newer "$marker")
    fi

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" "${find_args[@]}" \
            -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1) || true
            [ -n "$newest" ] && cp "$newest" "$dest_dir/"
        fi
    fi

    touch "$marker"
    echo "  Archived results to: $dest_dir"
    ls -1 "$dest_dir"/*.csv 2>/dev/null | sed 's|.*/|    |' || echo "  (no CSVs found)"
}

start_plain_cluster() {
    echo "Starting EPaxos heterogeneous cluster (n=${NUM_SERVERS}, clients=${NUM_CLIENTS})..."
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_plain_cluster() {
    CLIENT_COUNT="$NUM_CLIENTS" CONFIG_PATH="$CONFIG_PATH" SERVER_COUNT="$NUM_SERVERS" bash "$STOP_SCRIPT"
}

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${NUM_SERVERS}  clients=${NUM_CLIENTS}  threshold=${THRESHOLD}  runtime=${runtime}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    start_plain_cluster
    sleep "$runtime"
    stop_plain_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_plain_cluster || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   READ RATIO EVAL (EPaxos, n=5, 5-client pool)                 ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

# Seed the archive marker before the first case runs. Without this,
# archive_latest_result's "if [ -f "$marker" ]" check finds nothing on the
# very first case, so find_args stays empty and it copies EVERY merged
# CSV ever produced by any epaxos run (eval/merged/ accumulates forever,
# stop_cluster_hetero.sh never cleans it) into that case's folder.
touch "${RUN_DIR}/.last_archive_ts"

for read_ratio in "${READ_RATIO_VALUES[@]}"; do
    BASE_ENV=(
        "NUM_SERVERS=${NUM_SERVERS}" "NUM_CLIENTS=${NUM_CLIENTS}" "THRESHOLD=${THRESHOLD}" "OPS=0"
        "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
        "CONFIG_PATH=${CONFIG_PATH}"
        "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=${read_ratio}"
        "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
        "LOG_LEVEL=info" "THRIFTY=false"
    )
    run_case "n5_eval_readratio_${read_ratio}" "$RUNTIME_SECONDS"
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Read ratio evaluation (5c) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
