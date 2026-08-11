#!/bin/bash
# ================================================================
# CLIENT SCALING EVALUATION RUNNER (HOMOGENEOUS) — EPaxos
#
# Fixed 5-server cluster. Sweeps total client count from 1 up to 50,
# against config/cluster_homo.conf via start_cluster_homo.sh /
# stop_cluster_homo.sh, which cycle through their client-VM pool once
# NUM_CLIENTS exceeds its size (client VMs get a 2nd/3rd/... process as
# needed). Homogeneous counterpart of run_hetero_client_scaling_eval.sh.
#
# Fixed across the sweep: INDEP_RATIO=90.0, BATCHSIZE=1, MSG_SIZE=512.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_homo.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_homo.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/epaxos"
RESULT_ROOT="${SCRIPT_DIR}/results/homo_client_scaling_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

NUM_SERVERS=5
THRESHOLD=$(( (NUM_SERVERS - 1) / 2 ))
CONFIG_PATH="${REPO_ROOT}/config/cluster_homo.conf"

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
CLIENT_COUNTS=(1 2 3 4 5 10 15 20 25 30 35 40 45 50)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_homo_client_scaling_eval.sh

Sweeps total client count over 1,2,3,4,5,10,15,20,25,30,35,40,45,50 against a
fixed 5-server homogeneous cluster (config/cluster_homo.conf), cycling
through the client-VM pool once the count exceeds its size. INDEP_RATIO=90.0,
BATCHSIZE=1, MSG_SIZE=512 fixed.

Environment overrides:
  RUNTIME_SECONDS=30   wall-clock seconds per run

Results archived under: results/homo_client_scaling_eval/<timestamp>/<label>/
EOF
    exit 0
fi

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
    local all_ips=()
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
            cp "$merged_dir"/*.csv "$dest_dir/" 2>/dev/null || true
        fi
    fi

    touch "$marker"
    echo "  Archived results to: $dest_dir"
    ls -1 "$dest_dir"/*.csv 2>/dev/null | sed 's|.*/|    |' || echo "  (no CSVs found)"
}

start_homo_cluster() {
    echo "Starting EPaxos homogeneous cluster (n=${NUM_SERVERS}, clients=${CURRENT_CLIENT_COUNT})..."
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_homo_cluster() {
    SERVER_COUNT="$NUM_SERVERS" CLIENT_COUNT="$CURRENT_CLIENT_COUNT" bash "$STOP_SCRIPT"
}

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${NUM_SERVERS}  clients=${CURRENT_CLIENT_COUNT}  runtime=${runtime}s"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    start_homo_cluster
    sleep "$runtime"
    stop_homo_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_homo_cluster || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          CLIENT SCALING EVALUATION RUNNER (HOMOGENEOUS)         ║"
echo "║          5 servers fixed, clients swept 1..50 (pool cycled)     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for client_count in "${CLIENT_COUNTS[@]}"; do
    CURRENT_CLIENT_COUNT="$client_count"
    BASE_ENV=(
        "NUM_SERVERS=${NUM_SERVERS}" "NUM_CLIENTS=${client_count}" "THRESHOLD=${THRESHOLD}" "OPS=0"
        "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
        "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
        "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
        "LOG_LEVEL=info" "THRIFTY=false"
    )
    run_case "clients_${client_count}" "$RUNTIME_SECONDS"
done

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Client scaling evaluation sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
