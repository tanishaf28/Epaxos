#!/bin/bash
# ================================================================
# READ RATIO EVALUATION RUNNER (HOMOGENEOUS) — EPaxos
#
# Sweeps -readratio (% of ops that are reads vs writes) against
# homogeneous clusters of n = 3, 5, 7, 11 replicas, indep=90/batch=1/
# msgsize=512 fixed. Kept separate from run_homo_plainmsg_evals.sh's
# eval1/2/3, matching WOC's pattern of one dedicated script per knob
# that isn't part of the core ratio/batch/msgsize trio.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_homo.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_homo.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/homo_readratio_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
READ_RATIO_VALUES=(0 10 30 50 70 100)

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

# All 50 nodes for aggressive pre-cleanup (same pool as run_homo_plainmsg_evals.sh)
ALL_CLUSTER_IPS=(
    "192.168.73.220" "192.168.73.240" "192.168.73.108" "192.168.73.179" "192.168.73.154"
    "192.168.73.45"  "192.168.73.229" "192.168.73.109" "192.168.73.203" "192.168.73.30"
    "192.168.73.19"  "192.168.73.127" "192.168.73.75"  "192.168.73.142" "192.168.73.112"
    "192.168.73.88"  "192.168.73.140" "192.168.73.191" "192.168.73.226" "192.168.73.126"
    "192.168.73.96"  "192.168.73.143" "192.168.73.145" "192.168.73.135" "192.168.73.12"
    "192.168.73.180" "192.168.73.113" "192.168.73.129" "192.168.73.33"  "192.168.73.205"
    "192.168.73.55"  "192.168.73.209" "192.168.73.207" "192.168.73.102" "192.168.73.210"
    "192.168.73.153" "192.168.73.168" "192.168.73.23"  "192.168.73.89"  "192.168.73.170"
    "192.168.73.222" "192.168.73.8"   "192.168.73.238" "192.168.73.214" "192.168.73.195"
    "192.168.73.247" "192.168.73.164" "192.168.73.151" "192.168.73.73"  "192.168.73.239"
)

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_homo_readratio_eval.sh

Sweeps READ_RATIO over 0,10,30,50,70,100 with INDEP_RATIO=90.0,
BATCHSIZE=1, MSG_SIZE=512 fixed, across cluster sizes n = 3, 5, 7, 11
(homogeneous, sliced from config/cluster_homo.conf).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/homo_readratio_eval/<timestamp>/n<N>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"

remote_exec() {
    local host=$1; shift
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" "$USER@$host" "$*"
}

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: Purging stale EPaxos processes..."
    echo "=================================================="
    for ip in "${ALL_CLUSTER_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 epaxos 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" \
            -newer "${RUN_DIR}/.run_start_marker" \
            -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi
}

start_homo_cluster() {
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
    CLUSTER_ACTIVE=true
}

stop_homo_cluster() {
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT=2 bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
}

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${CURRENT_N}  threshold=${CURRENT_T}  runtime=${runtime}s"
    echo "=================================================="

    start_homo_cluster
    sleep "$runtime"
    stop_homo_cluster
    archive_latest_result "$label"

    echo "  Cooling down to release TCP ports..."
    sleep 5
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then stop_homo_cluster || true; fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          READ RATIO EVALUATION RUNNER (HOMOGENEOUS)             ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (homogeneous)           ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do
    t=$(( (n - 1) / 2 ))
    CURRENT_N="$n"
    CURRENT_T="$t"

    for read_ratio in "${READ_RATIO_VALUES[@]}"; do
        BASE_ENV=(
            "NUM_SERVERS=${n}" "NUM_CLIENTS=2" "THRESHOLD=${t}" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "INDEP_RATIO=90.0" "NUM_OBJECTS=100000" "READ_RATIO=${read_ratio}"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
        )
        run_case "n${n}_eval_readratio_${read_ratio}" "$RUNTIME_SECONDS"
    done
done

echo ""
echo "=================================================="
echo " Read ratio evaluation sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
