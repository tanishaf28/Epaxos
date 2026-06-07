#!/bin/bash
# ================================================================
# HOMOGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos
# Runs eval1 (ratio), eval2 (inflight), eval3 (batchsize),
# eval4 (msgsize) across cluster sizes n=3,5,7,11.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Source sampler functions
# shellcheck source=sampler_replacement.sh
source "${SCRIPT_DIR}/sampler_replacement.sh"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_homo.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_homo.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/homo_epaxos"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
_RUN_START_EPOCH=$(date +%s)

CLUSTER_ACTIVE=false
TIMESERIES_ENABLED=false

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
EVAL_ONLY="${1:-all}"

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

# All 50 nodes for aggressive pre-cleanup
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

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_homo_plainmsg.sh [selector]
 Selectors: all, eval1, eval2, eval3, eval4, n3, n5, n7, n11
EOF
    exit 0
fi

if [[ "${EVAL_ONLY}" == --* ]]; then
    EVAL_ONLY="${EVAL_ONLY#--}"
fi

remote_exec() {
    local host=$1; shift
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i "$SSH_KEY" "$USER@$host" "$*"
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

    if [ "${TIMESERIES_ENABLED:-false}" = "true" ]; then
        local timeline_src="${SCRIPT_DIR}/eval"
        if [ -d "$timeline_src" ]; then
            find "$timeline_src" -name "tps_timeline_*.csv" \
                -newer "${RUN_DIR}/.run_start_marker" \
                -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        fi
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

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: Purging stale EPaxos processes..."
    echo "=================================================="
    for ip in "${ALL_CLUSTER_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 epaxos 2>/dev/null" &
    done
    wait
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

run_eval_suite_for_size() {
    local n=$1
    local t=$(( (n - 1) / 2 ))
    CURRENT_N="$n"
    CURRENT_T="$t"

    case "${EVAL_ONLY}" in
        n3)  [ "$n" -ne 3  ] && return 0 ;;
        n5)  [ "$n" -ne 5  ] && return 0 ;;
        n7)  [ "$n" -ne 7  ] && return 0 ;;
        n11) [ "$n" -ne 11 ] && return 0 ;;
    esac

    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║  EPaxos HOMOGENEOUS n=${n}  (t=${t})  — starting eval suite       ║"
    echo "╚════════════════════════════════════════════════════════════════╝"

    # EVAL 1: Ratio sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" || "$EVAL_ONLY" == "n${n}" ]]; then
        for case in "100.0/0.0" "90.0/10.0" "80.0/20.0" "60.0/40.0" "40.0/60.0" "20.0/80.0" "10.0/90.0" "0.0/100.0"; do
            indep="${case%/*}"
            common="${case#*/}"
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=2" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=${indep}" "COMMON_RATIO=${common}"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval1_ratio_${indep}_common_${common}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 2: Max inflight sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" || "$EVAL_ONLY" == "n${n}" ]]; then
        for value in 1 2 3 4 5 8 10 15 20 25 30 35 40; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=2" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=${value}" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval2_inflight_${value}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 3: Batch size sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval3" || "$EVAL_ONLY" == "n${n}" ]]; then
        for batch_size in 1 10 50 100 500 1000 2000; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=2" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval_batching_${batch_size}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 4: Message size sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval4" || "$EVAL_ONLY" == "n${n}" ]]; then
        for msg_size in 64 512 1024 2048 4096; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=2" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=${msg_size}" "MODE=1"
                "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval_msgsize_${msg_size}" "$RUNTIME_SECONDS"
        done
    fi
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║       HOMOGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos         ║"
echo "║   Evals: ratio / inflight / batchsize / msgsize                ║"
echo "║   Sizes: n=3, 5, 7, 11  |  Clients: 2 (fixed VMs)              ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do run_eval_suite_for_size "$n"; done

echo -e "\n==================================================\n All EPaxos evaluations complete\n Results archived in: $RUN_DIR\n=================================================="