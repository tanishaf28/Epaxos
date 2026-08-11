#!/bin/bash
# ================================================================
# HOMOGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos
# Runs eval1 (ratio), eval2 (batchsize), eval3 (msgsize) across cluster
# sizes n=3,5,7,11. Mirrors WOC's run_homo_plainmsg_evals.sh structure:
# max-inflight and read-ratio sweeps live in their own dedicated scripts
# (run_hetero_maxinflight_eval.sh, run_homo_readratio_eval.sh), not here.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_homo.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_homo.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/homo_plainmsg_evals"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
_RUN_START_EPOCH=$(date +%s)

CLUSTER_ACTIVE=false
TIMESERIES_ENABLED=false

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
EVAL_ONLY="${1:-all}"
# A number, or the literal string "match" to run clients=servers for each
# size in the sweep (client VMs are cycled/reused if a size needs more
# clients than start_cluster_homo.sh's CLIENT_HOST_IPS pool has).
CLIENT_COUNT="${CLIENT_COUNT:-2}"

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
Usage: bash run_homo_plainmsg_evals.sh [eval1|eval2|eval3|all|n3|n5|n7|n11]

  eval1   Independent ratio sweep (8 I2D points), batch=1, msgsize=512
  eval2   Batch size sweep (1,10,50,100,500,1000,2000), indep=90, msgsize=512
  eval3   Message size sweep (64,512,1024,2048,4096), indep=90, batch=1
  all     Run eval1, eval2, eval3 (default)
  n3/n5/n7/n11  Run the full eval suite for one cluster size only

Each eval runs across cluster sizes n = 3, 5, 7, 11 (homogeneous, sliced
from config/cluster_homo.conf).

Environment overrides:
  RUNTIME_SECONDS=30   wall-clock seconds per run
  CLUSTER_SIZES="3 5 7 11"   override the cluster-size sweep
  CLIENT_COUNT=2       client count per size -- a number, or "match" to run
                       clients=servers for each size

Results archived under: results/homo_plainmsg_evals/<timestamp>/n<N>/<label>/
EOF
    exit 0
fi

if [[ "${EVAL_ONLY}" == --* ]]; then
    EVAL_ONLY="${EVAL_ONLY#--}"
fi

case "${EVAL_ONLY}" in
    all|eval1|eval2|eval3|n3|n5|n7|n11) ;;
    *)
        echo "ERROR: unknown selector '${EVAL_ONLY}'. Run with --help."
        exit 1
        ;;
esac

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
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT="$CURRENT_CLIENT_COUNT" bash "$STOP_SCRIPT"
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
    echo "  n=${CURRENT_N}  threshold=${CURRENT_T}  clients=${CURRENT_CLIENT_COUNT}  runtime=${runtime}s"
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
    if [ "$CLIENT_COUNT" = "match" ]; then
        CURRENT_CLIENT_COUNT=$n
    else
        CURRENT_CLIENT_COUNT=$CLIENT_COUNT
    fi

    case "${EVAL_ONLY}" in
        n3)  [ "$n" -ne 3  ] && return 0 ;;
        n5)  [ "$n" -ne 5  ] && return 0 ;;
        n7)  [ "$n" -ne 7  ] && return 0 ;;
        n11) [ "$n" -ne 11 ] && return 0 ;;
    esac

    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║  EPaxos HOMOGENEOUS n=${n}  (t=${t})  — starting eval suite       ║"
    echo "╚════════════════════════════════════════════════════════════════╝"

    # EVAL 1: Ratio sweep (indep%; dependent = 100-indep, binary WOC-style split)
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" || "$EVAL_ONLY" == "n${n}" ]]; then
        for indep in "100.0" "90.0" "80.0" "60.0" "40.0" "20.0" "10.0" "0.0"; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "INDEP_RATIO=${indep}" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval1_ratio_${indep}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 2: Batch size sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" || "$EVAL_ONLY" == "n${n}" ]]; then
        for batch_size in 1 10 50 100 500 1000 2000; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
                "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval2_batch_${batch_size}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 3: Message size sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval3" || "$EVAL_ONLY" == "n${n}" ]]; then
        for msg_size in 64 512 1024 2048 4096; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=${msg_size}" "MODE=1"
                "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5" "LOG_LEVEL=info" "THRIFTY=false"
            )
            run_case "n${n}_eval3_msgsize_${msg_size}" "$RUNTIME_SECONDS"
        done
    fi
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║       HOMOGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos         ║"
echo "║   Evals: ratio / batchsize / msgsize                           ║"
echo "║   Sizes: n=3, 5, 7, 11                                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do run_eval_suite_for_size "$n"; done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo -e "\n==================================================\n All EPaxos evaluations complete\n Results archived in: $RUN_DIR\n Summary CSV: $RUN_DIR/extracted_metrics.csv\n=================================================="
