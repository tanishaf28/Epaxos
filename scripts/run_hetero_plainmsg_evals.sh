#!/bin/bash
# ================================================================
# HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos
# Runs eval1 (ratio), eval2 (batchsize), eval3 (msgsize) across
# heterogeneous cluster sizes n=3,5,7,11. Mirrors WOC's
# run_plainmsg_evals.sh structure: max-inflight and crash injection live
# in their own dedicated scripts (run_hetero_maxinflight_eval.sh,
# run_hetero5_crash_eval.sh, run_hetero_crash_eval.sh), not here.
#
# Each size uses its own pre-built heterogeneous config (strong/weak mix
# differs per size, so sizes are NOT just slices of one big pool like the
# homo runner).
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/epaxos"
RESULT_ROOT="${SCRIPT_DIR}/results/hetero_plainmsg_evals"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false
CURRENT_CLIENT_COUNT=2  # updated by set_ips_for_n; default guards stop_plain_cluster before first call

# Client scaling mode (same semantics as WOC's run_plainmsg_evals.sh):
#   SCALE_CLIENTS=false (default) — FIXED_CLIENT_COUNT clients, round-robin routing
#   SCALE_CLIENTS=true  — clients = min(N,10), each pinned to one server
SCALE_CLIENTS="${SCALE_CLIENTS:-false}"
FIXED_CLIENT_COUNT="${FIXED_CLIENT_COUNT:-2}"

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

# Each size has its own pre-built heterogeneous config (strong/weak ratio
# is hand-picked per size — unlike the homo pool, you can't just slice the
# first N IPs of one big list and get a representative mix).
declare -A HETERO_CONFIG_FOR_N=(
    [3]="${REPO_ROOT}/config/cluster_hetero_3n_10c.conf"
    [5]="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"
    [7]="${REPO_ROOT}/config/cluster_hetero_7n_10c.conf"
    [11]="${REPO_ROOT}/config/cluster_hetero_11n_10c.conf"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
EVAL_ONLY="${1:-all}"

if ! [[ "$RUNTIME_SECONDS" =~ ^[0-9]+$ ]] || [ "$RUNTIME_SECONDS" -lt 1 ]; then
    echo "WARNING: RUNTIME_SECONDS=${RUNTIME_SECONDS} is invalid. Using 30."
    RUNTIME_SECONDS=30
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"

# ================================================================
# HELPERS
# ================================================================

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" "$USER@$host" "$*"
}

# set_ips_for_n <n> — populates CONFIG_PATH, SERVER_IPS, CLIENT_IPS from
# the size-specific config file (mirrors start_cluster_hetero.sh's own
# read_node_pool).
set_ips_for_n() {
    local n=$1
    local client_count
    if [ "${SCALE_CLIENTS}" = "true" ]; then
        client_count=$(( n < 10 ? n : 10 ))
    else
        client_count=${FIXED_CLIENT_COUNT}
    fi
    CONFIG_PATH="${HETERO_CONFIG_FOR_N[$n]:-}"
    if [ -z "$CONFIG_PATH" ]; then
        echo "ERROR: no heterogeneous config mapped for cluster size n=${n}"
        exit 1
    fi

    local node_pool=()
    mapfile -t node_pool < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH")
    SERVER_IPS=("${node_pool[@]:0:$n}")
    CLIENT_IPS=("${node_pool[@]:$n:$client_count}")
    CURRENT_CLIENT_COUNT="$client_count"

    if [ "${#SERVER_IPS[@]}" -ne "$n" ] || [ "${#CLIENT_IPS[@]}" -ne "$client_count" ]; then
        echo "ERROR: ${CONFIG_PATH} does not contain ${n} servers + ${client_count} clients"
        exit 1
    fi
}

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-sweep cleanup: purging stale EPaxos processes (all sizes)..."
    echo "=================================================="
    local all_ips=()
    for conf in "${HETERO_CONFIG_FOR_N[@]}"; do
        while IFS= read -r ip; do all_ips+=("$ip"); done \
            < <(awk 'NF >= 2 {print $2}' "$conf")
    done
    mapfile -t all_ips < <(printf '%s\n' "${all_ips[@]}" | sort -u)
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

start_plain_cluster() {
    echo "Starting EPaxos heterogeneous cluster (n=${CURRENT_N}, config=$(basename "$CONFIG_PATH"))..."
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_plain_cluster() {
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT="${CURRENT_CLIENT_COUNT}" CONFIG_PATH="$CONFIG_PATH" bash "$STOP_SCRIPT"
}

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${CURRENT_N}  threshold=${CURRENT_T}  runtime=${runtime}s"
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

# ================================================================
# CLEANUP
# ================================================================
cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_plain_cluster || true
    fi
}
trap cleanup EXIT

# ================================================================
# HELP
# ================================================================
if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_hetero_plainmsg_evals.sh [eval1|eval2|eval3|all|n3|n5|n7|n11]

  eval1   Independent ratio sweep (8 I2D points), batch=1, msgsize=512
  eval2   Batch size sweep (1,10,50,100,500,1000,2000), indep=90, msgsize=512
  eval3   Message size sweep (64,512,1024,2048,4096), indep=90, batch=1
  all     Run eval1, eval2, eval3 (default)
  n3/n5/n7/n11  Run the full eval suite for one cluster size only

Each eval runs across cluster sizes n = 3, 5, 7, 11 (heterogeneous configs).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/hetero_plainmsg_evals/<timestamp>/n<N>/<label>/
EOF
    exit 0
fi

[[ "${EVAL_ONLY}" == --* ]] && EVAL_ONLY="${EVAL_ONLY#--}"

case "${EVAL_ONLY}" in
    all|eval1|eval2|eval3|n3|n5|n7|n11) ;;
    *)
        echo "ERROR: unknown selector '${EVAL_ONLY}'. Run with --help."
        exit 1 ;;
esac

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║      HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos       ║"
echo "║   Evals: ratio / batchsize / msgsize                          ║"
echo "║   Sizes: n=3, 5, 7, 11  |  Clients: 2 (fixed VMs per size)     ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Result archive: $RUN_DIR"
echo ""

run_eval_suite_for_size() {
    local n=$1
    local t=$(( (n - 1) / 2 ))
    set_ips_for_n "$n"
    CURRENT_N="$n"
    CURRENT_T="$t"

    case "${EVAL_ONLY}" in
        n3)  [ "$n" -ne 3  ] && return 0 ;;
        n5)  [ "$n" -ne 5  ] && return 0 ;;
        n7)  [ "$n" -ne 7  ] && return 0 ;;
        n11) [ "$n" -ne 11 ] && return 0 ;;
    esac

    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║  EPaxos HETEROGENEOUS n=${n}  (t=${t})  config=$(basename "$CONFIG_PATH")"
    echo "╚════════════════════════════════════════════════════════════════╝"

    # EVAL 1: Ratio sweep (indep%; dependent = 100-indep, binary WOC-style split)
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" || "$EVAL_ONLY" == "n${n}" ]]; then
        echo "── EVAL 1: Ratio sweep ─────────────────────────────────────────"
        for indep in "100.0" "90.0" "80.0" "60.0" \
                     "40.0" "20.0" "10.0" "0.0"; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
                "CONFIG_PATH=${CONFIG_PATH}"
                "INDEP_RATIO=${indep}" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
                "LOG_LEVEL=info" "THRIFTY=false"
                "SCALE_CLIENTS=${SCALE_CLIENTS}" "FIXED_CLIENT_COUNT=${FIXED_CLIENT_COUNT}"
            )
            run_case "n${n}_eval1_ratio_${indep}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 2: batch size sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" || "$EVAL_ONLY" == "n${n}" ]]; then
        echo "── EVAL 2: Batch size sweep ────────────────────────────────────"
        for batch_size in 1 10 50 100 500 1000 2000; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
                "CONFIG_PATH=${CONFIG_PATH}"
                "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
                "LOG_LEVEL=info" "THRIFTY=false"
                "SCALE_CLIENTS=${SCALE_CLIENTS}" "FIXED_CLIENT_COUNT=${FIXED_CLIENT_COUNT}"
            )
            run_case "n${n}_eval2_batch_${batch_size}" "$RUNTIME_SECONDS"
        done
    fi

    # EVAL 3: message size sweep
    if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval3" || "$EVAL_ONLY" == "n${n}" ]]; then
        echo "── EVAL 3: Message size sweep ──────────────────────────────────"
        for msg_size in 64 512 1024 2048 4096; do
            BASE_ENV=(
                "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
                "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=${msg_size}" "MODE=1"
                "CONFIG_PATH=${CONFIG_PATH}"
                "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
                "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
                "LOG_LEVEL=info" "THRIFTY=false"
                "SCALE_CLIENTS=${SCALE_CLIENTS}" "FIXED_CLIENT_COUNT=${FIXED_CLIENT_COUNT}"
            )
            run_case "n${n}_eval3_msgsize_${msg_size}" "$RUNTIME_SECONDS"
        done
    fi
}

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do run_eval_suite_for_size "$n"; done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " All evaluations complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
