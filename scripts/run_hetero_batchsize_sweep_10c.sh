#!/bin/bash
# ================================================================
# EVAL 6 (EPaxos port): Batch Size Sweep x Cluster Size, 10-client pool
#
# Same sweep as run_hetero_plainmsg_evals.sh's eval2 (BATCHSIZE over
# 1,10,50,100,500,1000,2000, indep=90, msgsize=512), across cluster sizes
# n=3,5,7,11 -- but against the dedicated 10-VM client pool
# (config/cluster_hetero_{n}n_10c.conf) instead of the existing 2-client
# heterogeneous configs, mirroring WOC's eval_6_batchsize_size_sweep.sh.
#
# Added alongside, not replacing, run_hetero_plainmsg_evals.sh's eval2 --
# that script and its 2-client configs are untouched.
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
RESULT_ROOT="${SCRIPT_DIR}/results/hetero_batchsize_sweep_10c"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

# 10-client configs (servers + a dedicated 10-VM client pool), not the
# existing 2-client cluster_hetero_{n}n_*w.conf files.
declare -A HETERO_CONFIG_FOR_N=(
    [3]="${REPO_ROOT}/config/cluster_hetero_3n_10c.conf"
    [5]="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"
    [7]="${REPO_ROOT}/config/cluster_hetero_7n_10c.conf"
    [11]="${REPO_ROOT}/config/cluster_hetero_11n_10c.conf"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-40}"
# A number, or the literal string "match" to run clients=servers for each
# size in the sweep (client VMs are cycled/reused when a size needs more
# clients than the 10-VM pool has, e.g. n=11 matched — see
# start_cluster_hetero.sh's read_node_pool).
CLIENT_COUNT="${CLIENT_COUNT:-2}"
TEST_CASES=(1 10 50 100 500 1000 2000)
if [ -n "${BATCH_CASES:-}" ]; then
    read -r -a TEST_CASES <<< "$BATCH_CASES"
fi

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_hetero_batchsize_sweep_10c.sh

Sweeps BATCHSIZE over 1,10,50,100,500,1000,2000 (matches WOC's
eval_6_batchsize_size_sweep.sh / this repo's run_hetero_plainmsg_evals.sh
eval2) with INDEP_RATIO=90.0, MSG_SIZE=512 fixed, across cluster sizes
n=3,5,7,11, against a dedicated 10-VM client pool
(config/cluster_hetero_{n}n_10c.conf).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLIENT_COUNT=10             client count -- a number, or "match" to run
                               clients=servers for each size
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/hetero_batchsize_sweep_10c/<timestamp>/<label>/
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

set_ips_for_n() {
    local n=$1
    local client_count=$2
    CONFIG_PATH="${HETERO_CONFIG_FOR_N[$n]:-}"
    if [ -z "$CONFIG_PATH" ]; then
        echo "ERROR: no 10-client heterogeneous config mapped for cluster size n=${n}"
        exit 1
    fi
    local node_pool=()
    mapfile -t node_pool < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH")
    SERVER_IPS=("${node_pool[@]:0:$n}")
    if [ "${#SERVER_IPS[@]}" -ne "$n" ]; then
        echo "ERROR: ${CONFIG_PATH} does not contain ${n} servers"
        exit 1
    fi
    # CLIENT_IPS here is informational only (used for cleanup_all_nodes'
    # pool-wide sort -u pkill, which already covers every IP in the config
    # regardless) -- the actual client launch is start_cluster_hetero.sh's
    # job, which cycles/reuses client VMs when client_count exceeds the
    # 10-VM pool (e.g. matched mode at n=11).
    local client_pool=("${node_pool[@]:$n}")
    CLIENT_IPS=()
    if [ "${#client_pool[@]}" -gt 0 ]; then
        for ((k = 0; k < client_count; k++)); do
            CLIENT_IPS+=("${client_pool[$((k % ${#client_pool[@]}))]}")
        done
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
    echo "Starting EPaxos heterogeneous cluster (n=${CURRENT_N}, clients=${CURRENT_CLIENT_COUNT}, config=$(basename "$CONFIG_PATH"))..."
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_plain_cluster() {
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT="$CURRENT_CLIENT_COUNT" CONFIG_PATH="$CONFIG_PATH" bash "$STOP_SCRIPT"
}

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "  n=${CURRENT_N}  threshold=${CURRENT_T}  clients=${CURRENT_CLIENT_COUNT}  runtime=${runtime}s"
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
echo "║   BATCH SIZE SWEEP x CLUSTER SIZE, 10-CLIENT POOL (EPaxos)     ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (heterogeneous)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do
    t=$(( (n - 1) / 2 ))
    if [ "$CLIENT_COUNT" = "match" ]; then
        CURRENT_CLIENT_COUNT=$n
    else
        CURRENT_CLIENT_COUNT=$CLIENT_COUNT
    fi
    set_ips_for_n "$n" "$CURRENT_CLIENT_COUNT"
    CURRENT_N="$n"
    CURRENT_T="$t"

    for batch_size in "${TEST_CASES[@]}"; do
        BASE_ENV=(
            "NUM_SERVERS=${n}" "NUM_CLIENTS=${CURRENT_CLIENT_COUNT}" "THRESHOLD=${t}" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
            "CONFIG_PATH=${CONFIG_PATH}"
            "INDEP_RATIO=${INDEP_RATIO_FIXED:-90.0}" "NUM_OBJECTS=1000" "READ_RATIO=0.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "LOG_LEVEL=info" "THRIFTY=false"
            "BATCHWINDOWUS=${BATCHWINDOWUS:-0}" "MAXBATCH=${MAXBATCH:-1}"
        )
        run_case "n${n}_batch_${batch_size}" "$RUNTIME_SECONDS"
    done
done

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR"

echo ""
echo "=================================================="
echo " Batch size sweep (10-client pool) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
