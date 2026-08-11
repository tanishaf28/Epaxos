#!/bin/bash
# ================================================================
# READ RATIO EVALUATION RUNNER (HETEROGENEOUS) — EPaxos
#
# Sweeps -readratio (% of ops that are reads vs writes) against
# heterogeneous clusters of n = 3, 5, 7, 11 replicas, indep=90/batch=1/
# msgsize=512 fixed. Kept separate from run_hetero_plainmsg_evals.sh's
# eval1/2/3, matching WOC's pattern of one dedicated script per knob
# that isn't part of the core ratio/batch/msgsize trio (see WOC's
# eval_4_read_ratio.sh).
#
# What this actually measures: state.go's per-key conflict table now
# enforces the read/write interference rule (write-write and write-read
# conflict, read-read does not) — concurrent READs to the same key no
# longer create a false dependency on each other. Before that fix,
# -readratio had zero effect on consensus behavior (every op, read or
# write, registered identically in the conflict table), so this sweep
# would have shown flat noise across all read-ratio values.
#
# Uses PlainMsg (EVAL_TYPE=0), not MongoDB, mirroring WOC's same choice
# in eval_4_read_ratio.sh and for the same reason: in MongoDB mode the
# real DB operation comes from the preloaded workload trace file's own
# Op field, not from -readratio/CmdType, so a MongoDB-mode sweep here
# wouldn't measure anything either.
#
# Unlike WOC, there is no -readmode (fast/safe) axis to fix here — EPaxos
# has no local-read bypass; every read still goes through full
# PreAccept/Accept/Commit/Execute consensus, just without the spurious
# read-read conflict.
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
RESULT_ROOT="${SCRIPT_DIR}/results/hetero_readratio_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false

ALL_CLUSTER_SIZES=(3 5 7 11)
if [ -n "${CLUSTER_SIZES:-}" ]; then
    read -r -a ALL_CLUSTER_SIZES <<< "$CLUSTER_SIZES"
fi

declare -A HETERO_CONFIG_FOR_N=(
    [3]="${REPO_ROOT}/config/cluster_hetero_3n_2s_1w.conf"
    [5]="${REPO_ROOT}/config/cluster_hetero_5n_2s_3w.conf"
    [7]="${REPO_ROOT}/config/cluster_hetero_7n_3s_4w.conf"
    [11]="${REPO_ROOT}/config/cluster_hetero_11n_4s_7w.conf"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
READ_RATIO_VALUES=(0 25 50 75 100)  # matches WOC's eval_4_read_ratio.sh exactly, for apples-to-apples comparison

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_hetero_readratio_eval.sh

Sweeps READ_RATIO over 0,25,50,75,100 (matches WOC's eval_4_read_ratio.sh) with INDEP_RATIO=90.0,
BATCHSIZE=1, MSG_SIZE=512 fixed, across cluster sizes n = 3, 5, 7, 11
(heterogeneous configs).

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CLUSTER_SIZES="3 5 7 11"    override the cluster-size sweep

Results archived under: results/hetero_readratio_eval/<timestamp>/n<N>/<label>/
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
    CONFIG_PATH="${HETERO_CONFIG_FOR_N[$n]:-}"
    if [ -z "$CONFIG_PATH" ]; then
        echo "ERROR: no heterogeneous config mapped for cluster size n=${n}"
        exit 1
    fi
    local node_pool=()
    mapfile -t node_pool < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH")
    SERVER_IPS=("${node_pool[@]:0:$n}")
    CLIENT_IPS=("${node_pool[@]:$n:2}")
    if [ "${#SERVER_IPS[@]}" -ne "$n" ] || [ "${#CLIENT_IPS[@]}" -ne 2 ]; then
        echo "ERROR: ${CONFIG_PATH} does not contain ${n} servers + 2 clients"
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
    SERVER_COUNT="$CURRENT_N" CLIENT_COUNT=2 CONFIG_PATH="$CONFIG_PATH" bash "$STOP_SCRIPT"
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

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_plain_cluster || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          READ RATIO EVALUATION RUNNER (HETEROGENEOUS)           ║"
echo "║         Cluster sizes: n = 3, 5, 7, 11 (heterogeneous)         ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

for n in "${ALL_CLUSTER_SIZES[@]}"; do
    t=$(( (n - 1) / 2 ))
    set_ips_for_n "$n"
    CURRENT_N="$n"
    CURRENT_T="$t"

    for read_ratio in "${READ_RATIO_VALUES[@]}"; do
        BASE_ENV=(
            "NUM_SERVERS=${n}" "NUM_CLIENTS=2" "THRESHOLD=${t}" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "CONFIG_PATH=${CONFIG_PATH}"
            "INDEP_RATIO=90.0" "NUM_OBJECTS=1000" "READ_RATIO=${read_ratio}"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "LOG_LEVEL=info" "THRIFTY=false"
        )
        run_case "n${n}_eval_readratio_${read_ratio}" "$RUNTIME_SECONDS"
    done
done

echo ""
echo "=================================================="
echo " Read ratio evaluation sweep complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
