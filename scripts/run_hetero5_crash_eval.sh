#!/bin/bash
# ================================================================
# HETERO-5 CRASH EVAL (EPAXOS): follower crashes at replica 2, 3, 4 on
# the fixed 5-server heterogeneous cluster.
#
#   ./run_hetero5_crash_eval.sh [replica2|replica3|replica4|leader|all] [batchsize] [indep_ratio]
#
# Defaults to running all three follower cases. Uses
# sampler_replacement.sh's run_crash_case_sampled (per-client in-process
# TPS timeline + event injection) -- this script is now its only caller.
#
# NUM_CLIENTS=5, BATCHSIZE=100 (default), MSG_SIZE=512, RUNTIME_SECONDS=60,
# and the 5-host CLIENT_IPS list are shared byte-for-byte with woc's and
# cabinet's own crash-eval drivers (woc/scripts/run_hetero5_crash_eval.sh;
# cabinet/scripts/run_hetero_crash_{cab,raft}.sh) so all four protocols'
# crash evals run under identical offered load and are comparable.
# THRESHOLD=2 matches Raft's majority-quorum semantics; Cabinet's own
# crash script uses t=1 (its tunable priority-quorum default) and CORA's
# uses t=2 - see those scripts' own headers for why.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# shellcheck source=sampler_replacement.sh
source "${REPO_ROOT}/sampler_replacement.sh"

TARGET="${1:-all}"
BATCHSIZE_OVERRIDE="${2:-100}"
INDEP_RATIO_OVERRIDE="${3:-90.0}"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/epaxos"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_crash_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
touch_marker="${RUN_DIR}/.run_start_marker"

CLUSTER_ACTIVE=false

RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
CRASH_TRIGGER_SECONDS="${CRASH_TRIGGER_SECONDS:-10}"

CONFIG_PATH="/home/ubuntu/epaxos/config/cluster_hetero_5n_10c.conf"

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=5"
    "THRESHOLD=2"
    "BATCHSIZE=${BATCHSIZE_OVERRIDE}"
    "MSG_SIZE=512"
    "INDEP_RATIO=${INDEP_RATIO_OVERRIDE}"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "ENABLE_TIMESERIES=true"
    "LOG_LEVEL=info"
    "CONFIG_PATH=${CONFIG_PATH}"
)

SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.192"
    "192.168.73.134"
    "192.168.73.132"
)

# Was (218, 219) -- didn't match the client slice start_cluster_hetero.sh's
# read_node_pool() actually assigns for NUM_CLIENTS=2 (159, 84), so event
# injection was silently targeting hosts with no client process running.
# Also: start_cluster_hetero.sh's CONFIG_PATH defaults to
# cluster_hetero_5n_2s_3w.conf, which only has 2 client IPs (218, 219) in
# its pool -- with 5 clients requested that cycled to [218,219,218,219,218],
# putting 3 client processes on 2 already-occupied hosts, which is why
# clients 7/8/9 kept dying. Now explicitly pinned to
# cluster_hetero_5n_10c.conf (BASE_ENV above), same config woc/cabinet/raft
# and the delay-cycle script use, giving the true 5-distinct-host slice
# below -- same list used by woc/cabinet/raft's crash scripts so all 4
# protocols run on identical client VMs.
CLIENT_IPS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.218"
    "192.168.73.219"
    "192.168.73.25"
)

mkdir -p "$RUN_DIR"
touch "$touch_marker"

# ----------------------------------------------------------------
# remote_exec / kill_epaxos_on_node — needed by sampler_replacement.sh's
# run_crash_case_sampled, which calls kill_epaxos_on_node directly. This
# script is now their only caller (run_hetero_plainmsg_evals.sh's copies
# are no longer the canonical source), so they're defined locally rather
# than sourced from elsewhere.
# ----------------------------------------------------------------
remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" "$USER@$host" "$*"
}

kill_epaxos_on_node() {
    local ip=$1
    local label=${2:-epaxos}

    if remote_exec "$ip" "pgrep -x epaxos >/dev/null 2>&1"; then
        echo "  Killing ${label} on ${ip}..."
        remote_exec "$ip" "pkill -TERM -x epaxos 2>/dev/null || true" || true
        sleep 1
        if remote_exec "$ip" "pgrep -x epaxos >/dev/null 2>&1"; then
            remote_exec "$ip" "pkill -KILL -x epaxos 2>/dev/null || true" || true
            sleep 1
        fi
        if remote_exec "$ip" "pgrep -x epaxos >/dev/null 2>&1"; then
            echo "  WARNING: ${label} still running on ${ip} after SIGKILL"
            return 1
        fi
        echo "  Confirmed ${label} stopped on ${ip}"
    else
        echo "  Note: ${label} was not running on ${ip}"
        return 2
    fi
}

stop_plain_cluster() {
    # Must match BASE_ENV's NUM_CLIENTS/CONFIG_PATH: stop_cluster_hetero.sh
    # defaults CLIENT_COUNT to 2 and CONFIG_PATH to the 2-client
    # cluster_hetero_5n_2s_3w.conf, so without this it silently
    # stopped/collected the wrong (or too few) clients, leaving processes
    # running on hosts indefinitely.
    CLIENT_COUNT=5 CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"

    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$touch_marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            local newest
            newest=$(ls -t "$merged_dir"/*.csv 2>/dev/null | head -1)
            [ -n "$newest" ] && cp "$newest" "$dest_dir/"
        fi
    fi

    local timeline_src="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_src" ]; then
        find "$timeline_src" -name "tps_timeline_*.csv" -newer "$touch_marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    # Roll the marker forward so the next case's archive step doesn't
    # re-collect this case's merged/timeline files too (they're all
    # "newer" than a marker touched once at script start otherwise).
    touch "$touch_marker"

    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_plain_cluster || true
    fi
}
trap cleanup EXIT

run_case() {
    case "$1" in
        replica2) run_crash_case_sampled "case_replica2" "follower:2" ;;
        replica3) run_crash_case_sampled "case_replica3" "follower:3" ;;
        replica4) run_crash_case_sampled "case_replica4" "follower:4" ;;
        leader)   run_crash_case_sampled "case_leader" "leader" ;;
        *) echo "Usage: $0 [replica2|replica3|replica4|leader|all] [batchsize] [indep_ratio]"; exit 1 ;;
    esac
}

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║  HETERO-5 CRASH EVAL (EPAXOS)                                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Target: ${TARGET}  |  Batch: ${BATCHSIZE_OVERRIDE}  |  Indep ratio: ${INDEP_RATIO_OVERRIDE}"
echo "Result archive: $RUN_DIR"

if [ "$TARGET" = "all" ]; then
    run_case replica2
    run_case replica3
    run_case replica4
else
    run_case "$TARGET"
fi

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 crash eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
