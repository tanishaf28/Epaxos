#!/bin/bash
# ================================================================
# HETERO-5 CRASH EVAL: 2 cases on the fixed 5-server heterogeneous cluster
#   case1: kill server id 2 (follower:2)
#   case2: kill server id 4 (follower:4)
# Run sequentially in one invocation. Uses sampler_replacement.sh's
# run_crash_case_sampled (per-client in-process TPS timeline + event
# injection) -- this script is now its only caller.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# shellcheck source=sampler_replacement.sh
source "${REPO_ROOT}/sampler_replacement.sh"

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

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
CRASH_TRIGGER_SECONDS="${CRASH_TRIGGER_SECONDS:-10}"

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    "THRESHOLD=2"
    "BATCHSIZE=1"
    "INDEP_RATIO=90.0"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "ENABLE_TIMESERIES=true"
    "LOG_LEVEL=info"
)

SERVER_IPS=(
    "192.168.73.59"
    "192.168.73.243"
    "192.168.73.192"
    "192.168.73.134"
    "192.168.73.132"
)

CLIENT_IPS=(
    "192.168.73.167"
    "192.168.73.137"
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
    bash "$STOP_SCRIPT"
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

    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_plain_cluster || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║          HETERO-5 CRASH EVAL: case1 (server2), case2 (server4) ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"

run_crash_case_sampled "case1_replica2" "follower:2"
run_crash_case_sampled "case2_replica4" "follower:4"

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 crash eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
