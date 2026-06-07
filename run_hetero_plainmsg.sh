#!/bin/bash
# ================================================================
# HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos
# Crash-only sampled runner for the heterogeneous cluster.
# Uses the 5-server, 2-client heterogeneous cluster config.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# shellcheck source=sampler_replacement.sh
source "${SCRIPT_DIR}/sampler_replacement.sh"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
CONFIG_PATH="${SCRIPT_DIR}/config/cluster_hetero_5n_2s3w.conf"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/epaxos"
RESULT_ROOT="${SCRIPT_DIR}/results/hetero_plainmsg"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false
ACTIVE_CLUSTER_KIND="plain"

# ----------------------------------------------------------------
# EPaxos-specific BASE_ENV
# Key differences from WoC:
#   THRESHOLD=2       (EPaxos quorum setting for a 5-server setup)
#   CONFLICT_RATE     (EPaxos conflict rate, not indep/common ratio)
#   THRIFTY           (EPaxos thrifty mode flag)
#   No ENABLE_PRIORITY, RATIO_STEP, BATCH_MODE, BATCH_COMPOSITION
# ----------------------------------------------------------------
BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    "THRESHOLD=2"
    "OPS=0"
    "EVAL_TYPE=0"
    "BATCHSIZE=1"
    "MSG_SIZE=512"
    "MODE=1"
    "CONFLICT_RATE=0"
    "INDEP_RATIO=90.0"
    "COMMON_RATIO=10.0"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "LOG_LEVEL=info"
    "THRIFTY=false"
)

SERVER_IPS=(
    "192.168.73.159"
    "192.168.73.84"
    "192.168.73.69"
    "192.168.73.235"
    "192.168.73.194"
)

CLIENT_IPS=(
    "192.168.73.218"
    "192.168.73.219"
)

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
FAULT_PRE_DELAY_SECONDS="${FAULT_PRE_DELAY_SECONDS:-5}"
EVAL_ONLY="${1:-all}"

if ! [[ "$RUNTIME_SECONDS" =~ ^[0-9]+$ ]] || [ "$RUNTIME_SECONDS" -lt 1 ]; then
    echo "WARNING: RUNTIME_SECONDS=${RUNTIME_SECONDS} is invalid. Using 30."
    RUNTIME_SECONDS=30
fi

if ! [[ "$FAULT_PRE_DELAY_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "WARNING: FAULT_PRE_DELAY_SECONDS=${FAULT_PRE_DELAY_SECONDS} is invalid. Using 5."
    FAULT_PRE_DELAY_SECONDS=5
fi

if [ "$FAULT_PRE_DELAY_SECONDS" -ge "$RUNTIME_SECONDS" ]; then
    FAULT_PRE_DELAY_SECONDS=$(( RUNTIME_SECONDS > 1 ? RUNTIME_SECONDS - 1 : 0 ))
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

    local timeline_dir="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_dir" ]; then
        find "$timeline_dir" -path '*/tps_timeline_*.csv' "${find_args[@]}" \
            -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    touch "$marker"
    echo "  Archived results to: $dest_dir"
    ls -1 "$dest_dir"/*.csv 2>/dev/null | sed 's|.*/|    |' || echo "  (no CSVs found)"
}

start_plain_cluster() {
    echo "Starting EPaxos heterogeneous cluster..."
    ACTIVE_CLUSTER_KIND="plain"
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_plain_cluster() {
    bash "$STOP_SCRIPT"
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

run_case() {
    local label=$1
    local runtime=$2

    echo ""
    echo "=================================================="
    echo "Running: $label"
    echo "=================================================="

    CLUSTER_ACTIVE=true
    ACTIVE_CLUSTER_KIND="plain"
    start_plain_cluster
    sleep "$runtime"
    stop_plain_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"
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
Usage: bash run_hetero_plainmsg.sh [selector]

Selectors (default: all):
  eval1          Independent vs Common ratio sweep
  eval2          Max inflight sweep
  eval_batching  Batch size sweep
  eval_msgsize   Message size sweep
  eval_crash_follower  Crash one follower only (sampled)

Environment overrides:
  RUNTIME_SECONDS=30          wall-clock seconds per run
  CRASH_TRIGGER_SECONDS=10    stable time before crash injection
  FAULT_PRE_DELAY_SECONDS=5   stable time before fault injection

Results archived under: results/hetero_plainmsg/<timestamp>/
EOF
    exit 0
fi

[[ "${EVAL_ONLY}" == --* ]] && EVAL_ONLY="${EVAL_ONLY#--}"

case "${EVAL_ONLY}" in
    all|eval1|eval2|eval_batching|eval_msgsize|eval_crash_follower) ;;
    *)
        echo "ERROR: unknown selector '${EVAL_ONLY}'. Run with --help."
        exit 1 ;;
esac

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║      HETEROGENEOUS PLAIN-MSG EVALUATION RUNNER — EPaxos       ║"
echo "║               5-Server Cluster + 2 Clients                    ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "Result archive: $RUN_DIR"
echo ""

# ================================================================
# EVAL 1: independent vs common ratio sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval1" ]]; then
    echo "── EVAL 1: Ratio sweep ─────────────────────────────────────────"
    echo ""
    for case in "100.0/0.0" "90.0/10.0" "80.0/20.0" "60.0/40.0" \
                "40.0/60.0" "20.0/80.0" "10.0/90.0" "0.0/100.0"; do
        indep="${case%/*}"
        common="${case#*/}"
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=2" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "CONFLICT_RATE=0" "INDEP_RATIO=${indep}" "COMMON_RATIO=${common}"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "LOG_LEVEL=info" "THRIFTY=false"
        )
        run_case "eval1_ratio_${indep}_common_${common}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL 2: max inflight sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval2" ]]; then
    echo "── EVAL 2: Max inflight sweep ──────────────────────────────────"
    for value in 1 2 3 4 5 8 10 15 20 25 30 35 40; do
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=2" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
            "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=${value}"
            "LOG_LEVEL=info" "THRIFTY=false"
        )
        run_case "eval2_max_inflight_${value}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL batching: batch size sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_batching" ]]; then
    echo "── EVAL batching: batch size sweep ─────────────────────────────"
    for batch_size in 1 10 50 100 500 1000 2000; do
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=2" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=${batch_size}" "MSG_SIZE=512" "MODE=1"
            "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "LOG_LEVEL=info" "THRIFTY=false"
        )
        run_case "eval_batching_${batch_size}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL msgsize: message size sweep
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_msgsize" ]]; then
    echo "── EVAL msgsize: message size sweep ────────────────────────────"
    for msg_size in 64 512 1024 2048 4096; do
        BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=2" "OPS=0"
            "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=${msg_size}" "MODE=1"
            "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
            "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
            "LOG_LEVEL=info" "THRIFTY=false"
        )
        run_case "eval_msgsize_${msg_size}" "$RUNTIME_SECONDS"
    done
fi

# ================================================================
# EVAL crash follower: crash one follower only (sampled)
# ================================================================
if [[ "$EVAL_ONLY" == "all" || "$EVAL_ONLY" == "eval_crash_follower" ]]; then
    TIMESERIES_ENABLED=true
    _SAVED_RUNTIME=$RUNTIME_SECONDS
    RUNTIME_SECONDS=60
    _SAVED_CRASH_TRIGGER=${CRASH_TRIGGER_SECONDS:-10}
    CRASH_TRIGGER_SECONDS=15

    BASE_ENV=(
            "NUM_SERVERS=5" "NUM_CLIENTS=2" "THRESHOLD=2" "OPS=0"
        "EVAL_TYPE=0" "BATCHSIZE=1" "MSG_SIZE=512" "MODE=1"
        "CONFLICT_RATE=0" "INDEP_RATIO=90.0" "COMMON_RATIO=10.0"
        "PIPELINE_MODE=true" "MAX_INFLIGHT=5"
        "ENABLE_TIMESERIES=true" "LOG_LEVEL=info"
    )

    run_crash_case_sampled "eval_crash_follower1" "follower:1"

    RUNTIME_SECONDS=${_SAVED_RUNTIME}
    CRASH_TRIGGER_SECONDS=${_SAVED_CRASH_TRIGGER}
    TIMESERIES_ENABLED=false
fi

echo ""
echo "=================================================="
echo " All evaluations complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
