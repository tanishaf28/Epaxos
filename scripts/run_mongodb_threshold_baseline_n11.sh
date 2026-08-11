#!/bin/bash
# ================================================================
# MongoDB Failure-Threshold BASELINE, n=11 (EPaxos)
#
# EPaxos has NO tunable per-run failure-threshold knob: parameters.go's
# -t flag ("threshold") is only consumed by consensus.go's recovery-path
# quorum checks (prepareReplies <= threshold, fastRecoveryQuorum =
# numOfServers - threshold), and is always derived from n via the fixed
# formula floor((n-1)/2) - see start_mongodb_hetero_nsel.sh's
# THRESHOLD="${THRESHOLD:-$(( (NUM_SERVERS - 1) / 2 ))}" default. There is
# no equivalent of WOC's independently-sweepable weighted threshold.
#
# So unlike WOC's run_mongodb_threshold_sweep_n11.sh (which sweeps
# t=1..5), this script runs EPaxos ONCE at n=11, THRESHOLD unset so the
# start script's own floor((11-1)/2)=5 default applies, with INDEP_RATIO=
# 90, BATCHSIZE=1, MSG_SIZE=512 fixed (same fixed params as WOC's sweep).
# This is EPaxos's single fixed comparison point, plotted alongside
# WOC/Cabinet's swept-t=1..5 points at the same n=11.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_mongodb_hetero_nsel.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_mongodb_hetero_nsel.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
RESULT_ROOT="${SCRIPT_DIR}/results/mongodb_threshold_baseline_n11"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
CLUSTER_ACTIVE=false
BINARY_NAME="${BINARY_NAME:-epaxos}"
NUM_SERVERS=11

RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
WORKLOAD="${WORKLOAD:-a}"
INDEP_RATIO_FIXED="${INDEP_RATIO_FIXED:-90.0}"

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_mongodb_threshold_baseline_n11.sh

Runs EPaxos ONCE at n=11, at EPaxos's natural fixed majority (t=5,
THRESHOLD unset so the start script's floor((n-1)/2) default applies),
with INDEP_RATIO=90, BATCHSIZE=1, MSG_SIZE=512 fixed, against the EPaxos
MongoDB-backed cluster. EPaxos has no tunable per-run threshold to sweep -
see header comment.

Environment overrides:
  RUNTIME_SECONDS=30           wall-clock seconds
  WORKLOAD=a                   YCSB workload letter (a-f)
  INDEP_RATIO_FIXED=90.0       fixed indep ratio

Results archived under: results/mongodb_threshold_baseline_n11/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"
touch "${RUN_DIR}/.run_start_marker"
touch "${RUN_DIR}/.last_archive_ts"

cleanup_all_nodes() {
    echo "=================================================="
    echo " Pre-run cleanup: purging stale epaxos/mongod processes..."
    echo "=================================================="
    mapfile -t all_ips < <(awk 'NF >= 2 { print $2 }' "${REPO_ROOT}/config/cluster_hetero_11n_4s_7w.conf")
    for ip in "${all_ips[@]}"; do
        ssh -o ConnectTimeout=5 -i "$SSH_KEY" "$USER@$ip" "pkill -9 -x ${BINARY_NAME} 2>/dev/null; pkill -9 -x mongod 2>/dev/null" &
    done
    wait
}

archive_latest_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}/merged"
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
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        NUM_SERVERS="$NUM_SERVERS" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║       EPAXOS MONGODB THRESHOLD BASELINE, n=11 (t=5, fixed)       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""

cleanup_all_nodes

echo ""
echo "=================================================="
echo " n=${NUM_SERVERS}, fixed natural majority (t=5), no sweep"
echo "=================================================="

CLUSTER_ACTIVE=true
NUM_SERVERS="$NUM_SERVERS" INDEP_RATIO="$INDEP_RATIO_FIXED" BATCHSIZE=1 MSG_SIZE=512 NUM_OBJECTS=1000 READ_RATIO=0.0 \
    bash "$START_SCRIPT" "$WORKLOAD"
sleep "$RUNTIME_SECONDS"
NUM_SERVERS="$NUM_SERVERS" bash "$STOP_SCRIPT"
CLUSTER_ACTIVE=false
archive_latest_result "n11_mongo_baseline"

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 11

echo ""
echo "=================================================="
echo " EPaxos MongoDB threshold baseline (n=11) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
echo "Summary CSV: $RUN_DIR/extracted_metrics.csv"
