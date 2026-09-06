#!/bin/bash
# ================================================================
# Plain-msg Failure-Threshold BASELINE, n=11 (EPaxos)
#
# EPaxos has NO tunable per-run failure-threshold knob: parameters.go's
# -t flag ("threshold") is only consumed by consensus.go's recovery-path
# quorum checks (prepareReplies <= threshold, fastRecoveryQuorum =
# numOfServers - threshold), and is always derived from n via the fixed
# formula floor((n-1)/2) - see start_cluster_hetero.sh's own
# THRESHOLD="${THRESHOLD:-$(( (NUM_SERVERS - 1) / 2 ))}" default. There is
# no equivalent of WOC's independently-sweepable weighted threshold.
#
# Plain-msg counterpart of run_mongodb_threshold_baseline_n11.sh -- same
# reasoning, against the plain-msg cluster (start_cluster_hetero.sh,
# -et=0) instead of the MongoDB one. So unlike WOC's/Cabinet's
# run_threshold_sweep_n11*.sh (which sweep t=1..5), this script runs
# EPaxos ONCE at n=11, THRESHOLD unset so the start script's own
# floor((11-1)/2)=5 default applies, with INDEP_RATIO=90, BATCHSIZE=1,
# MSG_SIZE=512 fixed (same fixed params as WOC's sweep). This is EPaxos's
# single fixed comparison point, plotted alongside WOC/Cabinet's
# swept-t=1..5 points at the same n=11.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
CONFIG_PATH="${REPO_ROOT}/config/cluster_hetero_11n_10c.conf"

RESULT_ROOT="${SCRIPT_DIR}/results/threshold_baseline_n11"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

NUM_SERVERS=11
NUM_CLIENTS="${NUM_CLIENTS:-2}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
INDEP_RATIO_FIXED="${INDEP_RATIO_FIXED:-90.0}"
CLUSTER_ACTIVE=false

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_threshold_baseline_n11.sh

Runs EPaxos ONCE at n=11, at EPaxos's natural fixed majority (t=5,
THRESHOLD unset so the start script's floor((n-1)/2) default applies),
with INDEP_RATIO=90, BATCHSIZE=1, MSG_SIZE=512 fixed, against the EPaxos
plain-msg cluster. EPaxos has no tunable per-run threshold to sweep - see
header comment.

Environment overrides:
  RUNTIME_SECONDS=30           wall-clock seconds
  NUM_CLIENTS=10                 client count (10-VM pool)
  INDEP_RATIO_FIXED=90.0        fixed indep ratio

Results archived under: results/threshold_baseline_n11/<timestamp>/<label>/
EOF
    exit 0
fi

mkdir -p "$RUN_DIR"

archive_case() {
    local label=$1
    local marker=$2
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" -newer "$marker" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi
    echo "  Archived results to: $dest_dir"
}

cleanup() {
    if [ "$CLUSTER_ACTIVE" = true ]; then
        SERVER_COUNT="$NUM_SERVERS" CLIENT_COUNT="$NUM_CLIENTS" CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT" || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║     EPAXOS PLAIN-MSG THRESHOLD BASELINE, n=11 (t=5, fixed)       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"
echo ""
echo "n=${NUM_SERVERS}, fixed natural majority (t=5), no sweep"

label="n11_plain_baseline"
touch "${RUN_DIR}/.marker_${label}"
marker="${RUN_DIR}/.marker_${label}"

CLUSTER_ACTIVE=true
NUM_SERVERS="$NUM_SERVERS" NUM_CLIENTS="$NUM_CLIENTS" CONFIG_PATH="${CONFIG_PATH}" \
    BATCHSIZE=1 MSG_SIZE=512 INDEP_RATIO="$INDEP_RATIO_FIXED" NUM_OBJECTS=1000 READ_RATIO=0.0 \
    EVAL_TYPE=0 OPS=0 MODE=1 THRIFTY=false PIPELINE_MODE=true MAX_INFLIGHT=5 LOG_LEVEL=info \
    bash "$START_SCRIPT"

echo "  Running for ${RUNTIME_SECONDS}s..."
sleep "$RUNTIME_SECONDS"

SERVER_COUNT="$NUM_SERVERS" CLIENT_COUNT="$NUM_CLIENTS" CONFIG_PATH="${CONFIG_PATH}" bash "$STOP_SCRIPT"
CLUSTER_ACTIVE=false

archive_case "$label" "$marker"

echo ""
echo "Extracting throughput/latency summary..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 11

echo ""
echo "=================================================="
echo " EPaxos plain-msg threshold baseline (n=11) complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
