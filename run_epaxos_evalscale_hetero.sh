#!/bin/bash
# ================================================================
# EPaxos HETEROGENEOUS CLUSTER SCALE SWEEP
# Runs increasing server counts with 2 fixed clients.
# MAX_INFLIGHT is fixed at 5 across all cases.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/epaxos"
BINARY="epaxos"
RESULT_ROOT="${SCRIPT_DIR}/results/cluster_scale_hetero"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"

NUM_CLIENTS=2
WORKLOAD="${WORKLOAD:-a}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-30}"
SETTLE_SECONDS="${SETTLE_SECONDS:-15}"
POST_STOP_SECONDS="${POST_STOP_SECONDS:-5}"

SERVER_COUNTS=(3 5 7 11 15)

declare -A CONFIG_BY_COUNT=(
    [3]="config/cluster_hetero_3n_2s_1w.conf"
    [5]="config/cluster_hetero_5n_2s3w.conf"
    [7]="config/cluster_hetero_7n_3s_4w.conf"
    [11]="config/cluster_hetero_11n_4s_7w.conf"
    [15]="config/cluster_hetero_15n_6s_9w.conf"
)

BASE_INDEP_RATIO="${INDEP_RATIO:-90.0}"
BASE_COMMON_RATIO="${COMMON_RATIO:-10.0}"
BASE_CONFLICT_RATE="${CONFLICT_RATE:-0}"
BASE_BATCHSIZE="${BATCHSIZE:-1}"
BASE_MSG_SIZE="${MSG_SIZE:-512}"
BASE_MODE="${MODE:-1}"
BASE_EVAL_TYPE="${EVAL_TYPE:-0}"
BASE_PIPELINE_MODE="${PIPELINE_MODE:-true}"
BASE_THRIFTY="${THRIFTY:-false}"
BASE_LOG_LEVEL="${LOG_LEVEL:-info}"

mkdir -p "$RUN_DIR" "$LOCAL_EVAL_DIR" "$MERGED_DIR"

if ! [[ "$RUNTIME_SECONDS" =~ ^[0-9]+$ ]] || [ "$RUNTIME_SECONDS" -lt 1 ]; then
    echo "WARNING: RUNTIME_SECONDS=${RUNTIME_SECONDS} is invalid. Using 45."
    RUNTIME_SECONDS=45
fi

if ! [[ "$SETTLE_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "WARNING: SETTLE_SECONDS=${SETTLE_SECONDS} is invalid. Using 15."
    SETTLE_SECONDS=15
fi

if ! [[ "$POST_STOP_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "WARNING: POST_STOP_SECONDS=${POST_STOP_SECONDS} is invalid. Using 5."
    POST_STOP_SECONDS=5
fi

remote_exec() {
    local host=$1
    shift
    ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -i "$SSH_KEY" "$USER@$host" "$*"
}

read_node_pool() {
    local config_local=$1
    mapfile -t NODE_POOL_IPS < <(awk 'NF >= 2 {print $2}' "$config_local")
}

copy_binary_and_config() {
    local host=$1
    local config_local=$2
    echo "  Copying binary/config to ${host}"
    local remote_tmp="${REMOTE_DIR}/.${BINARY}.tmp"
    scp -i "$SSH_KEY" "$BINARY" "$USER@$host:${remote_tmp}"
    ssh -i "$SSH_KEY" "$USER@$host" "mv -f '${remote_tmp}' '${REMOTE_DIR}/${BINARY}' && chmod 755 '${REMOTE_DIR}/${BINARY}'"
    ssh -i "$SSH_KEY" "$USER@$host" "mkdir -p ${REMOTE_DIR}/config"
    scp -i "$SSH_KEY" "$config_local" "$USER@$host:${REMOTE_DIR}/config/"
}

cleanup_remote_eval() {
    local host=$1
    remote_exec "$host" "rm -rf ${REMOTE_DIR}/eval/server* ${REMOTE_DIR}/eval/client* ${REMOTE_DIR}/eval/merged && mkdir -p ${REMOTE_DIR}/eval"
}

start_server() {
    local server_id=$1
    local host=$2
    local threshold=$3
    local max_inflight=$4
    local config_remote=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_DIR/logs/server${server_id}' '$REMOTE_DIR/eval/server${server_id}'
GOGC=50 PIPELINE_MODE=${BASE_PIPELINE_MODE} MAX_INFLIGHT=${max_inflight} \
nohup ./$BINARY \
    -id=${server_id} \
    -n=${NUM_SERVERS} \
    -t=${threshold} \
    -suffix=s${server_id}_n${NUM_SERVERS}_f${threshold}_b=${BASE_BATCHSIZE} \
    -path='$config_remote' \
    -pd=true \
    -role=0 \
    -b=${BASE_BATCHSIZE} \
    -thrifty=${BASE_THRIFTY} \
    -ct=0 \
    -cm=0 \
    -indep=${BASE_INDEP_RATIO} \
    -common=${BASE_COMMON_RATIO} \
    -conflictrate=${BASE_CONFLICT_RATE} \
    -bcomp=object-specific \
    -et=${BASE_EVAL_TYPE} \
    -ms=${BASE_MSG_SIZE} \
    -mode=${BASE_MODE} \
    -log=${BASE_LOG_LEVEL} \
    > '$REMOTE_DIR/logs/server${server_id}/output.log' 2>&1 &
echo \$! > '$REMOTE_DIR/logs/server${server_id}/pid.txt'
EOF
}

start_client() {
    local client_id=$1
    local host=$2
    local threshold=$3
    local max_inflight=$4
    local config_remote=$5

    remote_exec "$host" "bash -s" <<EOF
set -e
cd '$REMOTE_DIR'
mkdir -p '$REMOTE_DIR/logs/client${client_id}' '$REMOTE_DIR/eval/client${client_id}'
GOGC=100 PIPELINE_MODE=${BASE_PIPELINE_MODE} MAX_INFLIGHT=${max_inflight} \
nohup ./$BINARY \
    -id=${client_id} \
    -n=${NUM_SERVERS} \
    -t=${threshold} \
    -suffix=client${client_id}_epaxos \
    -path='$config_remote' \
    -ops=0 \
    -et=${BASE_EVAL_TYPE} \
    -pd=true \
    -role=1 \
    -b=${BASE_BATCHSIZE} \
    -indep=${BASE_INDEP_RATIO} \
    -common=${BASE_COMMON_RATIO} \
    -conflictrate=${BASE_CONFLICT_RATE} \
    -bcomp=object-specific \
    -ms=${BASE_MSG_SIZE} \
    -mode=${BASE_MODE} \
    -log=${BASE_LOG_LEVEL} \
    > '$REMOTE_DIR/logs/client${client_id}/output.log' 2>&1 &
echo \$! > '$REMOTE_DIR/logs/client${client_id}/pid.txt'
EOF
}

stop_node() {
    local host=$1
    remote_exec "$host" "pkill -TERM -x ${BINARY} 2>/dev/null || true"
    sleep 1
    remote_exec "$host" "pkill -KILL -x ${BINARY} 2>/dev/null || true" || true
}

collect_eval_dir() {
    local host=$1
    local subdir=$2
    local remote_dir="${USER}@${host}:${REMOTE_DIR}/eval/${subdir}"
    rm -rf "${LOCAL_EVAL_DIR}/${subdir}"
    if scp -q -i "$SSH_KEY" -o ConnectTimeout=10 -o StrictHostKeyChecking=no -r "$remote_dir" "$LOCAL_EVAL_DIR/"; then
        return 0
    fi
    echo "  WARNING: failed to collect ${subdir} from ${host}"
    return 1
}

archive_merged_csvs() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    if compgen -G "${MERGED_DIR}/*.csv" >/dev/null; then
        cp "${MERGED_DIR}"/*.csv "$dest_dir/"
        echo "  Archived merged CSVs to ${dest_dir}"
        ls -1 "$dest_dir"/*.csv | sed 's|.*/|    |'
    else
        echo "  WARNING: no merged CSVs found for ${label}"
    fi
}

run_case() {
    local server_count=$1

    local config_rel="${CONFIG_BY_COUNT[$server_count]:-}"
    if [ -z "$config_rel" ]; then
        echo "Skipping ${server_count}-node run: no config mapping is defined."
        return 0
    fi

    local config_local="${SCRIPT_DIR}/${config_rel}"
    local config_remote="${REMOTE_DIR}/config/$(basename "$config_local")"

    read_node_pool "$config_local"
    if [ "${#NODE_POOL_IPS[@]}" -lt "$((server_count + NUM_CLIENTS))" ]; then
        echo "Skipping ${server_count}-node run: ${config_rel} does not contain enough IPs."
        return 0
    fi

    NUM_SERVERS="$server_count"
    THRESHOLD="$(((NUM_SERVERS - 1) / 2))"
    MAX_INFLIGHT=5
    SERVER_IPS=("${NODE_POOL_IPS[@]:0:${NUM_SERVERS}}")
    CLIENT_IPS=("${NODE_POOL_IPS[@]:${NUM_SERVERS}:${NUM_CLIENTS}}")

    echo ""
    echo "=================================================="
    echo "Heterogeneous scale case: N=${NUM_SERVERS}, F=${THRESHOLD}"
    echo "Servers: ${SERVER_IPS[*]}"
    echo "Clients: ${CLIENT_IPS[*]}"
    echo "=================================================="

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        cleanup_remote_eval "$host"
    done

    for host in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
        copy_binary_and_config "$host" "$config_local"
    done

    for ((i=${#SERVER_IPS[@]}-1; i>=0; i--)); do
        echo "  Starting server ${i} on ${SERVER_IPS[$i]}"
        start_server "$i" "${SERVER_IPS[$i]}" "$THRESHOLD" "$MAX_INFLIGHT" "$config_remote"
        sleep 1
    done

    echo "Waiting ${SETTLE_SECONDS}s for server stabilization..."
    sleep "$SETTLE_SECONDS"

    for i in "${!CLIENT_IPS[@]}"; do
        client_id=$((NUM_SERVERS + i))
        echo "  Starting client ${client_id} on ${CLIENT_IPS[$i]}"
        start_client "$client_id" "${CLIENT_IPS[$i]}" "$THRESHOLD" "$MAX_INFLIGHT" "$config_remote"
        sleep 1
    done

    echo "Running for ${RUNTIME_SECONDS}s..."
    sleep "$RUNTIME_SECONDS"

    echo "Stopping clients..."
    for host in "${CLIENT_IPS[@]}"; do
        stop_node "$host"
    done
    sleep "$POST_STOP_SECONDS"

    echo "Stopping servers..."
    for host in "${SERVER_IPS[@]}"; do
        stop_node "$host"
    done

    rm -rf "$LOCAL_EVAL_DIR"/server* "$LOCAL_EVAL_DIR"/client* "$MERGED_DIR"
    mkdir -p "$LOCAL_EVAL_DIR" "$MERGED_DIR"

    for i in "${!CLIENT_IPS[@]}"; do
        client_id=$((NUM_SERVERS + i))
        collect_eval_dir "${CLIENT_IPS[$i]}" "client${client_id}" || true
    done
    for i in "${!SERVER_IPS[@]}"; do
        collect_eval_dir "${SERVER_IPS[$i]}" "server${i}" || true
    done

    if [ -f "$MERGE_SCRIPT" ]; then
        python3 "$MERGE_SCRIPT" || true
    fi

    archive_merged_csvs "n${NUM_SERVERS}"
}

cleanup() {
    for host in "${CLIENT_IPS[@]-}" "${SERVER_IPS[@]-}"; do
        [ -n "${host:-}" ] || continue
        stop_node "$host" || true
    done
}
trap cleanup EXIT

if [[ "${1:-}" == "--help" ]]; then
    cat <<'EOF'
Usage: bash run_epaxos_evalscale_hetero.sh

Environment overrides:
    WORKLOAD=a|b|c|d|e|f
    RUNTIME_SECONDS=30
  SETTLE_SECONDS=15
  POST_STOP_SECONDS=5

Runs heterogeneous cluster sizes: 3, 5, 7, 11, 15
with 2 fixed clients and MAX_INFLIGHT fixed at 5.
Uses EPaxos hetero configs mapped per server count.
EOF
    exit 0
fi

if ! command -v go >/dev/null 2>&1; then
    echo "ERROR: go is required to build the EPaxos binary"
    exit 1
fi

echo "Building EPaxos binary..."
go build -o "$BINARY"

echo "Result root: ${RUN_DIR}"
echo "Workload: $WORKLOAD"

for server_count in "${SERVER_COUNTS[@]}"; do
    run_case "$server_count"
done

echo ""
echo "All heterogeneous scale cases complete."
