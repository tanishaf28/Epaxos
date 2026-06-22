#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Launcher - HOMOGENEOUS CLUSTER
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# -----------------------------
# USER / SSH CONFIG
# -----------------------------
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# -----------------------------
# REMOTE DIRECTORY SETUP
# -----------------------------
REMOTE_DIR="/home/ubuntu/epaxos"
BINARY="epaxos"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_homo.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# EPAXOS PARAMETERS (Enforced via Env Vars)
# -----------------------------
: "${NUM_SERVERS:?NUM_SERVERS must be set}"
: "${NUM_CLIENTS:?NUM_CLIENTS must be set}"
: "${THRESHOLD:?THRESHOLD must be set}"
: "${OPS:?OPS must be set}"
: "${EVAL_TYPE:?EVAL_TYPE must be set}"
: "${BATCHSIZE:?BATCHSIZE must be set}"
: "${MSG_SIZE:?MSG_SIZE must be set}"
: "${MODE:?MODE must be set}"
: "${INDEP_RATIO:?INDEP_RATIO must be set}"
: "${PIPELINE_MODE:?PIPELINE_MODE must be set}"
: "${MAX_INFLIGHT:?MAX_INFLIGHT must be set}"
: "${LOG_LEVEL:?LOG_LEVEL must be set}"
: "${THRIFTY:?THRIFTY must be set}"

NUM_OBJECTS="${NUM_OBJECTS:-100000}"
READ_RATIO="${READ_RATIO:-0.0}"
BATCH_COMPOSITION="${BATCH_COMPOSITION:-object-specific}"
CRASH_TIME="${CRASH_TIME:-0}"
CRASH_MODE="${CRASH_MODE:-0}"

# -----------------------------
# IPs AND CLUSTER SIZING
# -----------------------------
ALL_SERVER_IPS=(
    "192.168.73.220"   # 0
    "192.168.73.240"   # 1
    "192.168.73.108"   # 2
    "192.168.73.179"   # 3
    "192.168.73.154"   # 4
    "192.168.73.109"   # 5
    "192.168.73.203"   # 6
    "192.168.73.30"    # 7
    "192.168.73.19"    # 8
    "192.168.73.127"   # 9
    "192.168.73.75"    # 10
)

# Dynamically slice array based on requested size (3, 5, 7, 11)
SERVER_IPS=("${ALL_SERVER_IPS[@]:0:$NUM_SERVERS}")

CLIENT_HOST_IPS=(
    "192.168.73.45"
    "192.168.73.229"
)

# -----------------------------
# BUILD EPAXOS BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building EPaxos binary locally..."
echo "=============================================="
(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
echo " Build complete."

# -----------------------------
# COPY BINARY AND CONFIG
# -----------------------------
copy_binary() {
    local TARGET_IP=$1
    echo " Copying binary to $TARGET_IP ..."
    local remote_tmp="${REMOTE_DIR}/.${BINARY}.tmp"
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:"${remote_tmp}"
    ssh -i $SSH_KEY $USER@$TARGET_IP "mv -f '${remote_tmp}' '${REMOTE_DIR}/${BINARY}' && chmod 755 '${REMOTE_DIR}/${BINARY}'"
}

copy_config() {
    local TARGET_IP=$1
    echo " Copying config to $TARGET_IP ..."
    ssh -i $SSH_KEY $USER@$TARGET_IP "mkdir -p $REMOTE_DIR/config"
    scp -i $SSH_KEY "$CONFIG_PATH" $USER@$TARGET_IP:$REMOTE_DIR/config/
}

echo "=============================================="
echo "Copying binary to ${NUM_SERVERS} servers and client VMs..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
done

# Clean remote eval directories
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    ssh -i $SSH_KEY $USER@$ip "rm -rf ${EVAL_DIR}/server* ${EVAL_DIR}/client* ${EVAL_DIR}/merged && mkdir -p ${EVAL_DIR}" || true
done

# -----------------------------
# START SERVER FUNCTION
# -----------------------------
start_server() {
    local SERVER_ID=$1
    local SERVER_IP=$2

    echo " Starting EPaxos Server $SERVER_ID on $SERVER_IP ..."

    ssh -i $SSH_KEY $USER@$SERVER_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/server${SERVER_ID} ${EVAL_DIR}/server${SERVER_ID}
        GOGC=50 PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} \
        nohup ./$BINARY \
            -id=${SERVER_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -pd=true \
            -role=0 \
            -b=${BATCHSIZE} \
            -thrifty=${THRIFTY} \
            -ct=${CRASH_TIME} \
            -cm=${CRASH_MODE} \
            -indep=${INDEP_RATIO} \
            -numobjects=${NUM_OBJECTS} \
            -readratio=${READ_RATIO} \
            -bcomp=${BATCH_COMPOSITION} \
            -et=${EVAL_TYPE} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            > ${LOG_DIR}/server${SERVER_ID}/output.log 2>&1 &
        echo \$! > ${LOG_DIR}/server${SERVER_ID}/pid.txt
    "
}

verify_remote_process() {
    local node_ip=$1
    local pid_file=$2
    local label=$3
    local pid

    pid=$(ssh -i $SSH_KEY $USER@$node_ip "cat '${pid_file}' 2>/dev/null" | tr -d '[:space:]')
    if [ -z "$pid" ]; then
        echo " WARNING: ${label} failed to start (missing pid file)"
        return 1
    fi
    if ssh -i $SSH_KEY $USER@$node_ip "kill -0 ${pid} 2>/dev/null"; then
        return 0
    fi
    echo " WARNING: ${label} failed to stay alive (pid ${pid} not running)"
    return 1
}

# -----------------------------
# START CLIENT FUNCTION
# -----------------------------
start_client() {
    local CLIENT_ID=$1
    local CLIENT_IP=$2

    echo " Starting EPaxos Client $CLIENT_ID on $CLIENT_IP ..."

    ssh -i $SSH_KEY $USER@$CLIENT_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}/client${CLIENT_ID}
        GOGC=100 PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} \
        ENABLE_TIMESERIES=${ENABLE_TIMESERIES:-false} \
        TPS_TIMELINE_INTERVAL_MS=${TPS_TIMELINE_INTERVAL_MS:-500} \
        nohup ./$BINARY \
            -id=${CLIENT_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -ops=${OPS} \
            -et=${EVAL_TYPE} \
            -pd=true \
            -role=1 \
            -b=${BATCHSIZE} \
            -indep=${INDEP_RATIO} \
            -numobjects=${NUM_OBJECTS} \
            -readratio=${READ_RATIO} \
            -bcomp=${BATCH_COMPOSITION} \
            -ms=${MSG_SIZE} \
            -mode=${MODE} \
            -log=${LOG_LEVEL} \
            > ${LOG_DIR}/client${CLIENT_ID}/output.log 2>&1 &
        echo \$! > ${LOG_DIR}/client${CLIENT_ID}/pid.txt
    "
}

# -----------------------------
# START SERVERS
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_SERVERS} servers (t=${THRESHOLD})..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server "$i" "${SERVER_IPS[$i]}"
    sleep 1
done

for i in "${!SERVER_IPS[@]}"; do
    verify_remote_process "${SERVER_IPS[$i]}" "${LOG_DIR}/server${i}/pid.txt" "Server ${i}" || true
done

echo "Waiting 15 seconds for cluster stabilization..."
sleep 15

# -----------------------------
# START CLIENTS (Dynamic Round-Robin)
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients..."
echo "=============================================="

client_id=${NUM_SERVERS}
for (( i=0; i<NUM_CLIENTS; i++ )); do
    vm_ip="${CLIENT_HOST_IPS[$(( i % ${#CLIENT_HOST_IPS[@]} ))]}"
    start_client "$client_id" "$vm_ip"
    ((client_id++))
    sleep 1
done

for (( i=0; i<NUM_CLIENTS; i++ )); do
    cid=$((NUM_SERVERS + i))
    vm_ip="${CLIENT_HOST_IPS[$(( i % ${#CLIENT_HOST_IPS[@]} ))]}"
    verify_remote_process "$vm_ip" "${LOG_DIR}/client${cid}/pid.txt" "Client ${cid}" || true
done

echo "=============================================="
echo " EPaxos homogeneous cluster launched successfully!"
echo "  n=${NUM_SERVERS}  t=${THRESHOLD}  clients=${NUM_CLIENTS}"
echo "=============================================="