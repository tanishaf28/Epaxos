#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Launcher - HETEROGENEOUS CLUSTER
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

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
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_new.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# EPAXOS PARAMETERS
# -----------------------------
NUM_SERVERS="${NUM_SERVERS:-5}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"
THRESHOLD="${THRESHOLD:-2}"
OPS=0
EVAL_TYPE=0
BATCHSIZE="${BATCHSIZE:-1}"
MSG_SIZE=512
MODE=1
CONFLICT_RATE="${CONFLICT_RATE:-0}"
INDEP_RATIO="${INDEP_RATIO:-90.0}"
COMMON_RATIO="${COMMON_RATIO:-10.0}"
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-4}"
LOG_LEVEL="${LOG_LEVEL:-info}"
THRIFTY="${THRIFTY:-false}"
CRASH_TIME=0
CRASH_MODE=0

# Safety guard: this launcher is intended for no-crash runs.
if [ "${CRASH_MODE}" -ne 0 ] || [ "${CRASH_TIME}" -ne 0 ]; then
    echo "ERROR: start_cluster_hetero.sh is configured for no-crash runs, but CRASH_MODE=${CRASH_MODE} CRASH_TIME=${CRASH_TIME}."
    echo "Set both to 0 or use a dedicated crash-test launcher."
    exit 1
fi

# -----------------------------
# CLOUD IP LIST
# -----------------------------
SERVER_IPS=(
"192.168.73.59" "192.168.73.243" "192.168.73.192" "192.168.73.134" "192.168.73.132"
)

# CLIENT VMs for heterogeneous cluster
CLIENT_HOST_IPS=(
"192.168.73.218" "192.168.73.219"
)

CLIENTS_PER_VM=1  # One client per VM

# -----------------------------
# BUILD EPAXOS BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building EPaxos binary locally..."
echo "=============================================="
go build -o "$BINARY"
echo " Build complete."

# -----------------------------
# COPY BINARY TO ALL VMs
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
echo "Copying binary to all servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
done
# -----------------------------
# CLEAN REMOTE EVAL DIRECTORIES
# -----------------------------
echo "=============================================="
echo "Cleaning stale remote eval directories..."
echo "=============================================="
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
            -suffix=s${SERVER_ID}_n${NUM_SERVERS}_f${THRESHOLD}_b${BATCHSIZE} \
            -path=${CONFIG_PATH} \
            -pd=true \
            -role=0 \
            -b=${BATCHSIZE} \
            -thrifty=${THRIFTY} \
            -ct=0 \
            -cm=0 \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -conflictrate=${CONFLICT_RATE} \
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
        echo " WARNING: ${label} failed to start (missing pid file: ${pid_file})"
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
            -suffix=client${CLIENT_ID}_epaxos \
            -path=${CONFIG_PATH} \
            -ops=${OPS} \
            -et=${EVAL_TYPE} \
            -pd=true \
            -role=1 \
            -b=${BATCHSIZE} \
            -indep=${INDEP_RATIO} \
            -common=${COMMON_RATIO} \
            -conflictrate=${CONFLICT_RATE} \
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
echo "Starting all servers (Heterogeneous Cluster)..."
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
# START CLIENTS
# -----------------------------
echo "=============================================="
echo "Starting ${NUM_CLIENTS} clients (${CLIENTS_PER_VM} per VM)..."
echo "=============================================="

client_id=${NUM_SERVERS}

for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    for ((c=0; c<CLIENTS_PER_VM; c++)); do
        if [ $client_id -lt $((NUM_SERVERS + NUM_CLIENTS)) ]; then
            start_client "$client_id" "$vm_ip"
            ((client_id++))
            sleep 1
        fi
    done
done

for i in "${!CLIENT_HOST_IPS[@]}"; do
    cid=$((NUM_SERVERS + i))
    if [ "$cid" -lt $((NUM_SERVERS + NUM_CLIENTS)) ]; then
        verify_remote_process "${CLIENT_HOST_IPS[$i]}" "${LOG_DIR}/client${cid}/pid.txt" "Client ${cid}" || true
    fi
done

echo "=============================================="
echo " EPaxos heterogeneous cluster launched successfully!"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Cluster Type: HETEROGENEOUS (5-node setup)"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Config File: cluster_hetero_new.conf"
echo "  Threshold: ${THRESHOLD} (F=${THRESHOLD})"
echo ""
echo "Monitor logs:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log'"
echo ""
echo "Stop all processes:"
echo "  ./stop_cluster_hetero.sh"
echo "=============================================="
