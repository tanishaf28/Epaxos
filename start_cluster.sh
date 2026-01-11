#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Launcher
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
CONFIG_PATH="${REMOTE_DIR}/config/cluster_localhost.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# EPAXOS PARAMETERS
# -----------------------------
NUM_SERVERS=5
NUM_CLIENTS=2
THRESHOLD=2              # F (failures tolerated), N = 2F + 1
OPS=0                    # Infinite mode
EVAL_TYPE=0              # 0=plain msg, 1=mongodb
BATCHSIZE=10             # Batch size per RPC
MSG_SIZE=512
MODE=1                   # Distributed mode
CONFLICT_RATE=0          # Hot object conflict rate (0-100%)
INDEP_RATIO=95.0         # % independent objects (fast path)
COMMON_RATIO=5.0         # % common objects
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="true"
MAX_INFLIGHT=40          # Max concurrent batches (auto-adaptive)
LOG_LEVEL="info"

# -----------------------------
# CLOUD IP LIST
# -----------------------------
SERVER_IPS=(
"192.168.228.176" "192.168.228.57" "192.168.228.200" "192.168.228.113" "192.168.228.54"
)
CLIENT_IPS=(
"192.168.228.207" "192.168.228.150"
)

# -----------------------------
# BUILD WOC BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building WOC binary locally..."
echo "=============================================="
go build -o "$BINARY"
echo " Build complete."

# -----------------------------
# COPY BINARY TO ALL VMs
# -----------------------------
copy_binary() {
    local TARGET_IP=$1
    echo " Copying binary to $TARGET_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:$REMOTE_DIR/
}

echo "=============================================="
echo "Copying binary to all servers and clients..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    copy_binary $ip
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
        PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} \
        nohup ./$BINARY \
            -id=${SERVER_ID} \
            -n=${NUM_SERVERS} \
            -t=${THRESHOLD} \
            -path=${CONFIG_PATH} \
            -pd=true \
            -role=0 \
            -b=${BATCHSIZE} \
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
        PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} \
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
echo "Starting all servers..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    start_server $i "${SERVER_IPS[$i]}"
    sleep 1
done

echo "Waiting 15 seconds for cluster stabilization..."
sleep 15

# -----------------------------
# START CLIENTS
# -----------------------------
echo "=============================================="
echo "Starting all clients..."
echo "=============================================="

for i in "${!CLIENT_IPS[@]}"; do
    client_id=$((${#SERVER_IPS[@]} + i))
    start_client $client_id "${CLIENT_IPS[$i]}"
    sleep 1
done

echo "=============================================="
echo " EPaxos cluster launched successfully!"
echo "=============================================="
echo ""
echo "Monitor logs:"
echo "  tail -f /home/ubuntu/epaxos/logs/server0/output.log"
echo "  tail -f /home/ubuntu/epaxos/logs/client${#SERVER_IPS[@]}/output.log"
echo ""
echo "Stop cluster:"
echo "  ./stop_cluster.sh"
echo ""
echo "Collect metrics:"
echo "  scp -i ~/.ssh/tani.pem -r ubuntu@\${SERVER_IPS[0]}:/home/ubuntu/epaxos/eval/* ./eval/"
echo "=============================================="

