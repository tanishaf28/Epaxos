#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Launcher - HETEROGENEOUS CLUSTER
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
CONFIG_PATH="${CONFIG_PATH:-${REMOTE_DIR}/config/cluster_hetero_5n_2s_3w.conf}"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# -----------------------------
# EPAXOS PARAMETERS
# -----------------------------
NUM_SERVERS="${NUM_SERVERS:-5}"
NUM_CLIENTS="${NUM_CLIENTS:-2}"
THRESHOLD="${THRESHOLD:-$(( (NUM_SERVERS - 1) / 2 ))}"
OPS=0
EVAL_TYPE=0
BATCHSIZE="${BATCHSIZE:-1}"
MSG_SIZE=512
MODE=1
INDEP_RATIO="${INDEP_RATIO:-90.0}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"
READ_RATIO="${READ_RATIO:-0.0}"
BATCH_COMPOSITION="object-specific"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
LOG_LEVEL="${LOG_LEVEL:-info}"
THRIFTY="${THRIFTY:-false}"
CRASH_TIME=0
CRASH_MODE=0
# Server-side batch accumulation (0/1 = disabled, today's per-RPC-instance
# behavior). See batching.go's submitForBatching/processEpaxosBatch.
BATCHWINDOWUS="${BATCHWINDOWUS:-0}"
MAXBATCH="${MAXBATCH:-1}"

# Safety guard: this launcher is intended for no-crash runs.
if [ "${CRASH_MODE}" -ne 0 ] || [ "${CRASH_TIME}" -ne 0 ]; then
    echo "ERROR: start_cluster_hetero.sh is configured for no-crash runs, but CRASH_MODE=${CRASH_MODE} CRASH_TIME=${CRASH_TIME}."
    echo "Set both to 0 or use a dedicated crash-test launcher."
    exit 1
fi

read_node_pool() {
    mapfile -t NODE_POOL_IPS < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH")
    SERVER_IPS=("${NODE_POOL_IPS[@]:0:${NUM_SERVERS}}")
    local client_pool=("${NODE_POOL_IPS[@]:${NUM_SERVERS}}")
    if [ "${#client_pool[@]}" -eq 0 ]; then
        CLIENT_HOST_IPS=()
        return
    fi
    # Cycle through the client portion of the pool (rather than a plain
    # slice) so NUM_CLIENTS > available client IPs (e.g. "matched"
    # servers=clients mode at N=11 against a 10-client-VM pool) packs extra
    # client processes onto already-used VMs instead of coming up short.
    CLIENT_HOST_IPS=()
    for ((k = 0; k < NUM_CLIENTS; k++)); do
        CLIENT_HOST_IPS+=("${client_pool[$((k % ${#client_pool[@]}))]}")
    done
}

read_node_pool

if [ "${#SERVER_IPS[@]}" -ne "$NUM_SERVERS" ]; then
    echo "ERROR: CONFIG_PATH=${CONFIG_PATH} does not contain ${NUM_SERVERS} servers."
    exit 1
fi

CLIENTS_PER_VM=1  # One client per VM

# -----------------------------
# BUILD EPAXOS BINARY LOCALLY
# -----------------------------
echo "=============================================="
echo "Building EPaxos binary locally..."
echo "=============================================="
(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
echo " Build complete."

# -----------------------------
# COPY BINARY TO ALL VMs
# -----------------------------
copy_binary() {
    local TARGET_IP=$1
    echo " Copying binary to $TARGET_IP ..."
    local remote_tmp="${REMOTE_DIR}/.${BINARY}.tmp"
    ssh -i $SSH_KEY $USER@$TARGET_IP "mkdir -p '${REMOTE_DIR}'"
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
    ( copy_binary "$ip" && copy_config "$ip" ) &
done
wait
# -----------------------------
# CLEAN REMOTE EVAL DIRECTORIES
# -----------------------------
# Only remove the specific server<id>/client<id> directories THIS run is
# about to populate — not a blanket `server*`/`client*` glob. The VM pool is
# shared across configs that map the same physical IP to different ids
# (e.g. .218 is client11 in an 11-server run but client21/22 in the 55-node
# client-scaling sweep), so a blanket wipe here can delete another run's
# output before its own stop_cluster_hetero.sh has collected it.
echo "=============================================="
echo "Cleaning stale remote eval directories for this run's ids..."
echo "=============================================="
for i in "${!SERVER_IPS[@]}"; do
    ssh -i $SSH_KEY $USER@${SERVER_IPS[$i]} "rm -rf '${EVAL_DIR}/server${i}' && mkdir -p '${EVAL_DIR}'" || true
done
for j in "${!CLIENT_HOST_IPS[@]}"; do
    client_id=$((NUM_SERVERS + j))
    ssh -i $SSH_KEY $USER@${CLIENT_HOST_IPS[$j]} "rm -rf '${EVAL_DIR}/client${client_id}' && mkdir -p '${EVAL_DIR}'" || true
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
            -batchwindowus=${BATCHWINDOWUS} \
            -maxbatch=${MAXBATCH} \
            -thrifty=${THRIFTY} \
            -ct=0 \
            -cm=0 \
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
    local TARGET_SERVER=$3

    echo " Starting EPaxos Client $CLIENT_ID on $CLIENT_IP (pinserver=${TARGET_SERVER})..."

    ssh -i $SSH_KEY $USER@$CLIENT_IP "
        cd $REMOTE_DIR
        mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}/client${CLIENT_ID}
        PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} \
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
            -pinserver=${TARGET_SERVER} \
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
client_idx=0

for vm_ip in "${CLIENT_HOST_IPS[@]}"; do
    for ((c=0; c<CLIENTS_PER_VM; c++)); do
        if [ $client_id -lt $((NUM_SERVERS + NUM_CLIENTS)) ]; then
            start_client "$client_id" "$vm_ip" "$((client_idx % NUM_SERVERS))"
            client_id=$((client_id + 1))
            client_idx=$((client_idx + 1))
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
echo "  Cluster Type: HETEROGENEOUS (${NUM_SERVERS} servers + ${NUM_CLIENTS} clients)"
echo "  Servers: ${NUM_SERVERS} (IDs 0-$((NUM_SERVERS-1)))"
echo "  Clients: ${NUM_CLIENTS} (IDs ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo "  Config File: $(basename "$CONFIG_PATH")"
echo "  Threshold: ${THRESHOLD} (F=${THRESHOLD})"
echo ""
echo "Monitor logs:"
echo "  ssh -i $SSH_KEY ubuntu@${SERVER_IPS[0]} 'tail -f ${LOG_DIR}/server0/output.log'"
echo "  ssh -i $SSH_KEY ubuntu@${CLIENT_HOST_IPS[0]} 'tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log'"
echo ""
echo "Stop all processes:"
echo "  ./stop_cluster_hetero.sh"
echo "=============================================="
