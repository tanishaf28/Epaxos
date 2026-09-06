#!/bin/bash
# ================================================================
# MongoDB Cluster Launcher - HETEROGENEOUS 5-NODE (2 Strong + 3 Weak)
# ================================================================

set -e
trap 'echo " Script interrupted. Exiting..."; exit 1' INT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# Parse workload argument
WORKLOAD="${1:-a}"
if [[ ! "$WORKLOAD" =~ ^[a-f]$ ]]; then
    echo "ERROR: workload must be one of: a b c d e f"
    exit 1
fi

# -----------------------------
# USER / SSH CONFIG
# -----------------------------
USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# -----------------------------
# REMOTE DIRECTORY SETUP
# -----------------------------
REMOTE_DIR="${REMOTE_DIR:-/home/ubuntu/epaxos}"
BINARY="${BINARY:-epaxos}"
CONFIG_PATH="${REMOTE_DIR}/config/cluster_hetero_mongo_5n_b.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"

# MongoDB-specific parameters
MONGODB_PORT=27017

# EPaxos PARAMETERS
NUM_SERVERS=5                          # fixed: SERVER_IPS below has exactly 5 real hosts
NUM_CLIENTS="${NUM_CLIENTS:-5}"
THRESHOLD="${THRESHOLD:-2}"
OPS="${OPS:-0}"
EVAL_TYPE=0
BATCHSIZE="${BATCHSIZE:-10}"
MSG_SIZE="${MSG_SIZE:-512}"
MODE="${MODE:-1}"
INDEP_RATIO="${INDEP_RATIO:-90.0}"
NUM_OBJECTS="${NUM_OBJECTS:-1000}"    # size of the fixed, hash-ring-mapped object pool
READ_RATIO="${READ_RATIO:-0.0}"
BATCH_COMPOSITION="${BATCH_COMPOSITION:-object-specific}"
PIPELINE_MODE="${PIPELINE_MODE:-true}"
MAX_INFLIGHT="${MAX_INFLIGHT:-5}"
THRIFTY="${THRIFTY:-false}"
# Off by default (matches every other sweep script except the crash eval) -
# callers that want per-client TPS timeline CSVs (e.g. run_mongodb_delay_
# eval_5n.sh, run_mongodb_workload_sweep_5n.sh) set ENABLE_TIMESERIES=true
# before calling this script. Was previously not forwarded to the client at
# all, so ENABLE_TIMESERIES=true silently produced no tps_timeline_*.csv
# files no matter what the caller set locally.
ENABLE_TIMESERIES="${ENABLE_TIMESERIES:-false}"
TPS_TIMELINE_INTERVAL_MS="${TPS_TIMELINE_INTERVAL_MS:-500}"
LOG_LEVEL="${LOG_LEVEL:-info}"
USE_ADAPTIVE_LIMITER="false"
PARALLEL_FAST_PATH="true"
ENABLE_PRIORITY="true"
LATENCY_DEBUG="false"
SERVER_BATCHING="false"

# 5-Node Cluster: 2 Strong (c16) + 3 Weak (c4)
SERVER_IPS=(
"192.168.73.222"   # cora-c32-tani-2 (strong) -- shared with woc/cabinet/raft
"192.168.73.250"  # cora-c32-tani-1 (strong) -- shared with woc/cabinet/raft
"192.168.73.5"
"192.168.73.237"
"192.168.73.85"
)

CLIENT_HOST_IPS=(
"192.168.73.65"
"192.168.73.173"
"192.168.73.71"
"192.168.73.42"
"192.168.73.106"
)

CLIENTS_PER_VM=1

# Build EPaxos binary locally
echo "=============================================="
echo "Building EPaxos binary locally..."
echo "=============================================="
(cd "$REPO_ROOT" && go build -o "${SCRIPT_DIR}/${BINARY}")
echo "Build complete."

# Copy binary to all VMs
copy_binary() {
    local TARGET_IP=$1
    echo "  Copying binary to $TARGET_IP ..."
    scp -i $SSH_KEY "$BINARY" $USER@$TARGET_IP:$REMOTE_DIR/
}

copy_config() {
    local TARGET_IP=$1
    echo "  Copying config to $TARGET_IP ..."
    scp -i $SSH_KEY "$CONFIG_PATH" $USER@$TARGET_IP:$REMOTE_DIR/config/
}

setup_mongodb() {
    local TARGET_IP=$1
    local NODE_ID=$2
    echo "  Setting up MongoDB on $TARGET_IP (Node $NODE_ID) ..."
    ssh -i $SSH_KEY $USER@$TARGET_IP bash -s <<EOF
set -e
MONGODB_PORT=27017
REMOTE_DIR="$REMOTE_DIR"

# Stop existing MongoDB if running.
pkill -x mongod 2>/dev/null || true
sleep 1

# Clear stale database state from previous runs, then recreate directories.
rm -rf $REMOTE_DIR/mongodb_data/* $REMOTE_DIR/mongodb_data/.[!.]* $REMOTE_DIR/mongodb_data/..?* 2>/dev/null || true
rm -f $REMOTE_DIR/mongodb_data/mongod.lock $REMOTE_DIR/mongodb_data/WiredTiger.lock $REMOTE_DIR/logs/mongod.log 2>/dev/null || true
mkdir -p $REMOTE_DIR/mongodb_data $REMOTE_DIR/logs

# Standalone mongod (no replica set) -- each server's MongoFollower
# connects to its own local instance via MONGODB_URI (defaults to
# localhost:27017), so there's no need for cross-node replication.
nohup mongod --port $MONGODB_PORT \
    --dbpath $REMOTE_DIR/mongodb_data \
    --bind_ip 0.0.0.0 \
    --logpath $REMOTE_DIR/logs/mongod.log \
    --logappend \
    > $REMOTE_DIR/logs/mongod.out 2>&1 &

sleep 2
echo "MongoDB started on $MONGODB_PORT"
EOF
}

sync_workload_data() {
    local TARGET_IP=$1
    echo "  Syncing YCSB workload files to $TARGET_IP ..."
    ssh -i "$SSH_KEY" "$USER@$TARGET_IP" "mkdir -p '$REMOTE_DIR/ycsb/workData'"
    scp -i "$SSH_KEY" "${REPO_ROOT}/ycsb/workData"/*.dat "$USER@$TARGET_IP:$REMOTE_DIR/ycsb/workData/"
}

wait_for_mongo_ready() {
    local TARGET_IP=$1
    local NODE_LABEL=$2
    local attempt

    for attempt in $(seq 1 30); do
        if ssh -i $SSH_KEY $USER@$TARGET_IP "mongosh --quiet --eval 'db.adminCommand({ ping: 1 })' >/dev/null 2>&1"; then
            return 0
        fi
        sleep 1
    done

    echo "  Warning: MongoDB readiness timed out on ${NODE_LABEL} (${TARGET_IP})"
    return 1
}

# Copy binaries and configs to all servers
echo ""
echo "=============================================="
echo "Distributing to SERVER nodes..."
echo "=============================================="
for ip in "${SERVER_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
    sync_workload_data "$ip"
done

# Copy binaries to client nodes
echo ""
echo "=============================================="
echo "Distributing to CLIENT nodes..."
echo "=============================================="
for ip in "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"
    sync_workload_data "$ip"
done

# Setup MongoDB on all servers
echo ""
echo "=============================================="
echo "Setting up MongoDB on all servers..."
echo "=============================================="
for i in "${!SERVER_IPS[@]}"; do
    setup_mongodb "${SERVER_IPS[$i]}" "$i"
done

for i in "${!SERVER_IPS[@]}"; do
    wait_for_mongo_ready "${SERVER_IPS[$i]}" "server${i}" || true
done

# Start EPaxos servers
echo ""
echo "=============================================="
echo "Starting EPaxos servers (heterogeneous 5-node)..."
echo "=============================================="

for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    log_file="$LOG_DIR/server_${i}.log"
    
    echo "  Starting server $i on $ip..."
    ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
set -e
cd $REMOTE_DIR
GOGC=50 PIPELINE_MODE=$PIPELINE_MODE MAX_INFLIGHT=$MAX_INFLIGHT \
nohup ./$BINARY \
    -id=$i \
    -path=$CONFIG_PATH \
    -et=1 \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -b=$BATCHSIZE \
    -ms=$MSG_SIZE \
    -mode=$MODE \
    -mcli=$NUM_CLIENTS \
    -mload=$WORKLOAD \
    -indep=$INDEP_RATIO \
    -numobjects=$NUM_OBJECTS \
    -readratio=$READ_RATIO \
    -bcomp=$BATCH_COMPOSITION \
    -thrifty=$THRIFTY \
    -log=$LOG_LEVEL \
    -role=0 \
    > $log_file 2>&1 &
echo \$! > /tmp/epaxos_${i}.pid
EOF
    sleep 0.5
done

# Wait for each server's RPC listener to actually accept connections before
# starting clients. Servers now do a real MongoDB load (recordcount=100000)
# in initMongoDB() *before* calling net.Listen, which can take several
# seconds -- a fixed short sleep here used to be enough back when the load
# step was a no-op (missing workload.dat), but now clients that connect too
# early get "connection refused" and fatal out.
echo ""
echo "=============================================="
echo "Waiting for servers to finish loading and start listening..."
echo "=============================================="
for i in "${!SERVER_IPS[@]}"; do
    ip="${SERVER_IPS[$i]}"
    port=$((10000 + i))
    echo "  Waiting for server $i ($ip:$port) ..."
    for attempt in $(seq 1 60); do
        if (exec 3<>"/dev/tcp/${ip}/${port}") 2>/dev/null; then
            exec 3>&- 3<&- 2>/dev/null || true
            echo "    server $i is listening."
            break
        fi
        sleep 1
        if [ "$attempt" -eq 60 ]; then
            echo "    WARNING: server $i ($ip:$port) did not start listening within 60s"
        fi
    done
done

# Start EPaxos clients
echo ""
echo "=============================================="
echo "Starting EPaxos clients on MongoDB workload '$WORKLOAD'..."
echo "=============================================="

for i in "${!CLIENT_HOST_IPS[@]}"; do
    ip="${CLIENT_HOST_IPS[$i]}"
    log_file="$LOG_DIR/client_${i}.log"
    
    echo "  Starting client $i on $ip..."
    for j in $(seq 0 $((CLIENTS_PER_VM - 1))); do
        client_id=$((NUM_SERVERS + i * CLIENTS_PER_VM + j))
        pin_server=$(( (i * CLIENTS_PER_VM + j) % NUM_SERVERS ))
        ssh -i "$SSH_KEY" "$USER@$ip" bash -s <<EOF &
set -e
cd $REMOTE_DIR
GOGC=100 PIPELINE_MODE=$PIPELINE_MODE MAX_INFLIGHT=$MAX_INFLIGHT \
ENABLE_TIMESERIES=$ENABLE_TIMESERIES TPS_TIMELINE_INTERVAL_MS=$TPS_TIMELINE_INTERVAL_MS \
nohup ./$BINARY \
    -id=$client_id \
    -path=$CONFIG_PATH \
    -et=1 \
    -n=$NUM_SERVERS \
    -t=$THRESHOLD \
    -ops=$OPS \
    -b=$BATCHSIZE \
    -ms=$MSG_SIZE \
    -mode=$MODE \
    -role=1 \
    -mload=$WORKLOAD \
    -indep=$INDEP_RATIO \
    -numobjects=$NUM_OBJECTS \
    -readratio=$READ_RATIO \
    -bcomp=$BATCH_COMPOSITION \
    -log=$LOG_LEVEL \
    -pinserver=$pin_server \
    > $log_file 2>&1 &
EOF
        sleep 0.5
    done
done

echo ""
echo "=============================================="
echo "HETEROGENEOUS 5-NODE MONGODB CLUSTER STARTED"
echo "=============================================="
echo ""
echo "Cluster Configuration:"
echo "  - 2 Strong nodes (c32): 192.168.73.222, 192.168.73.250"
echo "  - 3 Weak nodes (c8):   192.168.73.5, 192.168.73.237, 192.168.73.85"
echo "  - Workload: $WORKLOAD"
echo "  - MongoDB: standalone mongod per node (port $MONGODB_PORT)"
echo ""
echo "Monitor logs with: tail -f $LOG_DIR/*.log"
echo "Stop cluster with: bash ./stop_mongodb_hetero_5n.sh"
