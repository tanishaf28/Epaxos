#!/bin/bash
###############################################################
# EPaxos Local Cluster Parameters (For WOC Comparison)
###############################################################

NUM_SERVERS=5                # Number of servers (N = 2F + 1)
NUM_CLIENTS=2                # Number of clients
OPS=0                        # Operations per client (0 for infinite mode)
THRESHOLD=2                  # F (failures tolerated), N = 2F + 1
EVAL_TYPE=0                  # 0=plain msg, 1=mongodb
BATCHSIZE=1               # Batch size (operations per RPC)
MSG_SIZE=512                 # Message size for plain msg
MODE=0                       # 0=localhost, 1=distributed
INDEP_RATIO=100.0              # % independent objects; remainder is dependent (WOC-style binary split)
NUM_OBJECTS=1000            # Total object key-space size (split into indep/dependent pools by INDEP_RATIO)
READ_RATIO=0.0                # % of ops that are reads (vs writes)
BATCH_COMPOSITION="object-specific" # "mixed" or "object-specific" or "single_obj"
PIPELINE_MODE="true"         # "true"=pipeline, "false"=sequential
MAX_INFLIGHT=4               # Max concurrent batches per client
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="./logs"
BINARY="./epaxos"
LOG_LEVEL="info"             # "debug" for verbose logs

# EPaxos quorum calculations — published formula (Moraru/Andersen/Kaminsky
# SOSP'13 §4.3, confirmed by Tollman/Park/Ousterhout NSDI'21 §2.1):
# fast quorum = F + floor((F+1)/2) TOTAL (incl. leader), slow quorum = plain
# majority (F+1 total). They coincide only at N=3,5; fast > majority for N>=7.
FAST_QUORUM=$((THRESHOLD + (THRESHOLD + 1) / 2))
SLOW_QUORUM=$((THRESHOLD + 1))

# Track PIDs
declare -a SERVER_PIDS=()
declare -a CLIENT_PIDS=()

# Build the binary
echo "Building EPaxos binary..."
go build -o "$BINARY"
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi
echo "Build complete"

# Create log directories
mkdir -p "$LOG_DIR"
mkdir -p "./eval"

# Start servers (role=0)
echo ""
echo "============================================"
echo "Starting EPaxos Replicas (Leaderless)"
echo "============================================"
echo "EPaxos is egalitarian - all replicas are equal"
echo "Any replica can propose commands"
echo ""

for ((i=0; i<NUM_SERVERS; i++)); do
    mkdir -p "${LOG_DIR}/server${i}"
    
    echo "Starting Replica ${i}..."
    
    GOGC=50 "$BINARY" \
        -id=${i} \
        -n=${NUM_SERVERS} \
        -t=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -pd=true \
        -role=0 \
        -ops=${OPS} \
        -b=${BATCHSIZE} \
        -indep=${INDEP_RATIO} \
        -numobjects=${NUM_OBJECTS} \
        -readratio=${READ_RATIO} \
        -et=${EVAL_TYPE} \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -log="${LOG_LEVEL}" \
        -bcomp="${BATCH_COMPOSITION}" \
        > "${LOG_DIR}/server${i}/output.log" 2>&1 &
    pid=$!
    SERVER_PIDS+=($pid)
    echo "  → PID: ${pid} | Log: ${LOG_DIR}/server${i}/output.log"
    
    # Small delay between server starts
    sleep 1
done

# Wait for servers to establish connections
echo ""
echo "============================================"
echo "Waiting for replica connections..."
echo "============================================"
echo "EPaxos replicas establishing peer-to-peer connections..."
echo "This may take up to 15 seconds..."
sleep 15

# Start clients (role=1)
echo ""
echo "============================================"
echo "Starting Clients"
echo "============================================"
for ((i=0; i<NUM_CLIENTS; i++)); do
    client_id=$((NUM_SERVERS + i))  # Client IDs start after server IDs
    mkdir -p "${LOG_DIR}/client${client_id}"
    mkdir -p "./eval/client${client_id}"
    
    echo "Starting Client ${client_id}..."
    
    CLIENT_CMD="GOGC=100 PIPELINE_MODE=${PIPELINE_MODE} MAX_INFLIGHT=${MAX_INFLIGHT} $BINARY \
        -id=${client_id} \
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
        -log=${LOG_LEVEL}"

    eval "${CLIENT_CMD}" > "${LOG_DIR}/client${client_id}/output.log" 2>&1 &
    pid=$!
    CLIENT_PIDS+=($pid)
    echo "  → PID: ${pid} | Log: ${LOG_DIR}/client${client_id}/output.log"
done

echo ""
echo "=============================================="
echo "EPaxos Cluster Started Successfully"
echo "=============================================="
echo "Topology:"
echo "  Replicas:    ${NUM_SERVERS}(all equal, leaderless)"
echo "  Clients:     ${NUM_CLIENTS}(IDs: ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo ""
echo "EPaxos Configuration:"
echo "  N (replicas): ${NUM_SERVERS}"
echo "  F (failures): ${THRESHOLD}"
echo "  Fast quorum:  ${FAST_QUORUM} replicas total (F + floor((F+1)/2), incl. leader)"
echo "  Slow quorum:  ${SLOW_QUORUM} replicas total (F+1 = plain majority)"
if [ ${OPS} -le 0 ]; then
    echo "  Operations:   infinite (run until stopped)"
else
    echo "  Operations:   ${OPS} per client"
fi
echo "  Batch size:   ${BATCHSIZE} operations per RPC"
echo "  Batch comp:   ${BATCH_COMPOSITION}"
if [ "${PIPELINE_MODE}" = "true" ]; then
    echo "  Pipeline:     ENABLED (fixed max-in-flight=${MAX_INFLIGHT})"
else
    echo "  Pipeline:     DISABLED (sequential mode)"
fi
echo ""
echo "Object Distribution (WOC-style binary split):"
echo "  Independent: ${INDEP_RATIO}% (namespaced per client, never conflicts)"
echo "  Dependent:   $(awk -v r="${INDEP_RATIO}" 'BEGIN{printf "%.1f", 100-r}')% (shared pool, where conflicts happen)"
echo "  NumObjects:  ${NUM_OBJECTS} (key-space size)"
echo "  ReadRatio:   ${READ_RATIO}%"
echo ""
echo "EPaxos Fast Path Conditions:"
echo "  ✓ Quorum of ${FAST_QUORUM} PreAccept replies received"
echo "  ✓ All replies have identical seq/deps (unanimity)"
echo "  ✓ All dependencies are committed"
echo "  ✓ Initial ballot (ballot = 0)"
echo "  → If any condition fails → Slow path (Accept phase)"
echo ""
echo "Batch Composition Mode:"
if [ "${BATCH_COMPOSITION}" == "mixed" ]; then
    echo "  MIXED: Each batch contains diverse objects & types"
    echo "   - Each of the ${BATCHSIZE} ops can target different objects"
    echo "   - Independent/Dependent distributed per ratios above"
    echo "   - Dependencies calculated per-operation"
elif [ "${BATCH_COMPOSITION}" == "single_obj" ]; then
    echo "  SINGLE-OBJ: Each batch targets ONE object (all ops same ObjID)"
    echo "   - All ${BATCHSIZE} ops in a batch write to same object"
    echo "   - Object type chosen per batch (Independent/Dependent)"
    echo "   - Simplified dependency tracking"
else
    echo "  OBJECT-SPECIFIC: Each batch contains ONE object type"
    echo "   - All ${BATCHSIZE} ops in batch are same type (Independent/Dependent)"
    echo "   - Each op can target different object within that type"
    echo "   - Type-specific dependency calculation"
fi
echo ""
echo "Concurrency Control:"
if [ "${PIPELINE_MODE}" = "true" ]; then
    echo "  Mode:         FIXED max-in-flight (ChannelLimiter, never adjusts)"
    echo "  Max-inflight: ${MAX_INFLIGHT} concurrent batches per client"
else
    echo "  Mode:         SEQUENTIAL (no pipelining)"
fi
echo ""
echo "System:"
echo "  Eval Type:   ${EVAL_TYPE} (0=plain msg, 1=mongodb)"
echo "  Mode:        ${MODE} (0=localhost, 1=distributed)"
echo "  Log Level:   ${LOG_LEVEL}"
echo "=============================================="
echo ""
echo "Monitor logs with:"
echo "  # Real per-replica/per-client logs (output.log is just the stdout"
echo "  # redirect and stays mostly empty — actual logrus output goes here):"
echo "  tail -f ${LOG_DIR}/server0/server0.txt"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/client${NUM_SERVERS}.txt"
echo ""
echo "  # Watch fast/slow path decisions:"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/client${NUM_SERVERS}.txt | grep -E '(FastPath|SlowPath|FAST|SLOW)'"
echo ""
echo "View all errors:"
echo "  grep -i error ${LOG_DIR}/*/*.txt"
echo ""

# Cleanup function for infinite mode
cleanup_infinite() {
    echo ""
    echo "Interrupt received, stopping all processes..."
    echo "Waiting for graceful shutdown (up to 30s)..."
    
    # Send SIGTERM to clients (triggers graceful shutdown)
    for pid in "${CLIENT_PIDS[@]}"; do
        kill -TERM $pid 2>/dev/null || true
    done
    
    # Wait up to 30s for clients to finish
    timeout=30
    elapsed=0
    while [ $elapsed -lt $timeout ]; do
        alive=0
        for pid in "${CLIENT_PIDS[@]}"; do
            if kill -0 $pid 2>/dev/null; then
                alive=$((alive + 1))
            fi
        done
        
        if [ $alive -eq 0 ]; then
            echo "All clients stopped gracefully"
            break
        fi
        
        echo "Waiting for $alive clients to finish... (${elapsed}s)"
        sleep 2
        elapsed=$((elapsed + 2))
    done
    
    # Force kill any remaining clients
    for pid in "${CLIENT_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    
    # Send SIGTERM to servers (triggers metrics save)
    echo "Stopping servers and saving metrics..."
    for pid in "${SERVER_PIDS[@]}"; do
        kill -TERM $pid 2>/dev/null || true
    done
    
    # Give servers enough time to save metrics (activeRPCs wait + execution stop + metrics save)
    echo "Waiting for servers to save metrics (10s grace period)..."
    sleep 10
    
    # Check if servers exited gracefully
    alive=0
    for pid in "${SERVER_PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            alive=$((alive + 1))
        fi
    done
    
    if [ $alive -gt 0 ]; then
        echo "Warning: $alive server(s) still running, force killing..."
        for pid in "${SERVER_PIDS[@]}"; do
            kill -9 $pid 2>/dev/null || true
        done
    else
        echo "All servers shut down gracefully"
    fi
    
    echo "All processes stopped."
}    

# Wait for clients to finish (or run indefinitely)
if [ ${OPS} -le 0 ]; then
    echo "Clients running in infinite mode..."
    echo "Press Ctrl+C to trigger graceful shutdown"
    echo ""
    # Set trap for user interrupt
    trap cleanup_infinite SIGINT SIGTERM
    wait
else
    echo "Waiting for clients to complete..."
    for pid in "${CLIENT_PIDS[@]}"; do
        wait $pid 2>/dev/null || {
            echo "Client PID $pid finished with error (or was killed)"
        }
    done

    echo ""
    echo "=============================================="
    echo "All clients completed!"
    echo "=============================================="
    sleep 10 
    

fi

echo ""
echo "Check logs in:"
echo "  ${LOG_DIR}/server*/output.log"
echo "  ${LOG_DIR}/client*/output.log"
echo ""
echo "Check metrics in:"
echo "  ./eval/client*/client*_epaxos_*.csv"
echo ""
echo "Servers are still running..."
echo "=============================================="
echo ""

# Optionally keep servers running or kill them
read -p "Press Enter to stop servers and exit (or Ctrl+C to keep them running)..."

# Cleanup
echo ""
echo "Stopping servers and saving metrics..."
for pid in "${SERVER_PIDS[@]}"; do
    kill -TERM $pid 2>/dev/null || true
done

echo "Waiting for graceful shutdown (10s)..."
sleep 10

echo "Checking for remaining processes..."
alive=0
for pid in "${SERVER_PIDS[@]}"; do
    if kill -0 $pid 2>/dev/null; then
        alive=$((alive + 1))
    fi
done

if [ $alive -gt 0 ]; then
    echo "Warning: $alive server(s) still running, force killing..."
    for pid in "${SERVER_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
else
    echo "All servers shut down gracefully"
fi
pkill -x epaxos > /dev/null 2>&1 || true

echo ""
echo "=============================================="
echo "All processes stopped. Done."
echo "=============================================="
