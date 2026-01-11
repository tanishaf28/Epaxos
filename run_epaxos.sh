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
CONFLICT_RATE=100              # Hot object conflict rate (0-100%)
INDEP_RATIO=0.0             # % independent objects (fast path)
COMMON_RATIO=0.0             # % common objects (conflicts)
BATCH_COMPOSITION="object-specific" # "mixed" or "object-specific" or "single_obj"
PIPELINE_MODE="true"         # "true"=pipeline, "false"=sequential
MAX_INFLIGHT=5               # Max concurrent batches per client (adaptive: starts at 2, max 5)
CONFIG_PATH="./config/cluster_localhost.conf"
LOG_DIR="./logs"
BINARY="./epaxos"
LOG_LEVEL="info"             # "debug" for verbose logs

# EPaxos quorum calculations
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
    
    "$BINARY" \
        -id=${i} \
        -n=${NUM_SERVERS} \
        -f=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -pd=true \
        -role=0 \
        -ops=${OPS} \
        -b=${BATCHSIZE} \
        -indep=${INDEP_RATIO} \
        -common=${COMMON_RATIO} \
        -conflictrate=${CONFLICT_RATE} \
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
    PIPELINE_MODE="${PIPELINE_MODE}" MAX_INFLIGHT="${MAX_INFLIGHT}" "$BINARY" \
        -id=${client_id} \
        -n=${NUM_SERVERS} \
        -f=${THRESHOLD} \
        -path="${CONFIG_PATH}" \
        -ops=${OPS} \
        -et=${EVAL_TYPE} \
        -pd=true \
        -role=1 \
        -b=${BATCHSIZE} \
        -indep=${INDEP_RATIO} \
        -common=${COMMON_RATIO} \
        -conflictrate=${CONFLICT_RATE} \
        -bcomp="${BATCH_COMPOSITION}" \
        -ms=${MSG_SIZE} \
        -mode=${MODE} \
        -log="${LOG_LEVEL}" \
        > "${LOG_DIR}/client${client_id}/output.log" 2>&1 &
    pid=$!
    CLIENT_PIDS+=($pid)
    echo "  → PID: ${pid} | Log: ${LOG_DIR}/client${client_id}/output.log"
done

echo ""
echo "=============================================="
echo "EPaxos Cluster Started Successfully"
echo "=============================================="
echo "Topology:"
echo "  Replicas:    ${NUM_SERVERS} (all equal, leaderless)"
echo "  Clients:     ${NUM_CLIENTS} (IDs: ${NUM_SERVERS}-$((NUM_SERVERS+NUM_CLIENTS-1)))"
echo ""
echo "EPaxos Configuration:"
echo "  N (replicas): ${NUM_SERVERS}"
echo "  F (failures): ${THRESHOLD}"
echo "  Fast quorum:  ${FAST_QUORUM} replicas (F + ⌊(F+1)/2⌋)"
echo "  Slow quorum:  ${SLOW_QUORUM} replicas (F + 1 = majority)"
if [ ${OPS} -le 0 ]; then
    echo "  Operations:   infinite (run until stopped)"
else
    echo "  Operations:   ${OPS} per client"
fi
echo "  Batch size:   ${BATCHSIZE} operations per RPC"
echo "  Batch comp:   ${BATCH_COMPOSITION}"
if [ "${PIPELINE_MODE}" = "true" ]; then
    echo "  Pipeline:     ENABLED (adaptive: starts at 2, max ${MAX_INFLIGHT} concurrent)"
else
    echo "  Pipeline:     DISABLED (sequential mode)"
fi
echo ""
echo "Object Distribution:"
echo "  Independent: ${INDEP_RATIO}% (non-interfering, fast path)"
echo "  Common:      ${COMMON_RATIO}% (may interfere)"
echo "  Hot/Conflict:${CONFLICT_RATE}% (high interference, slow path)"
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
    echo "   - Hot/Independent/Common distributed per ratios above"
    echo "   - Dependencies calculated per-operation"
elif [ "${BATCH_COMPOSITION}" == "single_obj" ]; then
    echo "  SINGLE-OBJ: Each batch targets ONE object (all ops same ObjID)"
    echo "   - All ${BATCHSIZE} ops in a batch write to same object"
    echo "   - Object type chosen per batch (Hot/Indep/Common)"
    echo "   - Simplified dependency tracking"
else
    echo "  OBJECT-SPECIFIC: Each batch contains ONE object type"
    echo "   - All ${BATCHSIZE} ops in batch are same type (Hot/Indep/Common)"
    echo "   - Each op can target different object within that type"
    echo "   - Type-specific dependency calculation"
fi
echo ""
echo "Adaptive Concurrency Control:"
echo "  Initial limit: 2 concurrent batches per client"
echo "  Max limit:     ${MAX_INFLIGHT} concurrent batches per client"
echo "  Auto-adjusts based on fast path ratio and latency"
echo "  ⬆ Increase: Fast path >85%, latency <50ms"
echo "  ⬇ Decrease: Fast path <40%, latency >100ms"
echo ""
echo "System:"
echo "  Eval Type:   ${EVAL_TYPE} (0=plain msg, 1=mongodb)"
echo "  Mode:        ${MODE} (0=localhost, 1=distributed)"
echo "  Log Level:   ${LOG_LEVEL}"
echo "=============================================="
echo ""
echo "Monitor logs with:"
echo "  # Replica logs:"
echo "  tail -f ${LOG_DIR}/server0/output.log"
echo ""
echo "  # Client logs (watch fast/slow path):"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log"
echo ""
echo "  # Watch fast path ratio:"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log | grep -E '(FAST|SLOW|fast=)'"
echo ""
echo "  # Watch adaptive limit changes:"
echo "  tail -f ${LOG_DIR}/client${NUM_SERVERS}/output.log | grep -E '(⬆|⬇|Limit=)'"
echo ""
echo "View all errors:"
echo "  grep -i error ${LOG_DIR}/*/output.log"
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
    
    sleep 3
    
    # Force kill any remaining servers
    for pid in "${SERVER_PIDS[@]}"; do
        kill -9 $pid 2>/dev/null || true
    done
    
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
echo "Stopping servers..."
for pid in "${SERVER_PIDS[@]}"; do
    kill -TERM $pid 2>/dev/null || true
done

sleep 3

echo "Cleaning up any remaining processes..."
for pid in "${SERVER_PIDS[@]}"; do
    kill -9 $pid 2>/dev/null || true
done
pkill -f "$BINARY" > /dev/null 2>&1 || true

echo ""
echo "=============================================="
echo "All processes stopped. Done."
echo "=============================================="
