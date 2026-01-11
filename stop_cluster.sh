#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Stopper (Client-first, Safe Shutdown)
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

# -----------------------------
# Cluster Node Lists
# -----------------------------

SERVER_IPS=(
"192.168.228.176"
"192.168.228.57"
"192.168.228.200"
"192.168.228.113"
"192.168.228.54"
)

CLIENT_IPS=(
"192.168.228.207"
"192.168.228.150"
)

BINARY_NAME="epaxos"
LOG_DIR="/home/ubuntu/epaxos/logs"
EVAL_DIR="/home/ubuntu/epaxos/eval"

echo "=================================================="
echo " EPaxos Cluster Shutdown  (Clients → Servers)"
echo "=================================================="

# ---------------------------------------------------------------
# FUNCTION: Kill EPaxos on a node
# ---------------------------------------------------------------
kill_on_node() {
    local ip=$1
    local type=$2   # "Client" or "Server"

    echo ""
    echo "→ Stopping ${type} on ${ip}"

    # Step 1: Graceful shutdown (SIGTERM)
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        ${USER}@${ip} "pkill -TERM -f ${BINARY_NAME} 2>/dev/null" || true

    # Give enough time to flush metrics properly
    if [ "$type" = "Client" ]; then
        echo "  Waiting 30s for client graceful shutdown..."
        sleep 30
    else
        echo "  Waiting 15s for server to save metrics..."
        sleep 15
    fi

    # Step 2: Check if still running
    local count
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        ${USER}@${ip} "pgrep -f ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)

    # Step 3: Force kill if still running
    if [ "$count" -gt 0 ]; then
        echo "  Force killing ${count} process(es) on ${ip}"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            ${USER}@${ip} "pkill -9 -f ${BINARY_NAME} 2>/dev/null" || true
        sleep 1
    fi

    # Final check
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        ${USER}@${ip} "pgrep -f ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)

    if [ "$count" -eq 0 ]; then
        echo "   ${type} on ${ip} stopped"
    else
        echo "  WARNING: $count process(es) STILL running on ${ip}"
    fi
}

# ---------------------------------------------------------------
# STEP 1 — STOP CLIENTS (parallel)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping Clients (${#CLIENT_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${CLIENT_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done

wait

echo ""
echo "✓ All clients stopped"
echo "Waiting 10 seconds for servers to finish processing..."
sleep 10

# ---------------------------------------------------------------
# STEP 2 — STOP SERVERS
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2: Stopping Servers (${#SERVER_IPS[@]} nodes)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${SERVER_IPS[@]}"; do
    kill_on_node "$ip" "Server"
done

sleep 2

# ---------------------------------------------------------------
# FINAL VERIFICATION
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Verification (checking if ANY epaxos processes remain)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

any_left=false

for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        ${USER}@${ip} "pgrep -f ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
    if [ "$count" -gt 0 ]; then
        echo " ${ip}: ${count} process(es) STILL running"
        any_left=true
    fi
done

if [ "$any_left" = false ]; then
    echo " All EPaxos processes stopped cleanly on all nodes."
else
    echo " Some EPaxos processes remain — manual cleanup required."
fi

echo ""
echo "=================================================="
echo " EPaxos Cloud Cluster Shutdown Complete"
echo "=================================================="
echo "Logs preserved at:   $LOG_DIR/"
echo "Metrics preserved at: $EVAL_DIR/"
echo ""
echo "To collect metrics:"
echo "  scp -i ~/.ssh/tani.pem -r ubuntu@${SERVER_IPS[0]}:${EVAL_DIR} ./eval/"
echo "=================================================="
