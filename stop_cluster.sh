#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Stopper (Client-first, Safe Shutdown)
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
MERGED_OUTPUT="${MERGED_DIR}/merged_epaxos_clients_${RUN_TS}.csv"

# -----------------------------
# Cluster Node Lists
# -----------------------------

SERVER_IPS=(
"192.168.73.159"
"192.168.73.84"
"192.168.73.218"
"192.168.73.69"
"192.168.73.235"
)

CLIENT_IPS=(
"192.168.73.194"
"192.168.73.7"
)

SERVER_ID_FILTER="0-$((${#SERVER_IPS[@]} - 1))"
CLIENT_START_ID="${#SERVER_IPS[@]}"
CLIENT_END_ID="$((CLIENT_START_ID + ${#CLIENT_IPS[@]} - 1))"
CLIENT_ID_FILTER="${CLIENT_START_ID}-${CLIENT_END_ID}"

BINARY_NAME="epaxos"
LOG_DIR="/home/ubuntu/epaxos/logs"
EVAL_DIR="/home/ubuntu/epaxos/eval"

is_local_ip() {
    local ip="$1"
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$ip"
}

echo "=================================================="
echo " EPaxos Cluster Shutdown  (Clients → Servers)"
echo "=================================================="

# ---------------------------------------------------------------
# FUNCTION: Kill EPaxos on a node
# ---------------------------------------------------------------
kill_on_node() {
    local ip=$1
    local type=$2   # "Client" or "Server"
    local grace_seconds

    if [ "$type" = "Client" ]; then
        grace_seconds=45
    else
        grace_seconds=60
    fi

    echo ""
    echo "→ Stopping ${type} on ${ip}"

    # Step 1: Graceful shutdown (SIGTERM)
    # Use -x for exact match to avoid killing SSH sessions
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        ${USER}@${ip} "pkill -TERM -x ${BINARY_NAME} 2>/dev/null" || true

    # Wait for graceful stop with polling; server0 can need extra time to flush eval CSV.
    echo "  Waiting up to ${grace_seconds}s for graceful shutdown..."
    local count=0
    local elapsed=0
    while [ "$elapsed" -lt "$grace_seconds" ]; do
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            ${USER}@${ip} "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
        if [ "$count" -eq 0 ]; then
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    # Step 3: Force kill if still running
    if [ "$count" -gt 0 ]; then
        echo "  Force killing ${count} process(es) on ${ip}"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            ${USER}@${ip} "pkill -9 -x ${BINARY_NAME} 2>/dev/null" || true
        sleep 1
    fi

    # Final check
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        ${USER}@${ip} "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)

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
    kill_on_node "$ip" "Server" &
done
wait

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
        ${USER}@${ip} "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
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

# ---------------------------------------------------------------
# STEP 2.5 — CLEAN UP STALE EVAL DIRECTORIES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2.5: Cleaning up stale eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Remove server directories outside the current cluster size
for server_dir in "${LOCAL_EVAL_DIR}"/server*/; do
    [ -d "$server_dir" ] || continue
    server_name=$(basename "$server_dir")
    server_id="${server_name#server}"
    
    # Check if this server ID is in range [0, ${#SERVER_IPS[@]}-1]
    if [ "$server_id" -ge "${#SERVER_IPS[@]}" ]; then
        echo " Removing stale $server_name (current cluster has servers 0-$((${#SERVER_IPS[@]}-1)))"
        rm -rf "$server_dir"
    fi
done

# Remove client directories outside the current cluster size
for client_dir in "${LOCAL_EVAL_DIR}"/client*/; do
    [ -d "$client_dir" ] || continue
    client_name=$(basename "$client_dir")
    client_id="${client_name#client}"
    
    # Check if this client ID is in range [${#SERVER_IPS[@]}, ${#SERVER_IPS[@]}+${#CLIENT_IPS[@]}-1]
    if [ "$client_id" -lt "${#SERVER_IPS[@]}" ] || [ "$client_id" -ge "$((${#SERVER_IPS[@]} + ${#CLIENT_IPS[@]}))" ]; then
        echo " Removing stale $client_name (current cluster has clients ${#SERVER_IPS[@]}-$((${#SERVER_IPS[@]} + ${#CLIENT_IPS[@]} - 1)))"
        rm -rf "$client_dir"
    fi
done

# ---------------------------------------------------------------
# STEP 3 — COLLECT CLIENT EVAL DIRECTORIES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 3: Collecting client eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p "${LOCAL_EVAL_DIR}"
mkdir -p "${MERGED_DIR}"

for i in "${!CLIENT_IPS[@]}"; do
    client_id=$((${#SERVER_IPS[@]} + i))
    remote_eval="${USER}@${CLIENT_IPS[$i]}:${EVAL_DIR}/client${client_id}"

    if is_local_ip "${CLIENT_IPS[$i]}"; then
        echo " Detected local source for client${client_id} (${CLIENT_IPS[$i]}), skipping SCP refresh"
        mkdir -p "${LOCAL_EVAL_DIR}/client${client_id}"
        continue
    fi

    # Refresh local copy to avoid stale files from previous runs.
    rm -rf "${LOCAL_EVAL_DIR}/client${client_id}"

    echo " Collecting ${remote_eval} -> ${LOCAL_EVAL_DIR}/"
    if ! scp -i "${SSH_KEY}" -o ConnectTimeout=10 -o StrictHostKeyChecking=no -r \
        "${remote_eval}" "${LOCAL_EVAL_DIR}/"; then
        echo " WARNING: Failed to collect metrics from client ${client_id} (${CLIENT_IPS[$i]})"
    fi
done

echo ""
echo "Client CSV collection check:"
for i in "${!CLIENT_IPS[@]}"; do
    client_id=$((${#SERVER_IPS[@]} + i))
    if ls "${LOCAL_EVAL_DIR}/client${client_id}"/*.csv >/dev/null 2>&1; then
        echo " client${client_id}: CSV found"
    else
        echo " WARNING: client${client_id}: no CSV found"
    fi
done

# ---------------------------------------------------------------
# STEP 4 — COLLECT SERVER EVAL DIRECTORIES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 4: Collecting server eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for i in "${!SERVER_IPS[@]}"; do
    remote_eval="${USER}@${SERVER_IPS[$i]}:${EVAL_DIR}/server${i}"

    # If this script runs on the same VM as the source server, do not delete
    # local eval/serverN before SCP or we may erase the source directory itself.
    if is_local_ip "${SERVER_IPS[$i]}"; then
        echo " Detected local source for server${i} (${SERVER_IPS[$i]}), skipping SCP refresh"
        mkdir -p "${LOCAL_EVAL_DIR}/server${i}"
        continue
    fi

    # Refresh local copy to avoid stale files from previous runs.
    rm -rf "${LOCAL_EVAL_DIR}/server${i}"

    echo " Collecting ${remote_eval} -> ${LOCAL_EVAL_DIR}/"
    if ! scp -i "${SSH_KEY}" -o ConnectTimeout=10 -o StrictHostKeyChecking=no -r \
        "${remote_eval}" "${LOCAL_EVAL_DIR}/"; then
        echo " WARNING: Failed to collect metrics from server ${i} (${SERVER_IPS[$i]})"
    fi
done

echo ""
echo "Server CSV collection check:"
for i in "${!SERVER_IPS[@]}"; do
    if ls "${LOCAL_EVAL_DIR}/server${i}"/*.csv >/dev/null 2>&1; then
        echo " server${i}: CSV found"
    else
        echo " WARNING: server${i}: no CSV found"
    fi
done

# ---------------------------------------------------------------
# STEP 5 — MERGE CLIENT CSVS LOCALLY
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 5: Merging client CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "${MERGE_SCRIPT}" ]; then
    if command -v python3 >/dev/null 2>&1; then
        if python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_OUTPUT}" --ids "${CLIENT_ID_FILTER}"; then
            echo " Merged client CSV written to: ${MERGED_OUTPUT}"
        else
            echo " WARNING: merge_eval.py failed for clients; check logs above"
        fi
    else
        echo " WARNING: python3 not found; skipping merge step"
    fi
else
    echo " WARNING: ${MERGE_SCRIPT} not found; skipping merge step"
fi

# ---------------------------------------------------------------
# STEP 6 — MERGE SERVER CSVS LOCALLY
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 6: Merging server CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SERVER_MERGED_OUTPUT="${MERGED_DIR}/merged_servers_${RUN_TS}.csv"

if [ -f "${MERGE_SCRIPT}" ]; then
    if command -v python3 >/dev/null 2>&1; then
        if python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${SERVER_MERGED_OUTPUT}" --servers --ids "${SERVER_ID_FILTER}"; then
            echo " Merged server CSV written to: ${SERVER_MERGED_OUTPUT}"
        else
            echo " WARNING: merge_eval.py failed for servers; check logs above"
        fi
    else
        echo " WARNING: python3 not found; skipping server merge step"
    fi
else
    echo " WARNING: ${MERGE_SCRIPT} not found; skipping server merge step"
fi

echo ""
echo "=================================================="
echo " EPaxos Cloud Cluster Shutdown Complete"
echo "=================================================="
echo "Logs preserved at:       $LOG_DIR/"
echo "Remote metrics at:       $EVAL_DIR/"
echo "Local collected eval at: ${LOCAL_EVAL_DIR}/"
echo "Local merged client CSV: ${MERGED_OUTPUT}"
echo "Local merged server CSV: ${SERVER_MERGED_OUTPUT}"
echo ""
echo "Merge commands used:"
echo "  python3 merge_eval.py ./eval ./eval/merged/ --ids ${CLIENT_ID_FILTER}  # clients"
echo "  python3 merge_eval.py ./eval ./eval/merged/ --servers --ids ${SERVER_ID_FILTER}  # servers"
echo "=================================================="
