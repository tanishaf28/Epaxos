#!/bin/bash
# ================================================================
# Cloud Cluster Stopper - HETEROGENEOUS CLUSTER
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/epaxos"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"
CONFIG_PATH="${CONFIG_PATH:-${REMOTE_DIR}/config/cluster_hetero_5n_2s_3w.conf}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${REPO_ROOT}/merge_eval.py"
RUN_TS="$(date +%Y%m%d_%H%M%S)"

SERVER_COUNT="${SERVER_COUNT:-5}"
CLIENT_COUNT="${CLIENT_COUNT:-2}"

read_node_pool() {
    mapfile -t NODE_POOL_IPS < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH")
    SERVER_IPS=("${NODE_POOL_IPS[@]:0:${SERVER_COUNT}}")
    local client_pool=("${NODE_POOL_IPS[@]:${SERVER_COUNT}}")
    CLIENT_IPS=()
    if [ "${#client_pool[@]}" -gt 0 ]; then
        # Mirrors start_cluster_hetero.sh's read_node_pool: cycle through the
        # client portion of the pool so CLIENT_COUNT > available client IPs
        # (matched mode at n=11) maps each client_id back to the VM it was
        # actually launched on, instead of index-ing past the array.
        for ((k = 0; k < CLIENT_COUNT; k++)); do
            CLIENT_IPS+=("${client_pool[$((k % ${#client_pool[@]}))]}")
        done
    fi
}

read_node_pool

if [ "${#SERVER_IPS[@]}" -ne "$SERVER_COUNT" ]; then
    echo "ERROR: CONFIG_PATH=${CONFIG_PATH} does not contain ${SERVER_COUNT} servers."
    exit 1
fi

SERVER_ID_FILTER="0-$((${#SERVER_IPS[@]} - 1))"
CLIENT_START_ID="${#SERVER_IPS[@]}"
CLIENT_END_ID="$((CLIENT_START_ID + CLIENT_COUNT - 1))"
CLIENT_ID_FILTER="${CLIENT_START_ID}-${CLIENT_END_ID}"
BINARY_NAME="epaxos"

is_local_ip() {
    local ip="$1"
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$ip"
}

# ---------------------------------------------------------------
# FUNCTION: Copy eval directory from remote node to local eval/
# ---------------------------------------------------------------
copy_eval_dir() {
    local ip=$1
    local remote_subdir=$2
    local local_subdir=$3
    local local_target="${LOCAL_EVAL_DIR}/${local_subdir}"
    local remote_source="${REMOTE_EVAL_DIR}/${remote_subdir}"

    if is_local_ip "$ip"; then
        echo " Detected local source for ${local_subdir} (${ip}), using local copy path"
        rm -rf "${local_target}"
        if [ -d "${remote_source}" ]; then
            cp -a "${remote_source}" "${LOCAL_EVAL_DIR}/" 2>/dev/null || mkdir -p "${local_target}"
            # Source is now safely copied -- clear it so it doesn't keep
            # accumulating every historical case's CSVs forever.
            rm -f "${remote_source}"/*.csv 2>/dev/null || true
        else
            mkdir -p "${local_target}"
        fi
        return 0
    fi

    # Refresh local copy to avoid stale files from previous runs.
    rm -rf "${local_target}"

    # Copy full directory so structure stays consistent.
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "test -d ${remote_source}" >/dev/null 2>&1 || {
            echo " WARNING: Missing remote directory ${remote_source} on ${ip}"
            return 1
        }

    echo " Collecting ${USER}@${ip}:${remote_source} -> ${LOCAL_EVAL_DIR}/"
    scp -q -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i $SSH_KEY -r \
        "$USER@$ip:${remote_source}" "${LOCAL_EVAL_DIR}/" 2>/dev/null || {
            echo " WARNING: Failed to collect ${remote_subdir} from ${ip}"
            return 1
        }

    # Source is now safely copied -- clear it so it doesn't keep
    # accumulating every historical case's CSVs forever.
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "rm -f ${remote_source}/*.csv" >/dev/null 2>&1 || true

    return 0
}

echo "=================================================="
echo " HETEROGENEOUS Cluster Shutdown"
echo " Clients → Servers"
echo "=================================================="

# ---------------------------------------------------------------
# FUNCTION: Kill processes on a node
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

    # Send SIGTERM first (exact binary match avoids false positives).
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pkill -TERM -x ${BINARY_NAME} 2>/dev/null" || true

    echo "  Waiting up to ${grace_seconds}s for graceful shutdown..."
    local count=0
    local elapsed=0
    while [ "$elapsed" -lt "$grace_seconds" ]; do
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
        count=$(echo "$count" | tr -d ' \n')
        if [ "$count" -eq 0 ]; then
            break
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    # Check if still running
    if [ "$count" -gt 0 ]; then
        echo "  Still running -> Killing $count process(es) on $ip"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@$ip "pkill -9 -x ${BINARY_NAME} 2>/dev/null" || true
        sleep 1
    fi

    # Final check
    count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
    count=$(echo "$count" | tr -d ' \n')

    if [ "$count" -eq 0 ]; then
        echo "   ${type} on ${ip} stopped"
    else
        echo "  WARNING: $count process(es) still active on ${ip}"
    fi
}

# ---------------------------------------------------------------
# STEP 1 — STOP CLIENTS (IN PARALLEL)
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 1: Stopping Clients (${CLIENT_COUNT} total)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for ip in "${CLIENT_IPS[@]}"; do
    kill_on_node "$ip" "Client" &
done

# Wait for all parallel client shutdowns to complete
wait

echo ""
echo "Waiting 5 seconds for servers to flush metrics..."
sleep 5

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
echo " Verification (checking if ANY processes remain)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

any_left=false

verify_dir=$(mktemp -d)
for ip in "${SERVER_IPS[@]}" "${CLIENT_IPS[@]}"; do
    (
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@$ip "pgrep -x ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
        count=$(echo "$count" | tr -d ' \n')
        [ "$count" -gt 0 ] && echo "$count" > "${verify_dir}/${ip}"
    ) &
done
wait
for f in "${verify_dir}"/*; do
    [ -f "$f" ] || continue
    ip=$(basename "$f")
    echo " ${ip}: $(cat "$f") processes STILL running"
    any_left=true
done
rm -rf "$verify_dir"

if [ "$any_left" = false ]; then
    echo " All  processes stopped on all nodes."
else
    echo " Some processes remain. Use manual pkill if needed."
fi

# ---------------------------------------------------------------
# STEP 2.5 — CLEAN UP STALE EVAL DIRECTORIES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 2.5: Cleaning up stale eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"

for server_dir in "${LOCAL_EVAL_DIR}"/server*/; do
    [ -d "$server_dir" ] || continue
    server_name=$(basename "$server_dir")
    server_id="${server_name#server}"
    if [ "$server_id" -ge "${#SERVER_IPS[@]}" ]; then
        echo " Removing stale ${server_name}"
        rm -rf "$server_dir"
    fi
done

for client_dir in "${LOCAL_EVAL_DIR}"/client*/; do
    [ -d "$client_dir" ] || continue
    client_name=$(basename "$client_dir")
    client_id="${client_name#client}"
    if [ "$client_id" -lt "${#SERVER_IPS[@]}" ] || [ "$client_id" -ge "$((CLIENT_START_ID + CLIENT_COUNT))" ]; then
        echo " Removing stale ${client_name}"
        rm -rf "$client_dir"
    fi
done

# ---------------------------------------------------------------
# STEP 3 — COLLECT EVAL DIRECTORIES FROM REMOTE NODES
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 3: Collecting client eval directories"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for client_id in $(seq $CLIENT_START_ID $CLIENT_END_ID); do
    vm_index=$((client_id - CLIENT_START_ID))
    ( copy_eval_dir "${CLIENT_IPS[$vm_index]}" "client${client_id}" "client${client_id}" || true ) &
done
wait

echo ""
echo "Client CSV collection check:"
for client_id in $(seq $CLIENT_START_ID $CLIENT_END_ID); do
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
    ( copy_eval_dir "${SERVER_IPS[$i]}" "server${i}" "server${i}" || true ) &
done
wait

echo ""
echo "Server CSV collection check:"
for i in "${!SERVER_IPS[@]}"; do
    if ls "${LOCAL_EVAL_DIR}/server${i}"/*.csv >/dev/null 2>&1; then
        echo " server${i}: CSV found"
    else
        echo " WARNING: server${i}: no CSV found"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@${SERVER_IPS[$i]} "ls -1 ${REMOTE_EVAL_DIR}/server${i} 2>/dev/null | sed 's/^/   remote: /'" 2>/dev/null || true
    fi
done

# ---------------------------------------------------------------
# STEP 5 — MERGE CLIENT CSVs
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 5: Merging client CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_ID_FILTER}"
    if [ $? -eq 0 ]; then
        echo " ✓ Merged client CSV written to: ${MERGED_DIR}/merged_clients_*.csv"
    else
        echo " ✗ Error merging client CSVs"
    fi
else
    echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
fi

# ---------------------------------------------------------------
# STEP 6 — MERGE SERVER CSVs
# ---------------------------------------------------------------
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " STEP 6: Merging server CSVs locally"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --servers --ids "${SERVER_ID_FILTER}"
    if [ $? -eq 0 ]; then
        echo " ✓ Merged server CSV written to: ${MERGED_DIR}/merged_servers_*.csv"
    else
        echo " ✗ Error merging server CSVs"
    fi
else
    echo " ✗ merge_eval.py not found at ${MERGE_SCRIPT}"
fi

echo ""
echo "=================================================="
echo " HETEROGENEOUS CLUSTER SHUTDOWN COMPLETE"
echo "=================================================="
