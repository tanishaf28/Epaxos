#!/bin/bash
# ================================================================
# EPaxos Cloud Cluster Stopper - HOMOGENEOUS CLUSTER
# ================================================================

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/epaxos"
REMOTE_EVAL_DIR="${REMOTE_DIR}/eval"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
LOCAL_EVAL_DIR="${SCRIPT_DIR}/eval"
MERGED_DIR="${LOCAL_EVAL_DIR}/merged"
MERGE_SCRIPT="${REPO_ROOT}/merge_eval.py"

# -----------------------------
# CLUSTER IPs
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

# Must match start_cluster_homo.sh's CLIENT_HOST_IPS pool exactly, so
# STEP 4 below reconnects to the same VMs each client was actually
# launched on (both cycle through this list via the same i % len formula).
ALL_CLIENT_IPS=(
    "192.168.73.45"  "192.168.73.229" "192.168.73.142" "192.168.73.112"
    "192.168.73.88"  "192.168.73.140" "192.168.73.191" "192.168.73.226"
    "192.168.73.126" "192.168.73.96"  "192.168.73.143" "192.168.73.145"
    "192.168.73.135" "192.168.73.12"  "192.168.73.180" "192.168.73.113"
    "192.168.73.129" "192.168.73.33"  "192.168.73.205" "192.168.73.55"
    "192.168.73.209" "192.168.73.207" "192.168.73.102" "192.168.73.210"
    "192.168.73.153" "192.168.73.168" "192.168.73.23"  "192.168.73.89"
)

: "${SERVER_COUNT:?SERVER_COUNT must be set (e.g. SERVER_COUNT=5 bash stop_cluster_homo.sh)}"
: "${CLIENT_COUNT:?CLIENT_COUNT must be set (e.g. CLIENT_COUNT=2 bash stop_cluster_homo.sh)}"

SERVER_IPS=("${ALL_SERVER_IPS[@]:0:$SERVER_COUNT}")
CLIENT_IPS=("${ALL_CLIENT_IPS[@]}")

SERVER_ID_FILTER="0-$((SERVER_COUNT - 1))"
CLIENT_START_ID="${SERVER_COUNT}"
CLIENT_END_ID="$((CLIENT_START_ID + CLIENT_COUNT - 1))"
CLIENT_ID_FILTER="${CLIENT_START_ID}-${CLIENT_END_ID}"
BINARY_NAME="epaxos"

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
is_local_ip() {
    local ip="$1"
    hostname -I 2>/dev/null | tr ' ' '\n' | grep -Fxq "$ip"
}

copy_eval_dir() {
    local ip=$1
    local remote_subdir=$2
    local local_subdir=$3

    if is_local_ip "$ip"; then
        mkdir -p "${LOCAL_EVAL_DIR}/${local_subdir}"
        return 0
    fi

    rm -rf "${LOCAL_EVAL_DIR:?}/${local_subdir}"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "test -d ${REMOTE_EVAL_DIR}/${remote_subdir}" >/dev/null 2>&1 || return 1

    echo " Collecting ${USER}@${ip}:${REMOTE_EVAL_DIR}/${remote_subdir} -> ${LOCAL_EVAL_DIR}/"
    scp -q -o ConnectTimeout=10 -o StrictHostKeyChecking=no -i $SSH_KEY -r \
        "$USER@$ip:${REMOTE_EVAL_DIR}/${remote_subdir}" "${LOCAL_EVAL_DIR}/" 2>/dev/null || return 1
}

kill_on_node() {
    local ip=$1
    local type=$2
    local grace_seconds=45

    echo "→ Stopping ${type} on ${ip}"
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
        $USER@$ip "pkill -TERM ${BINARY_NAME} 2>/dev/null" || true

    local count=0 elapsed=0
    while [ "$elapsed" -lt "$grace_seconds" ]; do
        count=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@$ip "pgrep ${BINARY_NAME} 2>/dev/null | wc -l" || echo 0)
        count=$(echo "$count" | tr -d ' \n')
        [ "$count" -eq 0 ] && break
        sleep 1
        elapsed=$((elapsed + 1))
    done

    if [ "$count" -gt 0 ]; then
        echo "  Killing $count process(es) on $ip"
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -i $SSH_KEY \
            $USER@$ip "pkill -9 ${BINARY_NAME} 2>/dev/null" || true
        sleep 1
    fi
}

echo "=================================================="
echo " EPaxos HOMOGENEOUS Cluster Shutdown (n=${SERVER_COUNT})"
echo "=================================================="

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n STEP 1: Stopping Clients\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for ip in "${CLIENT_IPS[@]}"; do kill_on_node "$ip" "Client" & done
wait

echo -e "\nWaiting 5 seconds for servers to flush metrics..."
sleep 5

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n STEP 2: Stopping Servers (${SERVER_COUNT} nodes)\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for ip in "${SERVER_IPS[@]}"; do kill_on_node "$ip" "Server" & done
wait
sleep 2

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n STEP 3: Cleaning up stale eval directories\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
mkdir -p "${LOCAL_EVAL_DIR}" "${MERGED_DIR}"
for server_dir in "${LOCAL_EVAL_DIR}"/server*/; do
    [ -d "$server_dir" ] || continue
    server_id=$(basename "$server_dir" | sed 's/server//')
    if [ "$server_id" -ge "$SERVER_COUNT" ]; then rm -rf "$server_dir"; fi
done
for client_dir in "${LOCAL_EVAL_DIR}"/client*/; do
    [ -d "$client_dir" ] || continue
    client_id=$(basename "$client_dir" | sed 's/client//')
    if [ "$client_id" -lt "$CLIENT_START_ID" ] || [ "$client_id" -ge "$((CLIENT_START_ID + CLIENT_COUNT))" ]; then
        rm -rf "$client_dir"
    fi
done

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n STEP 4: Collecting client data\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for client_id in $(seq $CLIENT_START_ID $CLIENT_END_ID); do
    i=$((client_id - CLIENT_START_ID))
    vm_ip="${CLIENT_IPS[$(( i % ${#CLIENT_IPS[@]} ))]}"
    copy_eval_dir "$vm_ip" "client${client_id}" "client${client_id}" || true
done

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n STEP 5: Collecting server data\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for i in "${!SERVER_IPS[@]}"; do
    copy_eval_dir "${SERVER_IPS[$i]}" "server${i}" "server${i}" || true
done

echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n STEP 6: Merging CSVs\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [ -f "${MERGE_SCRIPT}" ]; then
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --ids "${CLIENT_ID_FILTER}"
    python3 "${MERGE_SCRIPT}" "${LOCAL_EVAL_DIR}" "${MERGED_DIR}/" --servers --ids "${SERVER_ID_FILTER}"
fi
echo -e "\n==================================================\n HOMOGENEOUS CLUSTER SHUTDOWN COMPLETE\n=================================================="