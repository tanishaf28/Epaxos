#!/bin/bash
# ================================================================
# EVAL 6: Client Scaling Evaluation (PLAINMSG)
# Tests: 2, 3, 4, 5, 10, 15, 20, 25, 30 logical clients
# Up to 2 logical clients per VM (15 VMs = 30 max)
# Each configuration runs for 60 seconds
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

USER="ubuntu"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
REMOTE_DIR="/home/ubuntu/woc"
BINARY="woc"
CONFIG_PATH="${REMOTE_DIR}/config/hetero.conf"
LOG_DIR="${REMOTE_DIR}/logs"
EVAL_DIR="${REMOTE_DIR}/eval"
MERGE_SCRIPT="${SCRIPT_DIR}/merge_eval.py"
RESULT_ROOT="${SCRIPT_DIR}/results/eval6_plainmsg_scaling"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

RUNTIME=40
NUM_SERVERS=5
THRESHOLD=1
BATCHSIZE=1
MSG_SIZE=512
PIPELINE_MODE=true
MAX_INFLIGHT=5
LOG_LEVEL="info"
INDEP_RATIO=90.0
COMMON_RATIO=10.0

# ── 5-node WOC server cluster ──────────────────────────────────
SERVER_IPS=(
    "192.168.73.59"   # cora-c32-tani-2  (strong)
    "192.168.73.243"  # cora-c32-tani-1  (strong)
    "192.168.73.192"  # cora-c8-tani-3   (weak)
    "192.168.73.134"  # cora-c8-tani-2   (weak)
    "192.168.73.132"  # cora-c8-tani-1   (weak)
)

# ── 15 client VMs (2 logical clients per VM max = 30 total) ────
CLIENT_VM_IPS=(
    "192.168.73.159"  # c16-1
    "192.168.73.84"   # c16-2
    "192.168.73.218"  # c16-3
    "192.168.73.219"  # c16-4
    "192.168.73.25"   # c16-5
    "192.168.73.117"  # c16-6
    "192.168.73.16"   # c16-7
    "192.168.73.94"   # c16-8
    "192.168.73.173"  # c16-9
    "192.168.73.71"   # c16-10
    "192.168.73.42"   # c16-11
    "192.168.73.106"  # c16-12
    "192.168.73.224"  # c16-13
    "192.168.73.167"  # c16-14
    "192.168.73.137"  # c16-15
)

NUM_CLIENT_VMS=${#CLIENT_VM_IPS[@]}  # 15
CLIENT_COUNTS=(2 3 4 5 10 15 20 25 30)
SSH_OPTS=(-i "$SSH_KEY" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no)

mkdir -p "$RUN_DIR"

echo "=============================================="
echo "EVAL 6: Client Scaling (PLAINMSG)"
echo "  Server cluster : ${#SERVER_IPS[@]} nodes"
echo "  Client VMs     : ${NUM_CLIENT_VMS} VMs (2 slots each = 30 max)"
echo "  Sweep          : ${CLIENT_COUNTS[*]} logical clients"
echo "  Runtime/test   : ${RUNTIME}s"
echo "=============================================="
echo ""

# ── helpers ───────────────────────────────────────────────────

remote_exec() {
    local host=$1; shift
    ssh "${SSH_OPTS[@]}" "$USER@$host" "$*"
}

vm_for_client() {
    local cid=$1
    local slot=$(( cid - NUM_SERVERS ))
    if [ "$slot" -lt 15 ]; then
        echo "$slot"
    else
        echo $(( slot - 15 ))
    fi
}

# ── Binary distribution ────────────────────────────────────────

build_and_distribute() {
    echo "  Building WOC binary..."
    go build -o "$BINARY"

    echo "  Distributing binary to servers..."
    for ip in "${SERVER_IPS[@]}"; do
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" &
    done

    echo "  Distributing binary to all 15 client VMs..."
    for ip in "${CLIENT_VM_IPS[@]}"; do
        remote_exec "$ip" "mkdir -p '$REMOTE_DIR' '$LOG_DIR' '$EVAL_DIR'" &
        scp "${SSH_OPTS[@]}" "$BINARY" "$USER@$ip:$REMOTE_DIR/" &
    done
    wait
    echo "  ✓ Distribution complete"
}

# ── Workload node lifecycle ────────────────────────────────────

start_woc_servers() {
    local label=$1
    for i in "${!SERVER_IPS[@]}"; do
        ip="${SERVER_IPS[$i]}"
        remote_exec "$ip" "
            pkill -f 'woc.*-path' 2>/dev/null || true
            mkdir -p '$LOG_DIR' '$EVAL_DIR/server${i}'
            nohup '$REMOTE_DIR/$BINARY' \
                -id=$i \
                -path='$CONFIG_PATH' \
                -et=1 \
                -n=$NUM_SERVERS \
                -t=$THRESHOLD \
                -b=$BATCHSIZE \
                -mode=1 \
                -msgsize=$MSG_SIZE \
                -bcomp=object-specific \
                -indep=$INDEP_RATIO \
                -common=$COMMON_RATIO \
                -pipeline=$PIPELINE_MODE \
                -maxinflight=$MAX_INFLIGHT \
                -log=$LOG_LEVEL \
                -ep=true \
                -conflictrate=0 \
                -role=0 \
                > '$LOG_DIR/server_${i}_${label}.log' 2>&1 &
        " &
    done
    wait
}

start_woc_clients() {
    local num_clients=$1
    local label=$2

    echo "  Starting ${num_clients} logical clients..."
    for (( c=0; c<num_clients; c++ )); do
        local cid=$(( NUM_SERVERS + c ))
        local vm_idx=$(vm_for_client $cid)
        local ip="${CLIENT_VM_IPS[$vm_idx]}"

        remote_exec "$ip" "
            mkdir -p '$LOG_DIR' '$EVAL_DIR/client${cid}'
            nohup '$REMOTE_DIR/$BINARY' \
                -id=${cid} \
                -path='$CONFIG_PATH' \
                -et=1 \
                -n=$NUM_SERVERS \
                -t=$THRESHOLD \
                -b=$BATCHSIZE \
                -mode=1 \
                -msgsize=$MSG_SIZE \
                -bcomp=object-specific \
                -indep=$INDEP_RATIO \
                -common=$COMMON_RATIO \
                -pipeline=$PIPELINE_MODE \
                -maxinflight=$MAX_INFLIGHT \
                -log=$LOG_LEVEL \
                -ops=0 \
                -conflictrate=0 \
                -role=1 \
                > '$LOG_DIR/client_${cid}_${label}.log' 2>&1 &
        " &
    done
    wait
}

stop_woc_servers() {
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true" &
    done
    wait
    sleep 3
    for ip in "${SERVER_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true" &
    done
    wait
}

stop_woc_clients() {
    local num_clients=$1
    local killed_vms=()

    for (( c=0; c<num_clients; c++ )); do
        local cid=$(( NUM_SERVERS + c ))
        local vm_idx=$(vm_for_client $cid)
        local ip="${CLIENT_VM_IPS[$vm_idx]}"

        local already=false
        for v in "${killed_vms[@]:-}"; do [ "$v" = "$ip" ] && already=true && break; done
        if [ "$already" = false ]; then
            remote_exec "$ip" "pkill -TERM -x woc 2>/dev/null || true" &
            killed_vms+=("$ip")
        fi
    done
    wait
    sleep 3
    for ip in "${killed_vms[@]:-}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true" &
    done
    wait
}

# ── Result collection ──────────────────────────────────────────

archive_case() {
    local label=$1
    local num_clients=$2
    local case_dir="${RUN_DIR}/${label}"
    mkdir -p "$case_dir"

    # Servers
    local idx=0
    for ip in "${SERVER_IPS[@]}"; do
        local node_dir="${case_dir}/node_server_${idx}"
        mkdir -p "$node_dir"
        scp -q "${SSH_OPTS[@]}" -r "$USER@$ip:${EVAL_DIR}/" "$node_dir/" 2>/dev/null || true
        scp -q "${SSH_OPTS[@]}" -r "$USER@$ip:${LOG_DIR}/"  "$node_dir/" 2>/dev/null || true
        idx=$(( idx + 1 ))
    done

    # Clients
    local collected_vms=()
    for (( c=0; c<num_clients; c++ )); do
        local cid=$(( NUM_SERVERS + c ))
        local vm_idx=$(vm_for_client $cid)
        local ip="${CLIENT_VM_IPS[$vm_idx]}"

        local already=false
        for v in "${collected_vms[@]:-}"; do [ "$v" = "$ip" ] && already=true && break; done
        if [ "$already" = false ]; then
            local node_dir="${case_dir}/node_client_vm${vm_idx}"
            mkdir -p "$node_dir"
            scp -q "${SSH_OPTS[@]}" -r "$USER@$ip:${EVAL_DIR}/" "$node_dir/" 2>/dev/null || true
            scp -q "${SSH_OPTS[@]}" -r "$USER@$ip:${LOG_DIR}/"  "$node_dir/" 2>/dev/null || true
            collected_vms+=("$ip")
        fi
    done
}

merge_case_results() {
    local label=$1
    local num_clients=$2
    local case_dir="${RUN_DIR}/${label}"
    local case_eval_dir="${case_dir}/eval"
    local case_merged_dir="${case_dir}/merged"

    mkdir -p "$case_eval_dir" "$case_merged_dir"

    for node_dir in "${case_dir}"/node_*/; do
        [ -d "$node_dir/eval" ] || continue
        cp -r "$node_dir/eval/"* "$case_eval_dir/" 2>/dev/null || true
    done

    if [ ! -f "$MERGE_SCRIPT" ]; then
        echo "  Warning: merge_eval.py not found"
        return
    fi

    local client_start=$NUM_SERVERS
    local client_end=$(( NUM_SERVERS + num_clients - 1 ))
    local server_end=$(( NUM_SERVERS - 1 ))

    python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" \
        --ids "${client_start}-${client_end}"
    python3 "$MERGE_SCRIPT" "$case_eval_dir" "$case_merged_dir/" \
        --servers --ids "0-${server_end}"
}

# ── Per-test-case orchestration ────────────────────────────────

run_scaling_case() {
    local num_clients=$1
    local test_num=$2
    local label="clients_${num_clients}"

    echo ""
    echo "────────────────────────────────────────────────"
    echo "  Test ${test_num}: ${num_clients} logical clients"
    echo "  VMs used: $(( (num_clients + 14) / 15 )) × up to 2 clients each"
    echo "────────────────────────────────────────────────"

    start_woc_servers "$label"

    echo "  Waiting 8s for servers to initialize..."
    sleep 8

    start_woc_clients "$num_clients" "$label"

    echo "  Running for ${RUNTIME}s..."
    sleep "$RUNTIME"

    echo "  Stopping clients..."
    stop_woc_clients "$num_clients"
    sleep 2

    echo "  Stopping servers..."
    stop_woc_servers
    sleep 2

    echo "  Archiving results..."
    archive_case "$label" "$num_clients"
    merge_case_results "$label" "$num_clients"

    echo "  ✓ Test ${test_num} complete"
}

cleanup() {
    echo "  Cleanup: stopping all woc processes..."
    for ip in "${SERVER_IPS[@]}" "${CLIENT_VM_IPS[@]}"; do
        remote_exec "$ip" "pkill -9 -x woc 2>/dev/null || true" &
    done
    wait
}
trap cleanup EXIT

# ── Main ──────────────────────────────────────────────────────

echo "Building and distributing binary..."
build_and_distribute

echo ""
echo "Starting plainmsg client scaling sweep..."

test_num=1
for nc in "${CLIENT_COUNTS[@]}"; do
    run_scaling_case "$nc" "$test_num"
    test_num=$(( test_num + 1 ))
done

echo ""
echo "=============================================="
echo "✓ EVAL 6 COMPLETE (PLAINMSG)"
echo "=============================================="