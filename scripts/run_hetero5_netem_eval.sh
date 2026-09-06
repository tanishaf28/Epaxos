#!/bin/bash
# ================================================================
# HETERO-5 NETEM EVAL (EPAXOS): delay sweep {0,5,10}ms + burst, server-side
# only.
#
# Rebuilt from the old single-fixed-case version, which pointed at
# cluster_hetero_5n_2s_3w.conf -- a different server-VM set than the
# cluster_hetero_5n_10c.conf pool used by crash_eval/ratio_delay/etc.
# Standardized onto cluster_hetero_5n_10c.conf so this eval's server VMs
# match every other category's, and mirrors woc's/cabinet's/raft's netem
# scripts (same 3 delay points + burst, same server-only injection scope)
# so all 4 systems are directly comparable.
#
# Delay is applied to SERVER egress only -- a server's egress interface
# carries RPC replies and server-to-server traffic too, so this still
# shows up in client-measured latency; it's just never applied to a
# CLIENT's own interface.
# ================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$SCRIPT_DIR"

# shellcheck source=../sampler_replacement.sh
source "${REPO_ROOT}/sampler_replacement.sh"

START_SCRIPT="${SCRIPT_DIR}/start_cluster_hetero.sh"
STOP_SCRIPT="${SCRIPT_DIR}/stop_cluster_hetero.sh"
SSH_KEY="/home/ubuntu/.ssh/tani.pem"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/epaxos"

RESULT_ROOT="${SCRIPT_DIR}/results/hetero5_netem_eval"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RESULT_ROOT}/${RUN_TS}"

CONFIG_PATH="${REPO_ROOT}/config/cluster_hetero_5n_10c.conf"
mapfile -t ALL_POOL_IPS < <(awk 'NF >= 2 {print $2}' "$CONFIG_PATH")
SERVER_IPS=("${ALL_POOL_IPS[@]:0:5}")
NUM_CLIENTS="${NUM_CLIENTS:-2}"
CLIENT_IPS=("${ALL_POOL_IPS[@]:5:NUM_CLIENTS}")

RUNTIME_SECONDS="${RUNTIME_SECONDS:-60}"
INDEP_RATIO_FIXED="${INDEP_RATIO_FIXED:-90.0}"
CLUSTER_ACTIVE=false

BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=${NUM_CLIENTS}"
    "THRESHOLD=2"
    "BATCHSIZE=1"
    "MSG_SIZE=512"
    "INDEP_RATIO=${INDEP_RATIO_FIXED}"
    "NUM_OBJECTS=1000"
    "PIPELINE_MODE=true"
    "MAX_INFLIGHT=5"
    "ENABLE_TIMESERIES=true"
    "LOG_LEVEL=info"
    "CONFIG_PATH=${CONFIG_PATH}"
)

mkdir -p "$RUN_DIR"

remote_exec() {
    local host=$1
    shift
    ssh -i "$SSH_KEY" -o ConnectTimeout=5 "$USER@$host" "$*"
}

detect_interface() {
    local host=$1
    remote_exec "$host" "ip route show default 2>/dev/null | awk '{print \$5; exit}'"
}

cache_server_ifaces() {
    _CACHED_SERVER_IFACES=()
    for ip in "${SERVER_IPS[@]}"; do
        _CACHED_SERVER_IFACES+=("$(detect_interface "$ip")")
    done
}

apply_server_only_delay() {
    local delay_ms=$1
    local jitter_ms=$2
    if [ "$delay_ms" -eq 0 ]; then
        remove_server_delay
        return 0
    fi
    echo "  [netem] Applying ${delay_ms}ms ±${jitter_ms}ms to server links only..."
    # netem rejects "distribution normal" at jitter=0ms ("distribution
    # specified but no latency and jitter values"), failing the qdisc add
    # outright -- the `|| true` below swallows it silently, leaving NO
    # delay applied. Omit jitter/distribution entirely when jitter_ms=0.
    local netem_clause="delay ${delay_ms}ms"
    [ "$jitter_ms" -gt 0 ] && netem_clause="delay ${delay_ms}ms ${jitter_ms}ms distribution normal"
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && echo "  Warning: no interface on $ip" && continue
        remote_exec "$ip" \
            "sudo tc qdisc del dev '$iface' root 2>/dev/null || true; \
             sudo tc qdisc add dev '$iface' root netem ${netem_clause}" \
            || true
    done
    sleep 1
}

remove_server_delay() {
    echo "  [netem] Removing server-side delay..."
    for ip in "${SERVER_IPS[@]}"; do
        local iface
        iface=$(detect_interface "$ip")
        [ -z "$iface" ] && continue
        remote_exec "$ip" "sudo tc qdisc del dev '$iface' root 2>/dev/null || true" || true
    done
    sleep 1
}

archive_result() {
    local label=$1
    local dest_dir="${RUN_DIR}/${label}/merged"
    mkdir -p "$dest_dir"
    local marker="${RUN_DIR}/.last_archive_ts"
    local find_args=()
    if [ -f "$marker" ]; then
        find_args=(-newer "$marker")
    fi
    local merged_dir="${SCRIPT_DIR}/eval/merged"
    if [ -d "$merged_dir" ]; then
        find "$merged_dir" -maxdepth 1 -name "*.csv" "${find_args[@]}" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
        if [ -z "$(ls "$dest_dir"/*.csv 2>/dev/null)" ]; then
            cp "$merged_dir"/*.csv "$dest_dir/" 2>/dev/null || true
        fi
    fi

    # ENABLE_TIMESERIES=true (BASE_ENV) makes each client write its own
    # tps_timeline_*.csv under eval/client<id>/ -- previously only the
    # merged/ summary CSVs were archived here, so every timeline file got
    # silently wiped by the next case's client-dir cleanup before ever
    # being copied out.
    local timeline_src="${SCRIPT_DIR}/eval"
    if [ -d "$timeline_src" ]; then
        find "$timeline_src" -name "tps_timeline_*.csv" "${find_args[@]}" -exec cp {} "$dest_dir/" \; 2>/dev/null || true
    fi

    touch "$marker"
    echo "  Archived results to: $dest_dir"
}

start_cluster_with_timeseries() {
    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
}

stop_cluster() {
    # stop_cluster_hetero.sh reads CLIENT_COUNT (default 2), not NUM_CLIENTS
    # -- BASE_ENV only carries NUM_CLIENTS, so without this the stop/collect
    # phase silently falls back to 2 clients regardless of how many were
    # actually started, dropping every client beyond the 2nd from both the
    # graceful-stop pass and the eval-directory collection.
    env "${BASE_ENV[@]}" CLIENT_COUNT="${NUM_CLIENTS}" bash "$STOP_SCRIPT"
    CLUSTER_ACTIVE=false
}

run_d1_case() {
    local label=$1
    local delay_ms=$2
    local jitter_ms=$3

    echo ""
    echo "=================================================="
    echo "Running: $label  [D1 ${delay_ms}ms ±${jitter_ms}ms, server-only]"
    echo "=================================================="

    rm -rf "${SCRIPT_DIR}/eval"/client* "${SCRIPT_DIR}/eval"/server* "${SCRIPT_DIR}/eval/merged" 2>/dev/null || true

    apply_server_only_delay "$delay_ms" "$jitter_ms"
    start_cluster_with_timeseries
    inject_event "delay_${delay_ms}ms"

    sleep "$RUNTIME_SECONDS"

    remove_server_delay
    stop_cluster
    archive_result "$label"
}

run_d4_burst_case() {
    local label=$1
    local calm_duration=$2
    local burst_duration=$3
    local burst_delay_ms="${4:-1000}"
    local burst_jitter_ms="${5:-100}"
    # See apply_server_only_delay's comment: netem rejects jitter=0ms with
    # "distribution normal", which fails the qdisc add silently.
    local burst_netem_clause="delay ${burst_delay_ms}ms"
    [ "$burst_jitter_ms" -gt 0 ] && burst_netem_clause="delay ${burst_delay_ms}ms ${burst_jitter_ms}ms distribution normal"

    echo ""
    echo "=================================================="
    echo "Running: $label  [D4 ${calm_duration}s calm / ${burst_duration}s burst @ ${burst_delay_ms}ms±${burst_jitter_ms}ms, server-only]"
    echo "=================================================="

    rm -rf "${SCRIPT_DIR}/eval"/client* "${SCRIPT_DIR}/eval"/server* "${SCRIPT_DIR}/eval/merged" 2>/dev/null || true

    cache_server_ifaces
    remove_server_delay
    start_cluster_with_timeseries
    inject_event "calm_start"

    local elapsed=0
    local cycle=0
    while [ "$elapsed" -lt "$RUNTIME_SECONDS" ]; do
        inject_event "calm_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        wait

        local calm_sleep=$(( calm_duration < (RUNTIME_SECONDS - elapsed) ? calm_duration : (RUNTIME_SECONDS - elapsed) ))
        sleep "$calm_sleep"
        elapsed=$(( elapsed + calm_sleep ))
        [ "$elapsed" -ge "$RUNTIME_SECONDS" ] && break

        inject_event "burst_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true; \
                 sudo tc qdisc add dev '${_CACHED_SERVER_IFACES[$i]}' root netem ${burst_netem_clause}" \
                || true &
        done
        wait

        local burst_sleep=$(( burst_duration < (RUNTIME_SECONDS - elapsed) ? burst_duration : (RUNTIME_SECONDS - elapsed) ))
        sleep "$burst_sleep"
        elapsed=$(( elapsed + burst_sleep ))
        cycle=$(( cycle + 1 ))
    done

    inject_event "post_burst"
    remove_server_delay
    stop_cluster
    archive_result "$label"
}

cleanup() {
    remove_server_delay || true
    if [ "$CLUSTER_ACTIVE" = true ]; then
        stop_cluster || true
    fi
}
trap cleanup EXIT

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   HETERO-5 NETEM EVAL (EPAXOS): {0,5,10}ms + burst, server-only ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo "Result archive: $RUN_DIR"

D1_DELAYS=(0 5 10)
if [ -n "${DELAY_CASES:-}" ]; then
    read -r -a D1_DELAYS <<< "$DELAY_CASES"
fi
for delay_ms in "${D1_DELAYS[@]}"; do
    jitter_ms=0
    [ "$delay_ms" -ne 0 ] && jitter_ms=$(( delay_ms / 5 ))
    run_d1_case "D1_${delay_ms}ms" "$delay_ms" "$jitter_ms"
done

if [ "${SKIP_BURST:-false}" != "true" ]; then
    run_d4_burst_case "D4_burst_${BURST_DELAY_MS:-1000}ms" 15 10 "${BURST_DELAY_MS:-1000}" "${BURST_JITTER_MS:-100}"
fi

echo ""
echo "Extracting throughput/latency metrics..."
python3 "${REPO_ROOT}/extract_metrics.py" "$RUN_DIR" --size 5

echo ""
echo "=================================================="
echo " Hetero-5 netem eval complete"
echo "=================================================="
echo "Results archived in: $RUN_DIR"
