# Bug Report: Hetero Cluster Scripts

## Files Analyzed
1. ✅ `start_epaxos_hetero.sh` (NEW - created)
2. ✅ `stop_epaxos_hetero.sh` (NEW - created)  
3. ❌ `start_cluster_hetero.sh` (ORIGINAL - has BUGS)
4. ❌ `stop_cluster_hetero.sh` (ORIGINAL - mostly OK)
5. ⚠️  `run_hetero_plainmsg_evals.sh` (MINOR ISSUES)

---

## start_cluster_hetero.sh (ORIGINAL) - CRITICAL BUGS ❌

### Bug 1: THRESHOLD mismatch
```bash
# WRONG (line ~24):
THRESHOLD="${THRESHOLD:-1}"

# CORRECT should be:
THRESHOLD="${THRESHOLD:-2}"
```
**Impact**: EPaxos with 5 servers needs F=2 (2F+1=5). Threshold=1 would only be 2F+1=3 servers!

### Bug 2: INDEP_RATIO and COMMON_RATIO (WOC parameters)
```bash
# WRONG (these are WOC params):
INDEP_RATIO="${INDEP_RATIO:-90.0}"
COMMON_RATIO="${COMMON_RATIO:-10.0}"

# CORRECT for EPaxos:
INDEP_RATIO="${INDEP_RATIO:-0.0}"
COMMON_RATIO="${COMMON_RATIO:-0.0}"
```
**Impact**: EPaxos doesn't use independent/common objects; these are WOC concepts.

### Bug 3: Non-EPaxos parameters
```bash
# WRONG (these don't belong in EPaxos):
USE_ADAPTIVE_LIMITER="${USE_ADAPTIVE_LIMITER:-false}"
PARALLEL_FAST_PATH="${PARALLEL_FAST_PATH:-true}"
LOG_LEVEL="${LOG_LEVEL:-info}"
ENABLE_PRIORITY="${ENABLE_PRIORITY:-true}"
SERVER_BATCHING="false"
```
**Impact**: These are WOC-specific. EPaxos doesn't recognize them.

### Bug 4: Missing THRIFTY parameter
```bash
# MISSING (EPaxos requires this):
THRIFTY="${THRIFTY:-false}"
```
**Impact**: Server start function uses `-thrifty=${THRIFTY}` but param not defined.

### Bug 5: Missing config copy function
```bash
# WRONG (line ~67-75):
for ip in "${SERVER_IPS[@]}" "${CLIENT_HOST_IPS[@]}"; do
    copy_binary "$ip"
    copy_config "$ip"   # ← copy_config function NOT DEFINED
done

# CORRECT (should define copy_config):
copy_config() {
    local TARGET_IP=$1
    echo " Copying config to $TARGET_IP ..."
    ssh -i $SSH_KEY $USER@$TARGET_IP "mkdir -p $REMOTE_DIR/config"
    scp -i $SSH_KEY "$CONFIG_PATH" $USER@$TARGET_IP:$REMOTE_DIR/config/
}
```
**Impact**: Script will crash with "copy_config: command not found"

### Bug 6: Server eval directory not created properly
```bash
# WRONG (line ~94):
mkdir -p ${LOG_DIR}/server${SERVER_ID} ${EVAL_DIR}

# CORRECT should be:
mkdir -p ${LOG_DIR}/server${SERVER_ID} ${EVAL_DIR}/server${SERVER_ID}
```
**Impact**: Server eval data won't be organized by server ID; collection fails.

### Bug 7: Missing server parameters
```bash
# WRONG - Server start function missing:
-suffix=s${SERVER_ID}_n${NUM_SERVERS}_f${THRESHOLD}_b${BATCHSIZE} \
-thrifty=${THRIFTY} \
-ct=0 \
-cm=0 \

# And has wrong parameters:
-ops=${OPS} \        # ← Servers don't take -ops
-ep=${ENABLE_PRIORITY} \  # ← Not an EPaxos param
```
**Impact**: Servers won't start correctly or will ignore critical settings.

### Bug 8: Client eval directory not created properly
```bash
# WRONG (line ~120):
mkdir -p ${LOG_DIR}/client${CLIENT_ID} ${EVAL_DIR}/client${CLIENT_ID}

# But this is only partially used - directory naming inconsistent
```
**Impact**: Evaluation collection may fail for some clients.

---

## start_epaxos_hetero.sh (NEW - CREATED) ✅

**Status**: CORRECT ✅
- ✅ THRESHOLD=2 (correct for 5 servers)
- ✅ INDEP_RATIO=0.0, COMMON_RATIO=0.0 (EPaxos params)
- ✅ THRIFTY defined
- ✅ copy_config function defined
- ✅ Proper eval dir structure: `${EVAL_DIR}/server${SERVER_ID}`
- ✅ Complete server parameters: -suffix, -thrifty, -ct, -cm
- ✅ Correct client parameters

---

## stop_cluster_hetero.sh (ORIGINAL) ✅

**Status**: MOSTLY CORRECT (minor text issues)
- ✅ BINARY_NAME="epaxos" (correct, now updated from attached "woc")
- ✅ Proper graceful shutdown logic
- ✅ CSV collection functions
- ✅ Eval dir merging

**Minor issue fixed in stop_epaxos_hetero.sh**:
```bash
# Now says:
merged_epaxos_clients_*.csv    # Was: merged_woc_clients_*.csv
merged_epaxos_servers_*.csv    # Was: merged_woc_servers_*.csv
```

---

## stop_epaxos_hetero.sh (NEW - CREATED) ✅

**Status**: CORRECT ✅
- All bugs fixed from original
- Proper binary name
- Correct CSV naming

---

## run_hetero_plainmsg_evals.sh ⚠️

**Issues**:
1. Points to `start_cluster_hetero.sh` and `stop_cluster_hetero.sh` (the buggy versions)
2. BASE_ENV has mixed parameters:
   - ✅ Good: NUM_SERVERS, NUM_CLIENTS, THRESHOLD, BATCHSIZE
   - ❌ Bad: INDEP_RATIO=90.0, COMMON_RATIO=10.0, USE_ADAPTIVE_LIMITER, etc.

**Fix needed**: Update BASE_ENV to use EPaxos-correct values:
```bash
BASE_ENV=(
    "NUM_SERVERS=5"
    "NUM_CLIENTS=2"
    "THRESHOLD=2"          # ← Was wrong
    "INDEP_RATIO=0.0"      # ← Was 90.0
    "COMMON_RATIO=0.0"     # ← Was 10.0
    "BATCHSIZE=1"
    "THRIFTY=false"        # ← Add this
    # Remove: USE_ADAPTIVE_LIMITER, PARALLEL_FAST_PATH, ENABLE_PRIORITY, SERVER_BATCHING
)
```

---

## Summary

| Script | Status | Issue |
|--------|--------|-------|
| `start_cluster_hetero.sh` (original) | ❌ **8 bugs** | Mixed WOC/EPaxos params, missing copy_config function, wrong thresholds |
| `start_epaxos_hetero.sh` (created) | ✅ **OK** | Correct EPaxos configuration |
| `stop_cluster_hetero.sh` (original) | ✅ **OK** | Minor cosmetic issues only |
| `stop_epaxos_hetero.sh` (created) | ✅ **OK** | All correct |
| `run_hetero_plainmsg_evals.sh` | ⚠️ **Minor** | Uses wrong BASE_ENV values, calls buggy scripts |

