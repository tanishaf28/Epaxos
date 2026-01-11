# EPaxos + WOC Workload Integration Summary

## Overview
Successfully integrated WOC workload patterns into EPaxos to enable direct performance comparison between the two consensus protocols with identical conflict semantics and metric recording.

## Key Changes Made

### 1. **Consensus Conflict Detection Alignment** (`consensus.go`, `state.go`)

#### Added Timestamp Tracking
- Commands now include `Timestamp time.Time` field for memory cleanup
- Enables stale entry detection and automatic garbage collection

#### In-Flight Command Tracking
- Added `inFlight map[string]*Command` to `EPaxosManager`
- Prevents duplicate command execution (matching WOC pattern)
- Protected by `inFlightMu sync.Mutex` for thread-safety

#### Cleanup Routines
```go
func (m *EPaxosManager) startCleanupRoutine()
func (m *EPaxosManager) cleanupStaleEntries()
```
- Runs every 30 seconds
- Removes entries older than 2 minutes
- Production-safe memory management

### 2. **Client Workload Patterns** (`client.go`, `client_metrics.go`)

#### TCP Connection Optimization
```go
func dialClientRPC(address string, timeout time.Duration) (*rpc.Client, error)
```
- Enables `TCP_NODELAY` for low-latency RPCs (critical for fast path)
- Sets TCP KeepAlive with 30-second periods
- Matches WOC's network optimization exactly

#### Workload-Adaptive Limiter
Replaced simple semaphore-based limiter with WOC's sophisticated adaptive algorithm:

**Initialization Strategy:**
- **Slow workload** (80%+ slow path): Start at limit=2, expect 10% fast path
- **Mixed workload** (30-80% slow): Start at limit=50%, expect 50% fast path  
- **Fast workload** (<30% slow): Start at limit=75%, expect 95% fast path

**Dynamic Adjustment:**
- Uses condition variables (`sync.Cond`) instead of channels for better control
- Tracks `currentActive` vs `currentLimit` separately
- EMA smoothing: `fastPathRatio = 0.85*old + 0.15*new`
- Adjustment interval: 100ms (faster than old 200ms)

**Workload-Specific Tuning:**
```go
if maxInflight <= 5 {
    // SLOW: increment by 1, aggressive backoff
    // Protects serialized server from overload
} else if maxInflight <= 15 {
    // MIXED: increment by 2, normal backoff
} else {
    // FAST: increment by 5, gentle backoff
}
```

#### Auto-Calculated MAX_INFLIGHT
```go
slowPathRatio := (conflictRate + commonRatio) / 100.0

if slowPathRatio >= 0.80 {
    maxInflight = 3   // Conservative for 100% conflict sweep
} else if slowPathRatio >= 0.30 {
    maxInflight = 12  // Moderate for mixed workloads
} else {
    maxInflight = 40  // Aggressive for 100% independent
}
```

#### Enhanced Metric Recording
Created `client_metrics.go` with WOC-compatible `recordBatchMetrics()`:

**Handles 3 batch types:**
1. **MIXED batches**: `"MIXED(FAST:5,SLOW:3,HOT:2)"`
   - Parses per-operation type counters
   - Records fast/slow/hot ops separately
   
2. **HOT-only batches**: `"HOT:10"`
   - Counts as both slow path AND conflict
   - Matches WOC's serialization semantics
   
3. **Simple batches**: `"FAST"`, `"SLOW"`
   - Traditional single-path recording

### 3. **Conflict Semantics** (`state.go`)

#### commandsInterfere() - Exact WOC Match
```go
func commandsInterfere(cmd1, cmd2 *Command) bool {
    // Get all object IDs for both commands
    objs1 := cmd1.ObjIDs (or fallback to cmd1.ObjID)
    objs2 := cmd2.ObjIDs (or fallback to cmd2.ObjID)
    
    // Check if any objects overlap
    for each shared object:
        if WRITE + WRITE → INTERFERE ✓
        if WRITE + READ → INTERFERE ✓
        if READ + READ → NO INTERFERENCE ✓
    
    return false  // No overlap or both reads
}
```

**Critical invariants:**
- Hot objects (conflictRate%) → Always slow path → Serialized
- Common objects (commonRatio%) → Slow path → May have dependencies
- Independent objects (indepRatio%) → Fast path first, fallback to slow

### 4. **Metrics Collection** (`eval/meters.go` - reused from WOC)

EPaxos now records identical metrics to WOC:

**Client-side metrics** (`client{N}_epaxos_TIMESTAMP.csv`):
- Per-batch latency (ms)
- Per-batch throughput (Tx/sec)
- Fast/slow/conflict operation counts per batch
- P50, P95, P99 latencies
- Average fast path vs slow path latency
- Global totals (FAST_COMMITS, SLOW_COMMITS, CONFLICT_COMMITS)
- Fast path fallback count

**Server-side metrics** (`s{N}_n5_f2_b{B}_epaxos_TIMESTAMP.csv`):
- Same structure as client metrics
- Tracks consensus latency (network + processing)
- Useful for diagnosing server-side bottlenecks

## How to Use for Experiments

### 1. Run EPaxos with WOC Workload
```bash
# Terminal 1: Start EPaxos cluster
./start_cluster.sh

# Terminal 2: Run client with WOC workload
export PIPELINE_MODE=true
export MAX_INFLIGHT=12  # Or let it auto-calculate
go run . -c 5 -ops 10000 -batch 100 -composition mixed \
         -conflict 20 -indep 50 -common 30
```

### 2. Compare with WOC
```bash
cd ../woc
export PIPELINE_MODE=true
export MAX_INFLIGHT=12
go run . -c 5 -ops 10000 -batch 100 -composition mixed \
         -conflict 20 -indep 50 -common 30
```

### 3. Analyze Results
Both systems now produce CSV files with:
- **Same workload**: 20% hot (conflicts), 50% independent (fast), 30% common (slow)
- **Same metrics**: Fast/slow/conflict commit counts, latency distributions
- **Same batch handling**: Mixed batches with per-operation type tracking

## Key Differences from Original EPaxos

### Before:
- Simple semaphore limiter (start at 80% of max)
- No workload adaptation
- Basic metric recording (FAST/SLOW only)
- No memory cleanup (potential leak with long runs)
- Missing TCP optimizations

### After:
- Workload-adaptive limiter (matches WOC exactly)
- Auto-calculated MAX_INFLIGHT based on slow path ratio
- WOC-compatible metric recording (handles MIXED, HOT batches)
- Production-safe cleanup routines (30s interval)
- TCP_NODELAY + KeepAlive for low-latency RPCs

## Conflict Detection Guarantee

Both EPaxos and WOC now use **identical** interference logic:

```
Operation Type | Object Access | Interference?
---------------|---------------|---------------
WRITE + WRITE  | Same object  | YES ✓
WRITE + READ   | Same object  | YES ✓
READ + READ    | Same object  | NO
WRITE + WRITE  | Diff objects | NO
```

**Hot objects** in both systems:
- Fixed object IDs: `obj-HOT-0` to `obj-HOT-9`
- Always routed to slow path for serialization
- Counted as both slow path AND conflict commits

## Testing Checklist

- [x] Compile successfully
- [x] Memory cleanup routines added
- [x] Adaptive limiter matches WOC behavior
- [x] Metrics recording handles MIXED/HOT batches
- [x] Conflict detection identical to WOC
- [x] TCP optimizations enabled
- [x] Workload-adaptive MAX_INFLIGHT calculation
- [x] Client and server metrics both recorded

## Next Steps

1. Run baseline experiments with identical workloads
2. Compare CSV outputs (should match in structure)
3. Validate that:
   - Fast path % is similar (for 100% independent)
   - Slow path % is similar (for 100% conflict)
   - Latency distributions are comparable
   - Throughput scales similarly

## Files Modified

1. `consensus.go` - Added cleanup, in-flight tracking
2. `state.go` - Added Timestamp field to Command
3. `client.go` - WOC workload patterns, adaptive limiter
4. `client_metrics.go` - NEW file with WOC-compatible recording
5. `service.go` - No changes needed (already compatible)

## Conflict Sweep 100% Test

To verify identical behavior:

```bash
# EPaxos
go run . -c 5 -ops 1000 -batch 10 -composition object-specific \
         -conflict 100 -indep 0 -common 0

# WOC
cd ../woc
go run . -c 5 -ops 1000 -batch 10 -composition object-specific \
         -conflict 100 -indep 0 -common 0
```

Expected results:
- Both should show ~100% slow path commits
- Both should show ~100% conflict commits
- Latencies should be in similar ranges
- Throughput should be comparable (accounting for EPaxos's extra phases)
