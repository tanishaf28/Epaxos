package main

import (
	"context"
	"epaxos/eval"
	"epaxos/mongodb"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type EPaxosManager struct {
	sync.RWMutex

	state           *EPaxosState
	serverID        int
	globalClock     int64
	perfM           *eval.PerfMeter
	committedUpTo   []int32
	executedUpTo    []int32
	inFlight        map[string]*Command // Track in-flight commands for deduplication
	inFlightMu      sync.Mutex
	execDone        chan struct{} // Signal to stop execution goroutine
	execWg          sync.WaitGroup
	problemInstance []int         // Highest non-committed instance per replica
	problemTime     []time.Time   // Time when problem was first detected
	recovering map[InstanceID]bool
	recoveryMu sync.Mutex
	deadReplicas      map[int]bool
	deadReplicasMu    sync.RWMutex
	replicaFailures   map[int]int
	replicaFailuresMu sync.Mutex
}

func NewEPaxosManager(serverID, numReplicas int) *EPaxosManager {
	mgr := &EPaxosManager{
		state:           NewEPaxosState(serverID, numReplicas),
		serverID:        serverID,
		globalClock:     0,
		committedUpTo:   make([]int32, numReplicas),
		executedUpTo:    make([]int32, numReplicas),
		inFlight:        make(map[string]*Command),
		execDone:        make(chan struct{}),
		problemInstance: make([]int, numReplicas),
		problemTime:     make([]time.Time, numReplicas),
		recovering:      make(map[InstanceID]bool),
		deadReplicas:    make(map[int]bool),
		replicaFailures: make(map[int]int),
	}
	for i := 0; i < numReplicas; i++ {
		mgr.committedUpTo[i] = -1
		mgr.executedUpTo[i] = -1 
		mgr.problemInstance[i] = -1
	}

	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_epaxos", serverID, numOfServers, threshold, batchsize)
	mgr.perfM = &eval.PerfMeter{}
	mgr.perfM.Init(1, batchsize, fileName)
	mgr.execWg.Add(1)
	go mgr.continuousExecution()

	return mgr
}

// StopExecution stops the continuous execution goroutine with timeout protection
func (m *EPaxosManager) StopExecution() {
	// Close channel to signal stop
	select {
	case <-m.execDone:
		// Already closed
		return
	default:
		close(m.execDone)
	}

	done := make(chan struct{})
	go func() {
		m.execWg.Wait()
		close(done)
	}()

	timeout := time.After(5 * time.Second)
	select {
	case <-done:
		log.Infof("[Execution] Goroutine stopped cleanly")
	case <-timeout:
		log.Warnf("[Execution] Goroutine stop timeout - proceeding anyway")
	}
}

func (m *EPaxosManager) continuousExecution() {
	defer m.execWg.Done()

	ticker := time.NewTicker(50 * time.Millisecond) // Scan every 50ms
	defer ticker.Stop()
	const COMMIT_GRACE_PERIOD = 10 * time.Second // EPaxos reference implementation value

	log.Infof("[Execution] Continuous execution goroutine started for server %d", m.serverID)

	for {
		select {
		case <-m.execDone:
			log.Infof("[Execution] Stopping continuous execution goroutine")
			return
		case <-ticker.C:
			// Scan all replicas' instance spaces for committed instances
			m.state.RLock()
			maxInstances := make(map[int]int)
			for replicaID, maxInst := range m.state.maxInstance {
				maxInstances[replicaID] = maxInst
			}
			m.state.RUnlock()

			for replicaID, maxInst := range maxInstances {
				startInst := int(atomic.LoadInt32(&m.executedUpTo[replicaID])) + 1
				firstProblem := -1

				for instNo := startInst; instNo <= maxInst; instNo++ {
					instanceID := InstanceID{replicaID, instNo}

					// Skip if already executed
					if m.state.IsExecuted(instanceID) {
						currentExecutedUpTo := atomic.LoadInt32(&m.executedUpTo[replicaID])
						if instNo == int(currentExecutedUpTo) + 1 {
							atomic.CompareAndSwapInt32(&m.executedUpTo[replicaID], 
								currentExecutedUpTo, int32(instNo))
						}
						continue
					}

					// Check if committed
					inst := m.state.GetInstance(instanceID)
					inst.RLock()
					status := inst.Status
					deps := inst.Deps.Clone()
					inst.RUnlock()

					if status == COMMITTED {
						// CRITICAL: Check if all dependencies are committed/executed before executing
						// Without this, we violate correctness by executing out of order
						if !m.allDepsCommittedForExec(deps) {
							log.Debugf("[Execution] Waiting for dependencies | Instance=%s", instanceID)
							if firstProblem == -1 {
								firstProblem = instNo
							}
							continue
						}

						// Execute this instance
						err := m.ExecuteCommand(instanceID)
						if err != nil {
							log.Warnf("[Execution] Failed to execute | Instance=%s | Error=%v",
								instanceID, err)
						}
					} else {
						// Found non-committed instance
						if firstProblem == -1 {
							firstProblem = instNo
						}
					}
				}

				// EPaxos recovery mechanism: track stuck instances
				if firstProblem >= 0 {
					m.Lock()
					triggerRecovery := false
					if m.problemInstance[replicaID] != firstProblem {
						// New problem instance detected
						m.problemInstance[replicaID] = firstProblem
						m.problemTime[replicaID] = time.Now()
						log.Debugf("[Recovery] Tracking problem instance | Replica=%d | Instance=%d",
							replicaID, firstProblem)
					} else if time.Since(m.problemTime[replicaID]) > COMMIT_GRACE_PERIOD {
						m.problemTime[replicaID] = time.Now()
						triggerRecovery = true
					}
					m.Unlock()

					if triggerRecovery {
						log.Warnf("[Recovery] Instance stuck for %v, triggering recovery | Instance=R%d.%d",
							COMMIT_GRACE_PERIOD, replicaID, firstProblem)
						go m.startRecoveryForInstance(InstanceID{replicaID, firstProblem})
					}
				} else {
					// No problem instance, reset tracking
					m.Lock()
					m.problemInstance[replicaID] = -1
					m.Unlock()
				}
			}
		}
	}
}

func (m *EPaxosManager) IncrementClock() int64 {
	return atomic.AddInt64(&m.globalClock, 1)
}

func (m *EPaxosManager) HandleCommand(cmd *Command) (InstanceID, []InstanceID, bool, string) {
	globalClock := int(m.IncrementClock())
	m.perfM.RecordStarter(globalClock)
	defer m.perfM.RecordFinisher(globalClock)

	if cmd.IsMixed {
		return m.handleMixedBatch(cmd, globalClock)
	}

	// Single object type batch
	return m.handleSingleTypeBatch(cmd, globalClock)
}

func (m *EPaxosManager) handleMixedBatch(cmd *Command, globalClock int) (InstanceID, []InstanceID, bool, string) {
	if len(cmd.ObjIDs) == 0 {
		log.Errorf("[MixedBatch] Empty ObjIDs array")
		return InstanceID{}, nil, false, "ERROR"
	}

	if shuttingDown.Load() {
		log.Debugf("[MixedBatch] Aborting due to shutdown | ClientClock=%d", cmd.ClientClock)
		return InstanceID{}, nil, false, "SHUTDOWN"
	}

	log.Infof("[MixedBatch] Processing %d operations | ClientClock=%d",
		len(cmd.ObjIDs), cmd.ClientClock)

	// Track per-type results
	fastOps := 0
	slowOps := 0
	hotOps := 0

	// Store ALL instance IDs for client reply tracking (not just first!)
	var instanceIDs []InstanceID

	// Try fast path for all operations (Object Manager routing happens inside runFastPath)
	for i := 0; i < len(cmd.ObjIDs); i++ {
		if shuttingDown.Load() {
			log.Warnf("[MixedBatch] Shutdown during batch processing | Completed=%d/%d", i, len(cmd.ObjIDs))
			if len(instanceIDs) > 0 {
				return instanceIDs[0], instanceIDs, false, "SHUTDOWN"
			}
			return InstanceID{}, nil, false, "SHUTDOWN"
		}

		objID := cmd.ObjIDs[i]
		objType := cmd.ObjTypes[i]

		singleCmd := &Command{
			ClientID:    cmd.ClientID,
			ClientClock: cmd.ClientClock*1000 + i,  // Unique per operation in batch
			CmdType:     cmd.CmdType,
			ObjID:       objID,
			ObjType:     objType,
			ObjIDs:      []string{objID},
			ObjTypes:    []int{objType},
			IsMixed:     false,
			Timestamp:   time.Now(),
		}

		// Extract payload
		switch payload := cmd.Payload.(type) {
		case [][]byte:
			if i < len(payload) {
				singleCmd.Payload = [][]byte{payload[i]}
			}
		case []mongodb.Query:
			if i < len(payload) {
				singleCmd.Payload = []mongodb.Query{payload[i]}
			}
		}

		instanceID := m.state.GetNextInstance()
		instanceIDs = append(instanceIDs, instanceID)
		success, _, fastPathStart := m.runFastPath(instanceID, singleCmd)

		if !success {
			m.perfM.RecordFastPathFallback()
			success, _ = m.runSlowPath(instanceID, singleCmd, fastPathStart)

			if success {
				m.perfM.MarkSlowPath(globalClock) // Mark batch as using slow path
				if objType == DependentObject {
					hotOps++
					atomic.AddInt64(&m.perfM.ConflictCommits, 1)
					m.perfM.IncConflict(globalClock)
				} else {
					slowOps++
					atomic.AddInt64(&m.perfM.SlowCommits, 1)
					m.perfM.IncSlowPath(globalClock)
				}
			}
		} else {
			// Fast path succeeded
			fastOps++
			atomic.AddInt64(&m.perfM.FastCommits, 1)
			m.perfM.IncFastPath(globalClock)
		}

		if !success {
			log.Warnf("[MixedBatch] Operation %d/%d failed | ObjID=%s | ObjType=%d",
				i+1, len(cmd.ObjIDs), objID, objType)
		}
	}

	// Build result string
	pathUsed := fmt.Sprintf("MIXED(FAST:%d,SLOW:%d,HOT:%d)", fastOps, slowOps, hotOps)

	log.Infof("[MixedBatch] Complete | Fast=%d | Slow=%d | Hot=%d | Total=%d",
		fastOps, slowOps, hotOps, len(cmd.ObjIDs))

	allSuccess := (fastOps + slowOps + hotOps) == len(cmd.ObjIDs)

	// Return first instance ID (used as batch ID) and all instance IDs
	// The batch reply handler will use all instanceIDs to track all executions
	if len(instanceIDs) > 0 {
		return instanceIDs[0], instanceIDs, allSuccess, pathUsed
	}
	return InstanceID{}, nil, allSuccess, pathUsed
}

func (m *EPaxosManager) handleSingleTypeBatch(cmd *Command, globalClock int) (InstanceID, []InstanceID, bool, string) {
	if shuttingDown.Load() {
		log.Debugf("[SingleTypeBatch] Aborting due to shutdown | ClientClock=%d", cmd.ClientClock)
		return InstanceID{}, nil, false, "SHUTDOWN"
	}

	batchSize := len(cmd.ObjIDs)
	if batchSize == 0 {
		batchSize = 1
	}

	instanceID := m.state.GetNextInstance()

	log.Infof("[SingleTypeBatch] Starting | Instance=%s | ObjType=%d | BatchSize=%d | ClientClock=%d",
		instanceID, cmd.ObjType, batchSize, cmd.ClientClock)
	success, path, fastPathStart := m.runFastPath(instanceID, cmd)

	if !success {
		log.Infof("[EPaxos] Fast path failed, trying slow path | Instance=%s | ObjType=%d",
			instanceID, cmd.ObjType)
		m.perfM.RecordFastPathFallback()
		success, path = m.runSlowPath(instanceID, cmd, fastPathStart)

		if success {
			atomic.AddInt64(&m.perfM.SlowCommits, int64(batchSize))

			if cmd.ObjType == DependentObject {
				atomic.AddInt64(&m.perfM.ConflictCommits, int64(batchSize))
				for i := 0; i < batchSize; i++ {
					m.perfM.IncConflict(globalClock)
				}
			} else {
				for i := 0; i < batchSize; i++ {
					m.perfM.IncSlowPath(globalClock)
				}
			}

			m.perfM.MarkSlowPath(globalClock)
			if cmd.ObjType == DependentObject {
				path = fmt.Sprintf("HOT:%d", batchSize)
			}
		}
	} else {
		// Fast path succeeded
		atomic.AddInt64(&m.perfM.FastCommits, int64(batchSize))
		for i := 0; i < batchSize; i++ {
			m.perfM.IncFastPath(globalClock)
		}
	}

	if success {
		m.state.UpdateCommitIndex(1)
	}

	log.Infof("[SingleTypeBatch] Complete | Instance=%s | Path=%s | Success=%v | ObjType=%d | BatchSize=%d",
		instanceID, path, success, cmd.ObjType, batchSize)

	return instanceID, []InstanceID{instanceID}, success, path
}


func (m *EPaxosManager) runFastPath(instanceID InstanceID, cmd *Command) (bool, string, time.Time) {
	// Abort early if shutting down
	if shuttingDown.Load() {
		log.Debugf("[FastPath] Aborting due to shutdown | Instance=%s", instanceID)
		return false, "FAST", time.Time{}
	}

	start := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inst := m.state.GetInstance(instanceID)
	inst.Lock()
	inst.Command = cmd
	inst.Status = PREACCEPTED
	inst.Ballot = 0

	objIDs := cmd.ObjIDs
	if len(objIDs) == 0 && cmd.ObjID != "" {
		objIDs = []string{cmd.ObjID}
	}

	inst.Unlock()

	// Keep dependency read + registration + seq computation atomic per replica.
	var deps Dependencies
	var seq int
	m.state.AtomicPreAccept(func() {
		// Read deps BEFORE registering self to avoid self-dependency.
		deps = m.state.GetInterferingInstances(cmd)
		m.state.RegisterObjectAccess(instanceID, objIDs)
		seq = m.state.GetMaxSeq(deps) + 1
	})

	inst.Lock()
	inst.Seq = seq
	inst.Deps = deps.Clone()
	inst.Unlock()

	log.Debugf("[FastPath] Initial | Instance=%s | Seq=%d | Deps=%d",
		instanceID, seq, len(deps))

	args := &PreAcceptArgs{
		InstanceID: instanceID,
		Command:    cmd,
		Seq:        seq,
		Deps:       deps.Clone(),
		Ballot:     0, // Initial ballot
		LeaderID:   m.serverID,
	}

	responses := make(chan *PreAcceptReply, numOfServers)
	var wg sync.WaitGroup

	conns.RLock()
	connList := make([]*ServerConnection, 0, len(conns.m))
	for _, conn := range conns.m {
		connList = append(connList, conn)
	}
	conns.RUnlock()

	// Thrifty mode optimization for PreAccept: contact F + ⌊(F+1)/2⌋ replicas instead of all.
	contactCount := len(connList)
	if thriftyMode {
		contactCount = thriftyPreAcceptContact
		if contactCount > len(connList) {
			contactCount = len(connList)
		}
		log.Debugf("[FastPath-Thrifty] Contacting %d/%d replicas | Instance=%s",
			contactCount, len(connList), instanceID)
	}

	log.Infof("[FastPath] Broadcasting PreAccept to %d replicas | Instance=%s",
		contactCount, instanceID)

	for i := 0; i < contactCount && i < len(connList); i++ {
		conn := connList[i]

		// Skip known-dead replicas to avoid wasting time on timeouts
		if m.isReplicaDead(conn.replicaID) {
			log.Debugf("[FastPath] Skipping dead replica %d | Instance=%s",
				conn.replicaID, instanceID)
			continue
		}

		wg.Add(1)
		go func(c *ServerConnection) {
			defer wg.Done()
			reply := &PreAcceptReply{}

			done := make(chan error, 1)
			go func() {
				done <- c.rpcClient.Call("EPaxosService.PreAccept", args, reply)
			}()

			select {
			case err := <-done:
				if err == nil && reply.OK {
					m.trackReplicaSuccess(c.replicaID)
					select {
					case responses <- reply:
					case <-ctx.Done():
					}
					log.Infof("[FastPath] PreAccept SUCCESS | Replica=%d | Instance=%s | IsOK=%v",
						c.replicaID, instanceID, reply.IsPreAcceptOK)
				} else if err != nil {
					m.trackReplicaFailure(c.replicaID)
					// Send negative reply for errors so loop can count and terminate.
					select {
					case responses <- &PreAcceptReply{OK: false}:
					case <-ctx.Done():
					}
					log.Errorf("[FastPath] PreAccept RPC FAILED | Replica=%d | Instance=%s | Error=%v",
						c.replicaID, instanceID, err)
				} else {
					// Send negative reply for OK=false.
					select {
					case responses <- &PreAcceptReply{OK: false}:
					case <-ctx.Done():
					}
					log.Warnf("[FastPath] PreAccept returned OK=false | Replica=%d | Instance=%s",
						c.replicaID, instanceID)
				}
			case <-time.After(2 * time.Second):
				m.trackReplicaFailure(c.replicaID)
				// Count timeout as negative response so loop terminates properly.
				select {
				case responses <- &PreAcceptReply{OK: false}:
				case <-ctx.Done():
				}
				log.Warnf("[FastPath] PreAccept timeout (2s) | Replica=%d | Instance=%s",
					c.replicaID, instanceID)
			}
		}(conn)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	preAcceptOKCount := 0 // Lightweight OK replies (attrs unchanged, no need to inspect)
	fullMatchCount := 0   // Full replies that still agree with the leader's proposed attrs
	disagreeCount := 0    // Full replies that changed seq/deps from what the leader proposed
	nackCount := 0        // OK=false: RPC failure/timeout/stale-ballot — doesn't count either way

	mergedSeq := seq
	mergedDeps := deps.Clone()
	proposedSeq := seq
	proposedDeps := deps.Clone()
	allEqual := true // Tracks whether replies preserve the leader-proposed attributes.

	for reply := range responses {
		if !reply.OK {
			// Failure/timeout/stale-ballot reply: not a vote either way, must not
			// be treated as a disagreement (it carries no real Seq/Deps).
			nackCount++
		} else if reply.IsPreAcceptOK {
			preAcceptOKCount++
			// PreAcceptOK means attributes unchanged — original attrs still valid
		} else {
			// Merge from full replies, regardless of fast/slow path decision.
			if reply.Seq > mergedSeq {
				mergedSeq = reply.Seq
			}
			mergedDeps = mergedDeps.Union(reply.Deps)

			// Full reply is still fast-path compatible only if it preserves proposed attrs.
			if reply.Seq != proposedSeq || !reply.Deps.Equal(proposedDeps) {
				allEqual = false
				disagreeCount++
			} else {
				fullMatchCount++
			}
		}

		// BUG FIX #9: Early exit when we have all contacted replies (not all servers)
		if preAcceptOKCount+fullMatchCount+disagreeCount+nackCount >= contactCount {
			cancel()
			break
		}
	}

	latency := time.Since(start).Milliseconds()
	matchCount := preAcceptOKCount + fullMatchCount

	log.Debugf("[FastPath] Collected replies | Instance=%s | PreAcceptOKs=%d | FullMatches=%d | Nacks=%d | AllEqual=%v",
		instanceID, preAcceptOKCount, fullMatchCount, nackCount, allEqual)

	if matchCount >= fastQuorum && allEqual {
		inst.Lock()
		inst.Seq = seq   // Keep original seq
		inst.Deps = deps // Keep original deps
		inst.Status = COMMITTED
		inst.Unlock()

		m.state.SetInstance(instanceID, inst)
		m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)

		log.Infof("[FastPath] SUCCESS | Instance=%s | Seq=%d | MatchingReplies=%d | Latency=%dms",
			instanceID, seq, matchCount, latency)

		go m.broadcastCommit(instanceID, seq, deps)
		return true, "FAST", start
	}

	if !allEqual {
		inst.Lock()
		inst.Seq = mergedSeq
		inst.Deps = mergedDeps
		inst.Unlock()
	}

	log.Debugf("[FastPath] Cannot commit | Instance=%s | Reason: MatchingReplies=%d (need %d) OR allEqual=%v",
		instanceID, matchCount, fastQuorum, allEqual)

	return false, "FAST", start
}

func (m *EPaxosManager) updateCommittedUpTo(replicaID int, instanceNo int) {
	m.Lock()
	defer m.Unlock()

	if int32(instanceNo) > m.committedUpTo[replicaID] {
		m.committedUpTo[replicaID] = int32(instanceNo)
	}
}

func (m *EPaxosManager) allDepsCommittedForExec(deps Dependencies) bool {
	for replicaID, instNo := range deps {
		if instNo < 0 { // -1 means no dependency
			continue
		}

		depID := InstanceID{replicaID, int(instNo)}
		if m.state.IsExecuted(depID) {
			continue
		}

		dep := m.state.GetInstance(depID)
		dep.RLock()
		status := dep.Status
		dep.RUnlock()

		if status == NONE {
			log.Warnf("[Execute] Dependency instance not created yet | DepID=%s | Status=NONE",
				depID)
			return false
		}

		if status != COMMITTED && status != EXECUTED {
			// Dependency not ready
			return false
		}
	}
	return true
}

func (m *EPaxosManager) allDepsCommitted(deps Dependencies) bool {
	m.RLock()
	defer m.RUnlock()

	for replicaID, instNo := range deps {
		if instNo < 0 { // -1 means no dependency
			continue
		}
		// Check if this instance is committed
		if instNo > int32(m.committedUpTo[replicaID]) {
			return false // This dependency is not yet committed
		}
	}
	return true
}

func (m *EPaxosManager) runSlowPath(instanceID InstanceID, cmd *Command, fastPathStart time.Time) (bool, string) {
	if shuttingDown.Load() {
		log.Debugf("[SlowPath] Aborting due to shutdown | Instance=%s", instanceID)
		return false, "SLOW"
	}

	acceptPhaseStart := time.Now()
	rpcTimeout := 500 * time.Millisecond
	fallbackDelay := 500 * time.Millisecond

	inst := m.state.GetInstance(instanceID)
	inst.Lock()
	seq := inst.Seq
	deps := inst.Deps.Clone()
	ballot := inst.Ballot
	command := cmd
	if command == nil {
		command = inst.Command
	}
	inst.Status = ACCEPTED
	inst.Unlock()

	log.Debugf("[SlowPath] Starting Accept | Instance=%s | Seq=%d | Deps=%d",
		instanceID, seq, len(deps))

	args := &AcceptArgs{
		InstanceID: instanceID,
		Command:    command,
		Seq:        seq,
		Deps:       deps,
		LeaderID:   m.serverID,
		Ballot:     ballot,
	}

	conns.RLock()
	connList := make([]*ServerConnection, 0, len(conns.m))
	for _, conn := range conns.m {
		connList = append(connList, conn)
	}
	conns.RUnlock()

	// Thrifty mode for Accept: contact F+1 replicas first, then expand if needed.
	contactCount := len(connList)
	if thriftyMode {
		contactCount = thriftyAcceptContact
		if contactCount > len(connList) {
			contactCount = len(connList)
		}
		log.Debugf("[SlowPath-Thrifty] Contacting %d/%d replicas | Instance=%s",
			contactCount, len(connList), instanceID)
	}

	log.Infof("[SlowPath] Broadcasting Accept to %d replicas | Instance=%s",
		contactCount, instanceID)

	sendAcceptRound := func(targets []*ServerConnection, timeout time.Duration) int {
		if len(targets) == 0 {
			return 0
		}

		responses := make(chan *AcceptReply, len(targets))
		var roundWG sync.WaitGroup

		for _, conn := range targets {
			if m.isReplicaDead(conn.replicaID) {
				log.Debugf("[SlowPath] Skipping dead replica %d | Instance=%s",
					conn.replicaID, instanceID)
				continue
			}

			roundWG.Add(1)
			go func(c *ServerConnection) {
				defer roundWG.Done()
				reply := &AcceptReply{}

				done := make(chan error, 1)
				go func() {
					done <- c.rpcClient.Call("EPaxosService.Accept", args, reply)
				}()

				select {
				case err := <-done:
					if err == nil && reply.OK {
						m.trackReplicaSuccess(c.replicaID)
						responses <- reply
						log.Infof("[SlowPath] Accept SUCCESS | Replica=%d | Instance=%s",
							c.replicaID, instanceID)
					} else if err != nil {
						m.trackReplicaFailure(c.replicaID)
						log.Errorf("[SlowPath] Accept RPC FAILED | Replica=%d | Instance=%s | Error=%v",
							c.replicaID, instanceID, err)
					} else {
						log.Warnf("[SlowPath] Accept returned OK=false | Replica=%d | Instance=%s",
							c.replicaID, instanceID)
					}
				case <-time.After(timeout):
					m.trackReplicaFailure(c.replicaID)
					log.Warnf("[SlowPath] Accept timeout (%v) | Replica=%d | Instance=%s",
						timeout, c.replicaID, instanceID)
				}
			}(conn)
		}

		go func() {
			roundWG.Wait()
			close(responses)
		}()

		successful := 0
		for range responses {
			successful++
		}

		return successful
	}

	acceptCount := sendAcceptRound(connList[:contactCount], rpcTimeout)

	if thriftyMode && acceptCount < slowQuorum && contactCount < len(connList) {
		log.Warnf("[SlowPath-Thrifty] Quorum not reached after initial round (%d/%d), expanding to remaining replicas | Instance=%s",
			acceptCount, slowQuorum, instanceID)
		time.Sleep(fallbackDelay)
		acceptCount += sendAcceptRound(connList[contactCount:], rpcTimeout)
	}

	acceptPhaseLatency := time.Since(acceptPhaseStart).Milliseconds()
	totalLatency := time.Since(fastPathStart).Milliseconds()
	if acceptCount < slowQuorum {
		log.Warnf("[SlowPath] Insufficient accepts | Instance=%s | RemoteAccepts=%d | Need=%d",
			instanceID, acceptCount, slowQuorum)
		return false, "SLOW"
	}

	inst.Lock()
	inst.Status = COMMITTED
	inst.Unlock()

	m.state.SetInstance(instanceID, inst)
	m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)

	log.Infof("[SlowPath] SUCCESS | Instance=%s | Seq=%d | Deps=%d | TotalLatency(PreAccept+Accept)=%dms | AcceptPhase=%dms",
		instanceID, seq, len(deps), totalLatency, acceptPhaseLatency)

	go m.broadcastCommit(instanceID, seq, deps)

	return true, "SLOW"
}

func (m *EPaxosManager) broadcastCommit(instanceID InstanceID, seq int, deps Dependencies) {
	inst := m.state.GetInstance(instanceID)
	inst.RLock()
	command := inst.Command
	inst.RUnlock()

	args := &CommitArgs{
		InstanceID: instanceID,
		Command:    command,
		Seq:        seq,
		Deps:       deps,
	}

	conns.RLock()
	connList := make([]*ServerConnection, 0, len(conns.m))
	for _, conn := range conns.m {
		connList = append(connList, conn)
	}
	conns.RUnlock()

	for _, conn := range connList {
		go func(c *ServerConnection) {
			reply := &CommitReply{}
			err := c.rpcClient.Call("EPaxosService.Commit", args, reply)
			if err != nil {
				log.Errorf("[Commit] RPC FAILED | Replica=%d | Instance=%s | Error=%v",
					c.replicaID, instanceID, err)
			}
		}(conn)
	}
}

func (m *EPaxosManager) markReplicaDead(replicaID int) {
	m.deadReplicasMu.Lock()
	if !m.deadReplicas[replicaID] {
		m.deadReplicas[replicaID] = true
		m.deadReplicasMu.Unlock()
		log.Warnf("[CRASH-DETECT] Replica %d marked as dead", replicaID)
	} else {
		m.deadReplicasMu.Unlock()
	}
}

func (m *EPaxosManager) isReplicaDead(replicaID int) bool {
	m.deadReplicasMu.RLock()
	defer m.deadReplicasMu.RUnlock()
	return m.deadReplicas[replicaID]
}

func (m *EPaxosManager) trackReplicaFailure(replicaID int) {
	m.replicaFailuresMu.Lock()
	m.replicaFailures[replicaID]++
	failures := m.replicaFailures[replicaID]
	m.replicaFailuresMu.Unlock()

	// Mark dead after 3 consecutive failures
	if failures >= 3 {
		m.markReplicaDead(replicaID)
	}
}

// trackReplicaSuccess resets failure count on successful response
func (m *EPaxosManager) trackReplicaSuccess(replicaID int) {
	m.replicaFailuresMu.Lock()
	m.replicaFailures[replicaID] = 0
	m.replicaFailuresMu.Unlock()
}

func (m *EPaxosManager) SaveMetrics() error {
	if m.perfM == nil {
		return fmt.Errorf("performance meter not initialized")
	}
	return m.perfM.SaveToFile()
}

// startRecoveryForInstance runs the full EPaxos Explicit Prepare recovery
// Per EPaxos paper Figure 3: Prepare → discover state → Accept/Commit
// This implements all 4 recovery rules for safety
func (m *EPaxosManager) startRecoveryForInstance(instanceID InstanceID) {
	log.Warnf("[Recovery] Starting Explicit Prepare recovery | Instance=%s", instanceID)

	// Step 0: Only one goroutine should recover a given instance
	m.recoveryMu.Lock()
	if m.recovering[instanceID] {
		m.recoveryMu.Unlock()
		log.Debugf("[Recovery] Already recovering | Instance=%s", instanceID)
		return
	}
	m.recovering[instanceID] = true
	m.recoveryMu.Unlock()
	defer func() {
		m.recoveryMu.Lock()
		delete(m.recovering, instanceID)
		m.recoveryMu.Unlock()
	}()

	// ── PHASE 1: PREPARE ────────────────────────────────────────────────────
	// Increment ballot to be higher than anything seen so far
	inst := m.state.GetInstance(instanceID)
	inst.RLock()
	currentBallot := inst.Ballot
	inst.RUnlock()

	newBallot := currentBallot + numOfServers + m.serverID // Break ties by server ID

	prepareArgs := &PrepareArgs{
		InstanceID: instanceID,
		Ballot:     newBallot,
		LeaderID:   m.serverID,
	}

	type prepareResult struct {
		replicaID int
		reply     *PrepareReply
	}
	results := make(chan prepareResult, numOfServers)
	var wg sync.WaitGroup

	conns.RLock()
	connList := make([]*ServerConnection, 0, len(conns.m))
	for _, conn := range conns.m {
		connList = append(connList, conn)
	}
	conns.RUnlock()

	// Also include self
	selfInst := m.state.GetInstance(instanceID)
	selfInst.RLock()
	selfReply := &PrepareReply{
		OK:        true,
		Ballot:    selfInst.Ballot,
		Status:    selfInst.Status,
		Seq:       selfInst.Seq,
		Deps:      selfInst.Deps.Clone(),
		Command:   selfInst.Command,
		ReplicaID: m.serverID,
	}
	selfInst.RUnlock()
	results <- prepareResult{m.serverID, selfReply}

	for _, conn := range connList {
		wg.Add(1)
		go func(c *ServerConnection) {
			defer wg.Done()
			reply := &PrepareReply{}
			done := make(chan error, 1)
			go func() { done <- c.rpcClient.Call("EPaxosService.Prepare", prepareArgs, reply) }()
			select {
			case err := <-done:
				if err == nil && reply.OK {
					results <- prepareResult{c.replicaID, reply}
				}
			case <-time.After(3 * time.Second):
				log.Warnf("[Recovery] Prepare timeout | Replica=%d", c.replicaID)
			}
		}(conn)
	}

	go func() { wg.Wait(); close(results) }()

	// Collect until majority (F+1 including self)
	var prepareReplies []*PrepareReply
	prepareReplies = append(prepareReplies, selfReply)
	for res := range results {
		if res.reply.Ballot > newBallot {
			// Someone has a higher ballot — abort, they will recover
			log.Warnf("[Recovery] Higher ballot seen (%d > %d), aborting | Instance=%s",
				res.reply.Ballot, newBallot, instanceID)
			return
		}
		prepareReplies = append(prepareReplies, res.reply)
		if len(prepareReplies) > numOfServers/2 {
			break
		}
	}

	if len(prepareReplies) <= threshold {
		log.Warnf("[Recovery] Insufficient Prepare replies | Got=%d | Need=%d",
			len(prepareReplies), threshold+1)
		return
	}

	// ── PHASE 2: DECIDE what to do based on collected state ─────────────────
	// EPaxos paper Figure 3 rules (in priority order):

	// Rule 1: If ANY replica has COMMITTED or EXECUTED → just commit with those attrs
	for _, r := range prepareReplies {
		if r.Status == COMMITTED || r.Status == EXECUTED {
			log.Infof("[Recovery] Found committed state, broadcasting commit | Instance=%s", instanceID)
			inst.Lock()
			inst.Status = COMMITTED
			inst.Seq = r.Seq
			inst.Deps = r.Deps
			inst.Command = r.Command
			inst.Ballot = newBallot
			inst.Unlock()
			m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)
			go m.broadcastCommit(instanceID, r.Seq, r.Deps)
			return
		}
	}

	// Rule 2: If ANY replica has ACCEPTED → must run Accept with those attrs
	var highestAcceptedBallot int = -1
	var acceptedSeq int
	var acceptedDeps Dependencies
	var acceptedCmd *Command
	for _, r := range prepareReplies {
		if r.Status == ACCEPTED && r.Ballot > highestAcceptedBallot {
			highestAcceptedBallot = r.Ballot
			acceptedSeq = r.Seq
			acceptedDeps = r.Deps
			acceptedCmd = r.Command
		}
	}
	if highestAcceptedBallot >= 0 {
		log.Infof("[Recovery] Found accepted state, running Accept phase | Instance=%s", instanceID)
		inst.Lock()
		inst.Seq = acceptedSeq
		inst.Deps = acceptedDeps
		inst.Command = acceptedCmd
		inst.Ballot = newBallot
		inst.Unlock()
		m.runRecoveryAccept(instanceID, acceptedCmd, acceptedSeq, acceptedDeps, newBallot)
		return
	}

	// Rule 3: If N-F or more replicas have PREACCEPTED with same attrs →
	//         can fast-path commit (EPaxos "identical PreAccept" rule)
	type attrKey struct {
		seq      int
		depsHash string
	}
	attrCount := make(map[attrKey]int)
	attrSeq := make(map[attrKey]int)
	attrDeps := make(map[attrKey]Dependencies)
	attrCmd := make(map[attrKey]*Command)

	for _, r := range prepareReplies {
		if r.Status == PREACCEPTED {
			depsStr := fmt.Sprintf("%v", r.Deps)
			k := attrKey{r.Seq, depsStr}
			attrCount[k]++
			attrSeq[k] = r.Seq
			attrDeps[k] = r.Deps
			attrCmd[k] = r.Command
		}
	}

	// N-F = numOfServers - threshold (guarantees fast path safety)
	fastRecoveryQuorum := numOfServers - threshold
	for k, count := range attrCount {
		if count >= fastRecoveryQuorum {
			log.Infof("[Recovery] Fast recovery: %d replicas agree on attrs | Instance=%s",
				count, instanceID)
			inst.Lock()
			inst.Seq = attrSeq[k]
			inst.Deps = attrDeps[k]
			inst.Command = attrCmd[k]
			inst.Status = COMMITTED
			inst.Ballot = newBallot
			inst.Unlock()
			m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)
			go m.broadcastCommit(instanceID, attrSeq[k], attrDeps[k])
			return
		}
	}

	// Rule 4: Otherwise → run slow-path Accept with merged/local attrs
	var bestSeq int
	var bestDeps Dependencies
	var bestCmd *Command
	var bestBallot int = -1
	for _, r := range prepareReplies {
		if r.Status == PREACCEPTED && r.Ballot > bestBallot {
			bestBallot = r.Ballot
			bestSeq = r.Seq
			bestDeps = r.Deps
			bestCmd = r.Command
		}
	}
	if bestBallot < 0 {
		// BUG FIX #26: No replica has PREACCEPTED this instance - treat as noop
		log.Warnf("[Recovery] No replica has preaccepted %s - treating as noop", instanceID)
		inst.Lock()
		inst.Status = COMMITTED
		inst.Seq = 0
		inst.Deps = NewDependencies(numOfServers)
		inst.Command = nil  // Noop command
		inst.Ballot = newBallot
		inst.Unlock()
		m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)
		return
	}

	log.Infof("[Recovery] Slow recovery: running Accept with merged attrs | Instance=%s", instanceID)
	inst.Lock()
	inst.Seq = bestSeq
	inst.Deps = bestDeps
	inst.Command = bestCmd
	inst.Ballot = newBallot
	inst.Unlock()
	m.runRecoveryAccept(instanceID, bestCmd, bestSeq, bestDeps, newBallot)
}

// runRecoveryAccept runs the Accept phase during recovery with ballot tracking
func (m *EPaxosManager) runRecoveryAccept(instanceID InstanceID, command *Command, seq int, deps Dependencies, ballot int) {
	args := &AcceptArgs{
		InstanceID: instanceID,
		Command:    command,
		Seq:        seq,
		Deps:       deps,
		LeaderID:   m.serverID,
		Ballot:     ballot,
	}
	responses := make(chan *AcceptReply, numOfServers)
	var wg sync.WaitGroup

	conns.RLock()
	connList := make([]*ServerConnection, 0, len(conns.m))
	for _, conn := range conns.m {
		connList = append(connList, conn)
	}
	conns.RUnlock()

	for _, conn := range connList {
		wg.Add(1)
		go func(c *ServerConnection) {
			defer wg.Done()
			reply := &AcceptReply{}
			done := make(chan error, 1)
			go func() { done <- c.rpcClient.Call("EPaxosService.Accept", args, reply) }()
			select {
			case err := <-done:
				if err == nil && reply.OK {
					responses <- reply
				}
			case <-time.After(500 * time.Millisecond):
			}
		}(conn)
	}

	go func() { wg.Wait(); close(responses) }()

	acceptCount := 0 // Remote accepts only; leader vote is implicit.
	for range responses {
		acceptCount++
		if acceptCount >= slowQuorum {
			break
		}
	}

	if acceptCount >= slowQuorum {
		inst := m.state.GetInstance(instanceID)
		inst.Lock()
		inst.Status = COMMITTED
		inst.Unlock()
		m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)
		go m.broadcastCommit(instanceID, seq, deps)
		log.Infof("[Recovery] Accept quorum reached, committed | Instance=%s", instanceID)
	} else {
		log.Warnf("[Recovery] Accept quorum failed | Instance=%s", instanceID)
	}
}

// SaveServerMetrics is an alias for SaveMetrics (for compatibility)
func (m *EPaxosManager) SaveServerMetrics() error {
	if m.perfM == nil {
		return fmt.Errorf("server performance meter not initialized")
	}
	log.Infof("[METRICS] Saving server %d metrics | Total operations: %d | Fast: %d | Slow: %d | Conflicts: %d",
		m.serverID, m.globalClock,
		m.perfM.FastCommits, m.perfM.SlowCommits, m.perfM.ConflictCommits)
	if err := m.perfM.SaveToFile(); err != nil {
		log.Errorf("[METRICS] Failed to save server metrics: %v", err)
		return err
	}
	log.Infof("[METRICS] Server %d metrics saved successfully to eval/server%d/", m.serverID, m.serverID)
	return nil
}

// ===== MEMORY CLEANUP (Production Safety) =====
// Lightweight cleanup for stale entries (runs every 30s)
func (m *EPaxosManager) cleanupStaleEntries() {
	m.inFlightMu.Lock()
	now := time.Now()
	cleanedCount := 0

	// Clean inFlight map
	for objID, cmd := range m.inFlight {
		// Remove entries older than 2 minutes (safety net)
		if !cmd.Timestamp.IsZero() && now.Sub(cmd.Timestamp) > 2*time.Minute {
			delete(m.inFlight, objID)
			cleanedCount++
		}
	}
	m.inFlightMu.Unlock()

	if cleanedCount > 0 {
		log.Debugf("[CLEANUP] Removed %d stale inFlight entries", cleanedCount)
	}
}
