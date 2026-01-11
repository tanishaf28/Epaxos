package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"epaxos/eval"
	"epaxos/mongodb"
)

type EPaxosManager struct {
	sync.RWMutex
	
	state         *EPaxosState
	serverID      int
	globalClock   int64
	perfM         *eval.PerfMeter
	committedUpTo []int32
	inFlight      map[string]*Command  // Track in-flight commands for deduplication
	inFlightMu    sync.Mutex
}

func NewEPaxosManager(serverID, numReplicas int) *EPaxosManager {
	mgr := &EPaxosManager{
		state:         NewEPaxosState(serverID, numReplicas),
		serverID:      serverID,
		globalClock:   0,
		committedUpTo: make([]int32, numReplicas),
		inFlight:      make(map[string]*Command),
	}
	for i := 0; i < numReplicas; i++ {
		mgr.committedUpTo[i] = -1
	}
	
	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_epaxos", serverID, numOfServers, threshold, batchsize)
	mgr.perfM = &eval.PerfMeter{}
	mgr.perfM.Init(1, batchsize, fileName)
	
	return mgr
}

func (m *EPaxosManager) IncrementClock() int64 {
	return atomic.AddInt64(&m.globalClock, 1)
}

// ============== FIXED: Proper Batch Handling ==============

func (m *EPaxosManager) HandleCommand(cmd *Command) (bool, string) {
	if cmd.IsMixed {
		return m.handleMixedBatch(cmd)
	}
	
	// Single object type batch
	return m.handleSingleTypeBatch(cmd)
}

// handleMixedBatch processes each operation in a mixed batch according to its object type
func (m *EPaxosManager) handleMixedBatch(cmd *Command) (bool, string) {
	if len(cmd.ObjIDs) == 0 {
		log.Errorf("[MixedBatch] Empty ObjIDs array")
		return false, "ERROR"
	}
	
	globalClock := int(m.IncrementClock())
	m.perfM.RecordStarter(globalClock)
	defer m.perfM.RecordFinisher(globalClock)
	
	log.Infof("[MixedBatch] Processing %d operations | ClientClock=%d",
		len(cmd.ObjIDs), cmd.ClientClock)
	
	// Track per-type results
	fastOps := 0
	slowOps := 0
	hotOps := 0
	
	// Process each operation in the batch
	for i := 0; i < len(cmd.ObjIDs); i++ {
		objID := cmd.ObjIDs[i]
		objType := cmd.ObjTypes[i]
		
		// Create single command for this operation with timestamp
		singleCmd := &Command{
			ClientID:    cmd.ClientID,
			ClientClock: cmd.ClientClock,
			CmdType:     cmd.CmdType,
			ObjID:       objID,
			ObjType:     objType,
			ObjIDs:      []string{objID},
			ObjTypes:    []int{objType},
			IsMixed:     false,
			Timestamp:   time.Now(),
		}
		
		// Extract payload for this operation
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
		

		var success bool

		switch objType {
		case HotObject:
			// Hot objects always use slow path for serialization
			success, _ = m.runSlowPath(m.state.GetNextInstance(), singleCmd)
			if success {
				hotOps++
				atomic.AddInt64(&m.perfM.ConflictCommits, 1)
				m.perfM.IncConflict(globalClock)
			}
			
		case CommonObject:
			// Common objects use slow path
			success, _ = m.runSlowPath(m.state.GetNextInstance(), singleCmd)
			if success {
				slowOps++
				atomic.AddInt64(&m.perfM.SlowCommits, 1)
				m.perfM.IncSlowPath(globalClock)
			}
			
		case IndependentObject:
			// Independent objects try fast path first
			instanceID := m.state.GetNextInstance()
			success, _ = m.runFastPath(instanceID, singleCmd)
			
			if !success {
				// Fast path failed, try slow path
				m.perfM.RecordFastPathFallback()
				success, _ = m.runSlowPath(instanceID, singleCmd)
				if success {
					slowOps++
					atomic.AddInt64(&m.perfM.SlowCommits, 1)
					m.perfM.IncSlowPath(globalClock)
				}
			} else {
				fastOps++
				atomic.AddInt64(&m.perfM.FastCommits, 1)
				m.perfM.IncFastPath(globalClock)
			}
		}
		
		if !success {
			log.Warnf("[MixedBatch] Operation %d/%d failed | ObjID=%s | ObjType=%d",
				i+1, len(cmd.ObjIDs), objID, objType)
		}
	}
	
	// Build result string matching WOC format
	pathUsed := fmt.Sprintf("MIXED(FAST:%d,SLOW:%d,HOT:%d)", fastOps, slowOps, hotOps)
	
	log.Infof("[MixedBatch] Complete | Fast=%d | Slow=%d | Hot=%d | Total=%d",
		fastOps, slowOps, hotOps, len(cmd.ObjIDs))
	
	allSuccess := (fastOps + slowOps + hotOps) == len(cmd.ObjIDs)
	return allSuccess, pathUsed
}

// handleSingleTypeBatch processes a batch where all operations have the same object type
func (m *EPaxosManager) handleSingleTypeBatch(cmd *Command) (bool, string) {
	globalClock := int(m.IncrementClock())
	m.perfM.RecordStarter(globalClock)
	defer m.perfM.RecordFinisher(globalClock)
	
	batchSize := len(cmd.ObjIDs)
	if batchSize == 0 {
		batchSize = 1
	}
	
	instanceID := m.state.GetNextInstance()
	
	log.Infof("[SingleTypeBatch] Starting | Instance=%s | ObjType=%d | BatchSize=%d | ClientClock=%d",
		instanceID, cmd.ObjType, batchSize, cmd.ClientClock)
	
	var success bool
	var path string
	
	// Route based on object type
	switch cmd.ObjType {
	case HotObject:
		log.Debugf("[HOT] Routing to SLOW path | ObjID=%s", cmd.ObjID)
		success, path = m.runSlowPath(instanceID, cmd)
		if success {
			atomic.AddInt64(&m.perfM.SlowCommits, int64(batchSize))
			atomic.AddInt64(&m.perfM.ConflictCommits, int64(batchSize))
			for i := 0; i < batchSize; i++ {
				m.perfM.IncConflict(globalClock)
			}
		}
		
	case CommonObject:
		log.Debugf("[COMMON] Routing to SLOW path | ObjID=%s", cmd.ObjID)
		success, path = m.runSlowPath(instanceID, cmd)
		if success {
			atomic.AddInt64(&m.perfM.SlowCommits, int64(batchSize))
			for i := 0; i < batchSize; i++ {
				m.perfM.IncSlowPath(globalClock)
			}
		}
		
	case IndependentObject:
		log.Debugf("[INDEP] Trying FAST path | ObjID=%s", cmd.ObjID)
		success, path = m.runFastPath(instanceID, cmd)
		
		if !success {
			log.Warnf("[INDEP] Fast path failed, trying slow path | Instance=%s", instanceID)
			m.perfM.RecordFastPathFallback()
			success, path = m.runSlowPath(instanceID, cmd)
			
			if success {
				atomic.AddInt64(&m.perfM.SlowCommits, int64(batchSize))
				for i := 0; i < batchSize; i++ {
					m.perfM.IncSlowPath(globalClock)
				}
			}
		} else {
			atomic.AddInt64(&m.perfM.FastCommits, int64(batchSize))
			for i := 0; i < batchSize; i++ {
				m.perfM.IncFastPath(globalClock)
			}
		}
	}
	
	if success {
		m.state.UpdateCommitIndex(1)
	}
	
	log.Infof("[SingleTypeBatch] Complete | Instance=%s | Path=%s | Success=%v | BatchSize=%d",
		instanceID, path, success, batchSize)
	
	return success, path
}

// ============== FIXED: Added Error Logging ==============

func (m *EPaxosManager) runFastPath(instanceID InstanceID, cmd *Command) (bool, string) {
	start := time.Now()
	
	deps := m.state.GetInterferingInstances(cmd)
	seq := m.state.GetMaxSeq(deps) + 1
	
	log.Debugf("[FastPath] Initial | Instance=%s | Seq=%d | Deps=%d",
		instanceID, seq, len(deps))
	
	inst := m.state.GetInstance(instanceID)
	inst.Lock()
	inst.Command = cmd
	inst.Seq = seq
	inst.Deps = deps.Clone()
	inst.Status = PREACCEPTED
	inst.Ballot = 0
	
	objIDs := cmd.ObjIDs
	if len(objIDs) == 0 && cmd.ObjID != "" {
		objIDs = []string{cmd.ObjID}
	}
	m.state.RegisterObjectAccess(instanceID, objIDs)
	inst.Unlock()
	
	args := &PreAcceptArgs{
		InstanceID: instanceID,
		Command:    cmd,
		Seq:        seq,
		Deps:       deps.Clone(),
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
	
	log.Infof("[FastPath] Broadcasting PreAccept to %d replicas | Instance=%s", 
		len(connList), instanceID)
	
	for _, conn := range connList {
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
					responses <- reply
					log.Infof("[FastPath] PreAccept SUCCESS | Replica=%d | Instance=%s",
						c.replicaID, instanceID)
				} else if err != nil {
					log.Errorf("[FastPath] PreAccept RPC FAILED | Replica=%d | Instance=%s | Error=%v",
						c.replicaID, instanceID, err)
				} else {
					log.Warnf("[FastPath] PreAccept returned OK=false | Replica=%d | Instance=%s",
						c.replicaID, instanceID)
				}
			case <-time.After(2 * time.Second):
				log.Warnf("[FastPath] PreAccept timeout | Replica=%d | Instance=%s", 
					c.replicaID, instanceID)
			}
		}(conn)
	}
	
	go func() {
		wg.Wait()
		close(responses)
	}()
	
	replyCount := 1
	allEqual := true
	
	committedDeps := make([]int32, numOfServers)
	m.RLock()
	copy(committedDeps, m.committedUpTo)
	m.RUnlock()
	
	mergedSeq := seq
	mergedDeps := deps.Clone()
	
	for reply := range responses {
		replyCount++
		
		oldSeq := mergedSeq
		oldDeps := len(mergedDeps)
		
		if reply.Seq > mergedSeq {
			mergedSeq = reply.Seq
		}
		mergedDeps = mergedDeps.Union(reply.Deps)
		
		if reply.Seq != oldSeq || len(reply.Deps) != oldDeps {
			allEqual = false
		}
		
		for i, committed := range reply.CommittedUpTo {
			if committed > committedDeps[i] {
				committedDeps[i] = committed
			}
		}
		
		if replyCount >= fastQuorum && !allEqual {
			break
		}
	}
	
	latency := time.Since(start).Milliseconds()
	
	if replyCount < fastQuorum {
		log.Warnf("[FastPath] Insufficient responses | Instance=%s | Got=%d | Need=%d",
			instanceID, replyCount, fastQuorum)
		
		inst.Lock()
		inst.Seq = mergedSeq
		inst.Deps = mergedDeps
		inst.Unlock()
		
		return false, "FAST"
	}
	
	if !allEqual {
		log.Debugf("[FastPath] No unanimity | Instance=%s", instanceID)
		
		inst.Lock()
		inst.Seq = mergedSeq
		inst.Deps = mergedDeps
		inst.Unlock()
		
		return false, "FAST"
	}
	
	allCommitted := true
	for depID := range mergedDeps {
		if int32(depID.InstanceNo) > committedDeps[depID.ReplicaID] {
			allCommitted = false
			break
		}
	}
	
	if !allCommitted {
		log.Debugf("[FastPath] Uncommitted dependencies | Instance=%s", instanceID)
		
		inst.Lock()
		inst.Seq = mergedSeq
		inst.Deps = mergedDeps
		inst.Unlock()
		
		return false, "FAST"
	}
	
	if inst.Ballot != 0 {
		log.Debugf("[FastPath] Non-initial ballot | Instance=%s", instanceID)
		
		inst.Lock()
		inst.Seq = mergedSeq
		inst.Deps = mergedDeps
		inst.Unlock()
		
		return false, "FAST"
	}
	
	inst.Lock()
	inst.Seq = mergedSeq
	inst.Deps = mergedDeps
	inst.Status = COMMITTED
	inst.Unlock()
	
	m.state.SetInstance(instanceID, inst)
	m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)
	
	log.Infof("[FastPath] SUCCESS | Instance=%s | Seq=%d | Deps=%d | Latency=%dms",
		instanceID, mergedSeq, len(mergedDeps), latency)
	
	go m.broadcastCommit(instanceID, mergedSeq, mergedDeps)
	
	return true, "FAST"
}

func (m *EPaxosManager) updateCommittedUpTo(replicaID int, instanceNo int) {
	m.Lock()
	defer m.Unlock()
	
	if int32(instanceNo) > m.committedUpTo[replicaID] {
		m.committedUpTo[replicaID] = int32(instanceNo)
	}
}

func (m *EPaxosManager) runSlowPath(instanceID InstanceID, cmd *Command) (bool, string) {
	start := time.Now()
	
	inst := m.state.GetInstance(instanceID)
	inst.RLock()
	seq := inst.Seq
	deps := inst.Deps.Clone()
	inst.RUnlock()
	
	log.Debugf("[SlowPath] Starting Accept | Instance=%s | Seq=%d | Deps=%d",
		instanceID, seq, len(deps))
	
	inst.Lock()
	inst.Status = ACCEPTED
	inst.Unlock()
	
	args := &AcceptArgs{
		InstanceID: instanceID,
		Seq:        seq,
		Deps:       deps,
		LeaderID:   m.serverID,
	}
	
	responses := make(chan *AcceptReply, numOfServers)
	var wg sync.WaitGroup
	
	conns.RLock()
	connList := make([]*ServerConnection, 0, len(conns.m))
	for _, conn := range conns.m {
		connList = append(connList, conn)
	}
	conns.RUnlock()
	
	log.Infof("[SlowPath] Broadcasting Accept to %d replicas | Instance=%s", 
		len(connList), instanceID)
	
	for _, conn := range connList {
		wg.Add(1)
		go func(c *ServerConnection) {
			defer wg.Done()
			reply := &AcceptReply{}
			
			done := make(chan error, 1)
			go func() {
				done <- c.rpcClient.Call("EPaxosService.Accept", args, reply)
			}()
			
			select {
			case err := <-done:
				if err == nil && reply.OK {
					responses <- reply
					log.Infof("[SlowPath] Accept SUCCESS | Replica=%d | Instance=%s",
						c.replicaID, instanceID)
				} else if err != nil {
					log.Errorf("[SlowPath] Accept RPC FAILED | Replica=%d | Instance=%s | Error=%v",
						c.replicaID, instanceID, err)
				} else {
					log.Warnf("[SlowPath] Accept returned OK=false | Replica=%d | Instance=%s",
						c.replicaID, instanceID)
				}
			case <-time.After(3 * time.Second):
				log.Warnf("[SlowPath] Accept timeout | Replica=%d | Instance=%s", 
					c.replicaID, instanceID)
			}
		}(conn)
	}
	
	go func() {
		wg.Wait()
		close(responses)
	}()
	
	acceptCount := 1
	for range responses {
		acceptCount++
		if acceptCount >= slowQuorum {
			break
		}
	}
	
	latency := time.Since(start).Milliseconds()
	
	if acceptCount < slowQuorum {
		log.Warnf("[SlowPath] Insufficient accepts | Instance=%s | Got=%d | Need=%d",
			instanceID, acceptCount, slowQuorum)
		return false, "SLOW"
	}
	
	inst.Lock()
	inst.Status = COMMITTED
	inst.Unlock()
	
	m.state.SetInstance(instanceID, inst)
	m.updateCommittedUpTo(instanceID.ReplicaID, instanceID.InstanceNo)
	
	log.Infof("[SlowPath] SUCCESS | Instance=%s | Seq=%d | Deps=%d | Latency=%dms",
		instanceID, seq, len(deps), latency)
	
	go m.broadcastCommit(instanceID, seq, deps)
	
	return true, "SLOW"
}

func (m *EPaxosManager) broadcastCommit(instanceID InstanceID, seq int, deps Dependencies) {
	args := &CommitArgs{
		InstanceID: instanceID,
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

func (m *EPaxosManager) SaveMetrics() error {
	if m.perfM == nil {
		return fmt.Errorf("performance meter not initialized")
	}
	return m.perfM.SaveToFile()
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