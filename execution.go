package main

import (
	"fmt"
	"sort"
	"time"
)

// ExecuteCommand executes a command by building dependency graph and finding SCCs
func (m *EPaxosManager) ExecuteCommand(instanceID InstanceID) error {
	// Check if already executed
	if m.state.IsExecuted(instanceID) {
		return nil
	}
	
	log.Debugf("[Execute] Starting execution | Instance=%s", instanceID)
	
	// Build dependency graph
	graph, ready := m.buildDependencyGraph(instanceID)
	if !ready {
		// BUG FIX #4: Dependencies not all committed yet, skip and retry later
		log.Debugf("[Execute] Dependencies not ready | Instance=%s", instanceID)
		return fmt.Errorf("dependencies not ready")
	}
	
	// Find strongly connected components
	sccs := m.findSCCs(graph)
	
	// Topologically sort SCCs
	sorted := m.topologicalSort(sccs, graph)
	
	// Execute in order
	for _, scc := range sorted {
		m.executeSCC(scc)
	}
	
	return nil
}

// buildDependencyGraph builds the dependency graph starting from instanceID
// Returns (graph, allReady) where allReady=false means some deps not committed yet
func (m *EPaxosManager) buildDependencyGraph(startID InstanceID) (map[InstanceID]Dependencies, bool) {
	graph := make(map[InstanceID]Dependencies)
	visited := make(map[InstanceID]bool)
	allReady := true
	
	var build func(InstanceID) bool
	build = func(id InstanceID) bool {
		if visited[id] {
			return true
		}
		visited[id] = true
		
		inst := m.state.GetInstance(id)
		inst.RLock()
		deps := inst.Deps.Clone()
		status := inst.Status
		inst.RUnlock()
		
		// BUG FIX #4: CRITICAL - must be committed before we can build graph
		// Only include committed instances in dependency graph
		if status != COMMITTED && status != EXECUTED {
			log.Warnf("[Execute] Instance not committed yet | Instance=%s | Status=%v",
				id, status)
			allReady = false
			return false
		}
		
		graph[id] = deps
		
		// Recursively build dependencies
		for replicaID, instNo := range deps {
			if instNo >= 0 { // -1 means no dependency
				depID := InstanceID{replicaID, int(instNo)}
				if !build(depID) {
					return false
				}
			}
		}
		return true
	}
	
	build(startID)
	return graph, allReady
}

// findSCCs finds strongly connected components using Tarjan's algorithm
func (m *EPaxosManager) findSCCs(graph map[InstanceID]Dependencies) [][]InstanceID {
	index := 0
	stack := []InstanceID{}
	indices := make(map[InstanceID]int)
	lowlinks := make(map[InstanceID]int)
	onStack := make(map[InstanceID]bool)
	sccs := [][]InstanceID{}
	
	var strongConnect func(InstanceID)
	strongConnect = func(v InstanceID) {
		indices[v] = index
		lowlinks[v] = index
		index++
		stack = append(stack, v)
		onStack[v] = true
		
		// Consider successors
		for replicaID, instNo := range graph[v] {
			if instNo < 0 { // -1 means no dependency
				continue
			}
			w := InstanceID{replicaID, int(instNo)}
			if _, exists := indices[w]; !exists {
				strongConnect(w)
				if lowlinks[w] < lowlinks[v] {
					lowlinks[v] = lowlinks[w]
				}
			} else if onStack[w] {
				if indices[w] < lowlinks[v] {
					lowlinks[v] = indices[w]
				}
			}
		}
		
		// Root of SCC
		if lowlinks[v] == indices[v] {
			scc := []InstanceID{}
			for {
				w := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[w] = false
				scc = append(scc, w)
				if w == v {
					break
				}
			}
			sccs = append(sccs, scc)
		}
	}
	
	for v := range graph {
		if _, exists := indices[v]; !exists {
			strongConnect(v)
		}
	}
	
	return sccs
}

// topologicalSort sorts SCCs in topological order
// EPaxos paper: execute in inverse topological order (deps before dependents)
func (m *EPaxosManager) topologicalSort(sccs [][]InstanceID, graph map[InstanceID]Dependencies) [][]InstanceID {
	// Build SCC graph
	sccIndex := make(map[InstanceID]int)
	for i, scc := range sccs {
		for _, id := range scc {
			sccIndex[id] = i
		}
	}
	
	// Calculate in-degrees (FIXED: count edges TO each SCC, not FROM)
	// If node i depends on dep, that's an edge i → dep
	// We want to count incoming edges to each SCC (how many depend on it)
	// So we increment in-degree of the DEPENDENCY, not the dependent
	inDegree := make([]int, len(sccs))
	for i, scc := range sccs {
		for _, id := range scc {
			for replicaID, instNo := range graph[id] {
				if instNo < 0 { // -1 means no dependency
					continue
				}
				dep := InstanceID{replicaID, int(instNo)}
				depSCC := sccIndex[dep]
				if depSCC != i {
					// FIXED: Increment in-degree of DEPENDENCY (depSCC), not dependent (i)
					// This builds reverse graph for inverse topological order
					inDegree[depSCC]++
				}
			}
		}
	}
	
	// Topological sort
	sorted := [][]InstanceID{}
	queue := []int{}
	
	for i, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, i)
		}
	}
	
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		sorted = append(sorted, sccs[curr])
		
		// Update in-degrees: for each node in curr, find its dependencies
		// and decrement their in-degrees (we're "removing" the edge from curr to dep)
		for _, id := range sccs[curr] {
			for replicaID, instNo := range graph[id] {
				if instNo < 0 { // -1 means no dependency
					continue
				}
				dep := InstanceID{replicaID, int(instNo)}
				depSCC := sccIndex[dep]
				if depSCC != curr {
					inDegree[depSCC]--
					if inDegree[depSCC] == 0 {
						queue = append(queue, depSCC)
					}
				}
			}
		}
	}
	
	// Reverse to get correct execution order (inverse topological)
	for i := len(sorted)/2 - 1; i >= 0; i-- {
		opp := len(sorted) - 1 - i
		sorted[i], sorted[opp] = sorted[opp], sorted[i]
	}
	
	return sorted
}

// executeSCC executes commands in an SCC (sorted by seq number)
func (m *EPaxosManager) executeSCC(scc []InstanceID) {
	// Sort by sequence number
	sort.Slice(scc, func(i, j int) bool {
		instI := m.state.GetInstance(scc[i])
		instJ := m.state.GetInstance(scc[j])
		
		instI.RLock()
		seqI := instI.Seq
		instI.RUnlock()
		
		instJ.RLock()
		seqJ := instJ.Seq
		instJ.RUnlock()
		
		return seqI < seqJ
	})
	
	// Execute in order
	for _, id := range scc {
		if !m.state.IsExecuted(id) {
			m.executeInstance(id)
		}
	}
}

// executeInstance executes a single instance
func (m *EPaxosManager) executeInstance(id InstanceID) {
	inst := m.state.GetInstance(id)
	inst.Lock()
	
	if inst.Status == EXECUTED {
		inst.Unlock()
		return
	}
	
	log.Debugf("[Execute] Executing instance | Instance=%s | Seq=%d",
		id, inst.Seq)
	
	// Execute the command (application-specific)
	// For now, just mark as executed
	inst.Status = EXECUTED
	cmd := inst.Command
	inst.Unlock()
	
	m.state.MarkExecuted(id)
	
	log.Infof("[Execute] Instance executed | Instance=%s | Seq=%d | CmdType=%v",
		id, inst.Seq, cmd.CmdType)
	
	// EPaxos paper Section 7.3: Reply to client AFTER execution, not after commit
	// For batches with multiple instances, only reply when ALL instances execute
	m.repliesMu.Lock()
	replyInfo, hasPendingReply := m.pendingReplies[id]
	m.repliesMu.Unlock()
	
	if hasPendingReply {
		// Increment executed count atomically
		executedCount := int(replyInfo.ExecutedCount.Add(1))
		
		log.Debugf("[Execute] Batch progress | Instance=%s | Executed=%d/%d",
			id, executedCount, replyInfo.TotalInstances)
		
		// Only send reply when ALL instances in batch have executed
		if executedCount == replyInfo.TotalInstances {
			// All instances executed - clean up and send reply
			m.repliesMu.Lock()
			for _, instID := range replyInfo.InstanceIDs {
				delete(m.pendingReplies, instID)
			}
			m.repliesMu.Unlock()
			
			// Send reply after execution
			latency := time.Since(replyInfo.StartTime).Seconds() * 1000 // milliseconds
			reply := &ClientReply{
				Success:     true,
				PathUsed:    "EXECUTED", // Mark as executed
				Latency:     latency,
				ClientClock: replyInfo.ClientClock,
			}
			
			log.Infof("[Execute] Sending client reply (all %d instances done) | LastInstance=%s | ClientID=%d | ClientClock=%d | Latency=%.2fms",
				replyInfo.TotalInstances, id, replyInfo.ClientID, replyInfo.ClientClock, latency)
			
			// Non-blocking send (with timeout to avoid goroutine leak)
			select {
			case replyInfo.ReplyChan <- reply:
				// Reply sent successfully
			case <-time.After(5 * time.Second):
				log.Warnf("[Execute] Client reply timeout | Instance=%s | ClientID=%d", 
					id, replyInfo.ClientID)
			}
		}
	}
}