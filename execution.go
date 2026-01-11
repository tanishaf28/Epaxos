package main

import (
	"sort"
)

// ExecuteCommand executes a command by building dependency graph and finding SCCs
func (m *EPaxosManager) ExecuteCommand(instanceID InstanceID) error {
	// Check if already executed
	if m.state.IsExecuted(instanceID) {
		return nil
	}
	
	log.Debugf("[Execute] Starting execution | Instance=%s", instanceID)
	
	// Build dependency graph
	graph := m.buildDependencyGraph(instanceID)
	
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
func (m *EPaxosManager) buildDependencyGraph(startID InstanceID) map[InstanceID]Dependencies {
	graph := make(map[InstanceID]Dependencies)
	visited := make(map[InstanceID]bool)
	
	var build func(InstanceID)
	build = func(id InstanceID) {
		if visited[id] {
			return
		}
		visited[id] = true
		
		inst := m.state.GetInstance(id)
		inst.RLock()
		deps := inst.Deps.Clone()
		status := inst.Status
		inst.RUnlock()
		
		// Only include committed instances
		if status != COMMITTED && status != EXECUTED {
			log.Warnf("[Execute] Instance not committed yet | Instance=%s | Status=%v",
				id, status)
			return
		}
		
		graph[id] = deps
		
		// Recursively build dependencies
		for depID := range deps {
			build(depID)
		}
	}
	
	build(startID)
	return graph
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
		for w := range graph[v] {
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
func (m *EPaxosManager) topologicalSort(sccs [][]InstanceID, graph map[InstanceID]Dependencies) [][]InstanceID {
	// Build SCC graph
	sccIndex := make(map[InstanceID]int)
	for i, scc := range sccs {
		for _, id := range scc {
			sccIndex[id] = i
		}
	}
	
	// Calculate in-degrees
	inDegree := make([]int, len(sccs))
	for i, scc := range sccs {
		for _, id := range scc {
			for dep := range graph[id] {
				depSCC := sccIndex[dep]
				if depSCC != i {
					inDegree[i]++
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
		
		// Update in-degrees
		for _, id := range sccs[curr] {
			for dep := range graph[id] {
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
	
	// Reverse to get correct execution order
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
	defer inst.Unlock()
	
	if inst.Status == EXECUTED {
		return
	}
	
	log.Debugf("[Execute] Executing instance | Instance=%s | Seq=%d",
		id, inst.Seq)
	
	// Execute the command (application-specific)
	// For now, just mark as executed
	inst.Status = EXECUTED
	m.state.MarkExecuted(id)
	
	log.Infof("[Execute] Instance executed | Instance=%s | Seq=%d | CmdType=%v",
		id, inst.Seq, inst.Command.CmdType)
}