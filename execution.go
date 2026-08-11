package main

import (
	"epaxos/mongodb"
	"fmt"
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
	graph, ready := m.buildDependencyGraph(instanceID)
	if !ready {
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

	// Build edges with direction dependency -> dependent.
	// If SCC i depends on depSCC, we add edge depSCC -> i so dep executes first.
	inDegree := make([]int, len(sccs))
	edges := make([][]int, len(sccs))
	seenEdges := make(map[[2]int]struct{})

	for i, scc := range sccs {
		for _, id := range scc {
			for replicaID, instNo := range graph[id] {
				if instNo < 0 { // -1 means no dependency
					continue
				}
				dep := InstanceID{replicaID, int(instNo)}
				depSCC := sccIndex[dep]
				if depSCC != i {
					edge := [2]int{depSCC, i}
					if _, exists := seenEdges[edge]; exists {
						continue
					}
					seenEdges[edge] = struct{}{}
					inDegree[i]++
					edges[depSCC] = append(edges[depSCC], i)
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

		for _, dependent := range edges[curr] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	return sorted
}

// executeSCC executes commands in an SCC (sorted by seq number, ties broken
// deterministically by replica ID then instance number — paper §2.2: "ties
// broken in a deterministic manner (e.g. by replica id)". Without this,
// sort.Slice's instability plus findSCCs's map-iteration order (randomized
// per the Go spec) means two same-seq instances in a conflicting SCC could
// execute in a different relative order on different replicas.
func (m *EPaxosManager) executeSCC(scc []InstanceID) {
	sort.Slice(scc, func(i, j int) bool {
		instI := m.state.GetInstance(scc[i])
		instJ := m.state.GetInstance(scc[j])

		instI.RLock()
		seqI := instI.Seq
		instI.RUnlock()

		instJ.RLock()
		seqJ := instJ.Seq
		instJ.RUnlock()

		if seqI != seqJ {
			return seqI < seqJ
		}
		if scc[i].ReplicaID != scc[j].ReplicaID {
			return scc[i].ReplicaID < scc[j].ReplicaID
		}
		return scc[i].InstanceNo < scc[j].InstanceNo
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

	seq := inst.Seq
	cmd := inst.Command
	inst.Unlock()

	if err := m.applyCommandPayload(cmd); err != nil {
		log.Errorf("[Execute] Command apply failed | Instance=%s | Error=%v", id, err)
		return
	}

	inst.Lock()
	if inst.Status == EXECUTED {
		inst.Unlock()
		return
	}
	inst.Status = EXECUTED
	inst.Unlock()

	m.state.MarkExecuted(id)

	if cmd != nil {
		log.Infof("[Execute] Instance executed | Instance=%s | Seq=%d | CmdType=%v",
			id, seq, cmd.CmdType)
	} else {
		log.Warnf("[Execute] Instance executed without command payload | Instance=%s | Seq=%d",
			id, seq)
	}
}

func (m *EPaxosManager) applyCommandPayload(cmd *Command) error {
	if cmd == nil {
		return nil
	}

	if evalType != MongoDB {
		return nil
	}

	queries, ok := cmd.Payload.([]mongodb.Query)
	if !ok {
		return fmt.Errorf("unexpected MongoDB payload type %T", cmd.Payload)
	}

	if len(queries) == 0 {
		return nil
	}

	if mongoDbFollower == nil {
		return fmt.Errorf("mongodb follower is not initialized")
	}

	_, _, err := mongoDbFollower.FollowerAPI(queries)
	if err != nil {
		return fmt.Errorf("mongodb follower query execution failed: %w", err)
	}

	return nil
}
