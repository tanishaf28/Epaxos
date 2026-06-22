package main

import (
	"fmt"
	"sync"
	"time"
)

// InstanceID uniquely identifies an instance across the cluster
type InstanceID struct {
	ReplicaID  int
	InstanceNo int
}

func (id InstanceID) String() string {
	return fmt.Sprintf("R%d.%d", id.ReplicaID, id.InstanceNo)
}

// Dependencies stores the highest conflicting instance per replica (original EPaxos design)
// deps[i] = highest instance number from replica i that conflicts with this command
// A value of -1 means no dependency on that replica
type Dependencies []int32

func NewDependencies(numReplicas int) Dependencies {
	deps := make(Dependencies, numReplicas)
	for i := range deps {
		deps[i] = -1 // No dependencies initially
	}
	return deps
}

func (d Dependencies) Clone() Dependencies {
	if d == nil {
		return nil
	}
	clone := make(Dependencies, len(d))
	copy(clone, d)
	return clone
}

// Union merges two dependency sets, taking the maximum instance per replica
func (d Dependencies) Union(other Dependencies) Dependencies {
	if d == nil {
		return other.Clone()
	}
	if other == nil {
		return d.Clone()
	}
	result := d.Clone()
	for i := 0; i < len(result) && i < len(other); i++ {
		if other[i] > result[i] {
			result[i] = other[i]
		}
	}
	return result
}

// Equal checks if two dependency sets are identical (for fast path)
func (d Dependencies) Equal(other Dependencies) bool {
	if len(d) != len(other) {
		return false
	}
	for i := 0; i < len(d); i++ {
		if d[i] != other[i] {
			return false
		}
	}
	return true
}

// Max returns the highest dependency instance number across all replicas
func (d Dependencies) Max() int32 {
	max := int32(-1)
	for _, inst := range d {
		if inst > max {
			max = inst
		}
	}
	return max
}

// Command represents a client command with object information
type Command struct {
	ClientID    int
	ClientClock int
	CmdType     CmdType

	// Object identification (for interference detection)
	ObjID    string   // Primary object ID
	ObjType  int      // IndependentObject | DependentObject
	ObjIDs   []string // All object IDs (for batches)
	ObjTypes []int    // All object types (for batches)
	IsMixed  bool     // True if batch contains multiple object types

	Payload   interface{} // [][]byte or []mongodb.Query
	Timestamp time.Time   // For cleanup of stale entries
}

// Instance represents a Paxos instance with EPaxos attributes
type Instance struct {
	sync.RWMutex

	InstanceID InstanceID
	Command    *Command

	// EPaxos attributes
	Seq    int
	Deps   Dependencies
	Status InstanceStatus
	Ballot int // For recovery

	// Metadata
	LeaderID  int
	Timestamp int64
}

func NewInstance(replicaID, instanceNo, numReplicas int) *Instance {
	return &Instance{
		InstanceID: InstanceID{replicaID, instanceNo},
		Deps:       NewDependencies(numReplicas),
		Status:     NONE,
		Ballot:     0,
	}
}

// EPaxosState manages all instances across all replicas
type EPaxosState struct {
	sync.RWMutex
	preAcceptMu sync.Mutex

	myID        int
	numReplicas int // Total number of replicas in cluster

	// EPaxos reference implementation: preallocated array per replica
	// instanceSpace[replicaID][instanceNo] = *Instance
	// This is O(1) access without map overhead or locking per access
	instanceSpace [][]*Instance // Preallocated arrays per replica

	maxInstance map[int]int         // Max instance number per replica
	executed    map[InstanceID]bool // Execution tracking
	commitIndex int                 // Number of committed instances

	// Per-key per-replica conflict tracking (EPaxos paper Section 4.5)
	// conflicts[replicaID][key] = highest instance number from that replica touching that key
	// This is O(1) lookup per key per replica, not O(all instances)
	conflicts []map[string]int32 // conflicts[replicaID][objKey] = instanceNo
}

func NewEPaxosState(myID int, numReplicas int) *EPaxosState {
	state := &EPaxosState{
		myID:          myID,
		numReplicas:   numReplicas,
		instanceSpace: make([][]*Instance, numReplicas),
		maxInstance:   make(map[int]int),
		executed:      make(map[InstanceID]bool),
		commitIndex:   0,
		conflicts:     make([]map[string]int32, numReplicas),
	}

	// Preallocate instance space per replica (EPaxos reference: 2*1024*1024)
	// For production, use smaller size or add dynamic growth
	const INSTANCE_SPACE_SIZE = 1024 * 1024 // 1M instances per replica

	for i := 0; i < numReplicas; i++ {
		state.maxInstance[i] = 0
		state.conflicts[i] = make(map[string]int32)
		state.instanceSpace[i] = make([]*Instance, INSTANCE_SPACE_SIZE)
	}

	return state
}

// AtomicPreAccept serializes dependency-read/register/seq-compute for PreAccept.
func (s *EPaxosState) AtomicPreAccept(fn func()) {
	s.preAcceptMu.Lock()
	defer s.preAcceptMu.Unlock()
	fn()
}

// GetNextInstance returns the next instance ID for this replica
func (s *EPaxosState) GetNextInstance() InstanceID {
	s.Lock()
	defer s.Unlock()

	instNo := s.maxInstance[s.myID]
	s.maxInstance[s.myID]++
	return InstanceID{s.myID, instNo}
}


func (s *EPaxosState) GetInstance(id InstanceID) *Instance {
	// Bounds check (can be done without lock since array dimensions are fixed)
	if id.ReplicaID < 0 || id.ReplicaID >= s.numReplicas {
		log.Errorf("[State] Invalid replica ID: %d", id.ReplicaID)
		return NewInstance(id.ReplicaID, id.InstanceNo, s.numReplicas)
	}

	if id.InstanceNo < 0 || id.InstanceNo >= len(s.instanceSpace[id.ReplicaID]) {
		log.Errorf("[State] Instance number out of bounds: %d (max %d)",
			id.InstanceNo, len(s.instanceSpace[id.ReplicaID])-1)
		return NewInstance(id.ReplicaID, id.InstanceNo, s.numReplicas)
	}

	// Fast path: read with read lock (allows concurrent reads)
	s.RLock()
	inst := s.instanceSpace[id.ReplicaID][id.InstanceNo]
	s.RUnlock()

	if inst != nil {
		return inst
	}

	// Slow path: need to create instance, acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check: another goroutine might have created it while we waited for write lock
	if s.instanceSpace[id.ReplicaID][id.InstanceNo] != nil {
		return s.instanceSpace[id.ReplicaID][id.InstanceNo]
	}

	// Create new instance
	inst = NewInstance(id.ReplicaID, id.InstanceNo, s.numReplicas)
	s.instanceSpace[id.ReplicaID][id.InstanceNo] = inst
	return inst
}

// SetInstance updates an instance (kept for API compatibility)
func (s *EPaxosState) SetInstance(id InstanceID, inst *Instance) {
	s.Lock()
	defer s.Unlock()

	if id.ReplicaID >= 0 && id.ReplicaID < s.numReplicas &&
		id.InstanceNo >= 0 && id.InstanceNo < len(s.instanceSpace[id.ReplicaID]) {
		s.instanceSpace[id.ReplicaID][id.InstanceNo] = inst
	}
}

// RegisterObjectAccess tracks object access by instance using per-key per-replica conflict table
// EPaxos paper approach: conflicts[replicaID][key] = highest instance touching that key
func (s *EPaxosState) RegisterObjectAccess(instanceID InstanceID, objIDs []string) {
	s.Lock()
	defer s.Unlock()

	for _, objID := range objIDs {
		// Update conflict table: store highest instance per key per replica
		if int32(instanceID.InstanceNo) > s.conflicts[instanceID.ReplicaID][objID] {
			s.conflicts[instanceID.ReplicaID][objID] = int32(instanceID.InstanceNo)
		}
	}
}

// GetInterferingInstances returns the highest conflicting instance per replica
// EPaxos paper Section 4.5: deps[r] = highest instance from replica r that conflicts
// Uses O(keys * replicas) conflict table lookup instead of O(all instances) scan
func (s *EPaxosState) GetInterferingInstances(cmd *Command) Dependencies {
	s.RLock()
	defer s.RUnlock()

	deps := NewDependencies(s.numReplicas)

	// Get all object IDs this command touches
	objIDs := cmd.ObjIDs
	if len(objIDs) == 0 && cmd.ObjID != "" {
		objIDs = []string{cmd.ObjID}
	}

	// For each key, check conflict table for each replica
	// O(keys * replicas) instead of O(all instances)
	for _, objID := range objIDs {
		for replicaID := 0; replicaID < s.numReplicas; replicaID++ {
			if instNo, exists := s.conflicts[replicaID][objID]; exists {
				// Keep highest instance per replica
				if instNo > deps[replicaID] {
					deps[replicaID] = instNo
				}
			}
		}
	}

	return deps
}

// GetMaxSeq returns the maximum sequence number from interfering instances
func (s *EPaxosState) GetMaxSeq(deps Dependencies) int {
	s.RLock()
	defer s.RUnlock()

	maxSeq := 0
	for replicaID, instNo := range deps {
		if instNo >= 0 { // -1 means no dependency
			instanceNo := int(instNo)
			if instanceNo < len(s.instanceSpace[replicaID]) {
				inst := s.instanceSpace[replicaID][instanceNo]
				if inst != nil {
					inst.RLock()
					if inst.Seq > maxSeq {
						maxSeq = inst.Seq
					}
					inst.RUnlock()
				}
			}
		}
	}

	return maxSeq
}

// UpdateCommitIndex increments commit counter
func (s *EPaxosState) UpdateCommitIndex(delta int) {
	s.Lock()
	defer s.Unlock()
	s.commitIndex += delta
}

// GetCommitIndex returns current commit index
func (s *EPaxosState) GetCommitIndex() int {
	s.RLock()
	defer s.RUnlock()
	return s.commitIndex
}

// commandsInterfere determines if two commands interfere based on object access
func commandsInterfere(cmd1, cmd2 *Command) bool {
	// Get all object IDs for both commands
	objs1 := cmd1.ObjIDs
	if len(objs1) == 0 && cmd1.ObjID != "" {
		objs1 = []string{cmd1.ObjID}
	}

	objs2 := cmd2.ObjIDs
	if len(objs2) == 0 && cmd2.ObjID != "" {
		objs2 = []string{cmd2.ObjID}
	}

	// Check if any objects overlap
	objSet := make(map[string]bool)
	for _, obj := range objs1 {
		objSet[obj] = true
	}

	for _, obj := range objs2 {
		if objSet[obj] {
			// Same object accessed
			// Both writes = interfere
			if cmd1.CmdType == WRITE && cmd2.CmdType == WRITE {
				return true
			}
			// Write and read = interfere
			if (cmd1.CmdType == WRITE && cmd2.CmdType == READ) ||
				(cmd1.CmdType == READ && cmd2.CmdType == WRITE) {
				return true
			}
		}
	}
	return false
}


func (s *EPaxosState) IsExecuted(id InstanceID) bool {
	s.RLock()
	defer s.RUnlock()
	return s.executed[id]
}
func (s *EPaxosState) MarkExecuted(id InstanceID) {
	s.Lock()
	defer s.Unlock()
	s.executed[id] = true

}
