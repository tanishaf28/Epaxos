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

// Dependencies is a set of instance IDs
type Dependencies map[InstanceID]bool

func (d Dependencies) Clone() Dependencies {
	clone := make(Dependencies)
	for k, v := range d {
		clone[k] = v
	}
	return clone
}

func (d Dependencies) Union(other Dependencies) Dependencies {
	result := d.Clone()
	for k, v := range other {
		result[k] = v
	}
	return result
}

// Command represents a client command with object information
type Command struct {
	ClientID    int
	ClientClock int
	CmdType     CmdType
	
	// Object identification (for interference detection)
	ObjID    string   // Primary object ID
	ObjType  int      // IndependentObject | CommonObject | HotObject
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
	Seq         int
	Deps        Dependencies
	Status      InstanceStatus
	Ballot      int // For recovery
	
	// Metadata
	LeaderID    int
	Timestamp   int64
}

func NewInstance(replicaID, instanceNo int) *Instance {
	return &Instance{
		InstanceID: InstanceID{replicaID, instanceNo},
		Deps:       make(Dependencies),
		Status:     NONE,
		Ballot:     0,
	}
}

// EPaxosState manages all instances across all replicas
type EPaxosState struct {
	sync.RWMutex
	
	myID           int
	instances      map[InstanceID]*Instance // All instances from all replicas
	maxInstance    map[int]int              // Max instance number per replica
	executed       map[InstanceID]bool      // Execution tracking
	commitIndex    int                      // Number of committed instances
	
	// Object tracking for interference detection
	objectAccess   map[string][]InstanceID  // Track which instances touched each object
}

func NewEPaxosState(myID int, numReplicas int) *EPaxosState {
	state := &EPaxosState{
		myID:         myID,
		instances:    make(map[InstanceID]*Instance),
		maxInstance:  make(map[int]int),
		executed:     make(map[InstanceID]bool),
		commitIndex:  0,
		objectAccess: make(map[string][]InstanceID),
	}
	
	// Initialize max instance counters
	for i := 0; i < numReplicas; i++ {
		state.maxInstance[i] = 0
	}
	
	return state
}

// GetNextInstance returns the next instance ID for this replica
func (s *EPaxosState) GetNextInstance() InstanceID {
	s.Lock()
	defer s.Unlock()
	
	s.maxInstance[s.myID]++
	return InstanceID{s.myID, s.maxInstance[s.myID]}
}

// GetInstance returns an instance, creating if necessary
func (s *EPaxosState) GetInstance(id InstanceID) *Instance {
	s.Lock()
	defer s.Unlock()
	
	inst, exists := s.instances[id]
	if !exists {
		inst = NewInstance(id.ReplicaID, id.InstanceNo)
		s.instances[id] = inst
	}
	return inst
}

// SetInstance updates an instance
func (s *EPaxosState) SetInstance(id InstanceID, inst *Instance) {
	s.Lock()
	defer s.Unlock()
	s.instances[id] = inst
}

// RegisterObjectAccess tracks object access by instance
func (s *EPaxosState) RegisterObjectAccess(instanceID InstanceID, objIDs []string) {
	s.Lock()
	defer s.Unlock()
	
	for _, objID := range objIDs {
		s.objectAccess[objID] = append(s.objectAccess[objID], instanceID)
	}
}

// GetInterferingInstances returns instances with commands that interfere with cmd
// Based on object IDs: commands interfere if they touch the same objects
func (s *EPaxosState) GetInterferingInstances(cmd *Command) Dependencies {
	s.RLock()
	defer s.RUnlock()
	
	deps := make(Dependencies)
	
	// Get all object IDs this command touches
	objIDs := cmd.ObjIDs
	if len(objIDs) == 0 && cmd.ObjID != "" {
		objIDs = []string{cmd.ObjID}
	}
	
	// Find all instances that touched these objects
	seenInstances := make(map[InstanceID]bool)
	for _, objID := range objIDs {
		if instances, exists := s.objectAccess[objID]; exists {
			for _, instID := range instances {
				if !seenInstances[instID] {
					seenInstances[instID] = true
					
					// Check if this instance's command interferes
					if inst, exists := s.instances[instID]; exists {
						inst.RLock()
						if inst.Command != nil && inst.Status != NONE {
							if commandsInterfere(cmd, inst.Command) {
								deps[instID] = true
							}
						}
						inst.RUnlock()
					}
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
	for id := range deps {
		if inst, exists := s.instances[id]; exists {
			inst.RLock()
			if inst.Seq > maxSeq {
				maxSeq = inst.Seq
			}
			inst.RUnlock()
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
	
	// No overlapping objects or both reads = don't interfere
	return false
}

// IsExecuted checks if instance has been executed
func (s *EPaxosState) IsExecuted(id InstanceID) bool {
	s.RLock()
	defer s.RUnlock()
	return s.executed[id]
}

// MarkExecuted marks instance as executed
func (s *EPaxosState) MarkExecuted(id InstanceID) {
	s.Lock()
	defer s.Unlock()
	s.executed[id] = true
}