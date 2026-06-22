package main

import (
	"epaxos/mongodb"
	"errors"
	"fmt"
	"time"
)

type EPaxosService struct{}

func NewEPaxosService() *EPaxosService {
	return &EPaxosService{}
}

// RPC Message Types
type PreAcceptArgs struct {
	InstanceID InstanceID
	Command    *Command
	Seq        int
	Deps       Dependencies
	Ballot     int
	LeaderID   int
}

type PreAcceptReply struct {
	OK            bool
	Seq           int
	Deps          Dependencies
	Ballot        int
	ReplicaID     int
	CommittedUpTo []int32
	IsPreAcceptOK bool // True if this is a lightweight OK (attributes unchanged)
}

// PreAcceptOK is a lightweight reply when attributes don't change (original EPaxos optimization)
type PreAcceptOK struct {
	InstanceID InstanceID
	ReplicaID  int
}

type AcceptArgs struct {
	InstanceID InstanceID
	Command    *Command
	Seq        int
	Deps       Dependencies
	LeaderID   int
	Ballot     int // Include ballot for recovery Accept phase
}

type AcceptReply struct {
	OK        bool
	ReplicaID int
}

type PrepareArgs struct {
	InstanceID InstanceID
	Ballot     int
	LeaderID   int
}

type PrepareReply struct {
	OK        bool
	Ballot    int
	Status    InstanceStatus
	Seq       int
	Deps      Dependencies
	Command   *Command
	ReplicaID int
}

type CommitArgs struct {
	InstanceID InstanceID
	Command    *Command // Include command for proper recovery and execution
	Seq        int
	Deps       Dependencies
}

type CommitReply struct {
	OK bool
}

// Client RPC structures (matching WOC pattern)
type ClientArgs struct {
	ClientID    int
	ClientClock int
	ObjID       string
	ObjType     int
	Type        int // PlainMsg, MongoDB
	CmdType     CmdType
	CmdPlain    [][]byte
	CmdMongo    []mongodb.Query
	IsMixed     bool
	ObjIDs      []string
	ObjTypes    []int
}

type ClientReply struct {
	Success     bool
	PathUsed    string
	ErrorMsg    error
	Latency     float64
	ClientClock int
}

// PreAccept handles PreAccept phase requests
// Critical distinction per EPaxos paper: send PreAcceptOK if attributes unchanged, PreAcceptReply otherwise
func (s *EPaxosService) PreAccept(args *PreAcceptArgs, reply *PreAcceptReply) error {
	inst := epaxosMgr.state.GetInstance(args.InstanceID)

	inst.Lock()

	// Reject stale PreAccept ballots
	if args.Ballot < inst.Ballot {
		reply.OK = false
		reply.Seq = inst.Seq
		reply.Deps = inst.Deps.Clone()
		reply.Ballot = inst.Ballot
		reply.ReplicaID = epaxosMgr.serverID
		inst.Unlock()

		epaxosMgr.RLock()
		reply.CommittedUpTo = make([]int32, numOfServers)
		copy(reply.CommittedUpTo, epaxosMgr.committedUpTo)
		epaxosMgr.RUnlock()

		return nil
	}

	if args.Ballot > inst.Ballot {
		inst.Ballot = args.Ballot
	}

	// Set command first
	inst.Command = args.Command
	inst.Status = PREACCEPTED
	inst.Unlock() // ← Release inst lock BEFORE touching state

	objIDs := args.Command.ObjIDs
	if len(objIDs) == 0 && args.Command.ObjID != "" {
		objIDs = []string{args.Command.ObjID}
	}

	// Keep dependency read + registration + seq computation atomic per replica.
	var localDeps Dependencies
	var localSeq int
	epaxosMgr.state.AtomicPreAccept(func() {
		// Read deps BEFORE registering self to avoid self-dependency.
		localDeps = epaxosMgr.state.GetInterferingInstances(args.Command)
		epaxosMgr.state.RegisterObjectAccess(args.InstanceID, objIDs)
		localSeq = epaxosMgr.state.GetMaxSeq(localDeps) + 1
	})

	// Merge with proposed attributes
	finalDeps := args.Deps.Union(localDeps)
	finalSeq := args.Seq
	if localSeq > finalSeq {
		finalSeq = localSeq
	}

	// Re-acquire to update instance attributes
	inst.Lock()
	inst.Seq = finalSeq
	inst.Deps = finalDeps
	attributesChanged := (finalSeq != args.Seq) || !finalDeps.Equal(args.Deps)
	// EPaxos: Any replica can propose any instance, so only check ballot==0, not leader==replica
	isInitialBallot := (args.Ballot == 0)

	// Build full reply (always fill in for fallback)
	reply.OK = true
	reply.Seq = finalSeq
	reply.Deps = finalDeps
	reply.Ballot = inst.Ballot
	reply.ReplicaID = epaxosMgr.serverID
	reply.IsPreAcceptOK = false // Default to full reply
	inst.Unlock()               // ← Release before accessing epaxosMgr

	// Include committed instances per replica
	epaxosMgr.RLock()
	reply.CommittedUpTo = make([]int32, numOfServers)
	copy(reply.CommittedUpTo, epaxosMgr.committedUpTo)
	epaxosMgr.RUnlock()
	uncommittedDeps := false
	epaxosMgr.RLock()
	for replicaID, instNo := range finalDeps {
		if instNo >= 0 && instNo > epaxosMgr.committedUpTo[replicaID] {
			uncommittedDeps = true
			break
		}
	}
	epaxosMgr.RUnlock()

	// Paper Section 4.4: Send lightweight PreAcceptOK if ALL conditions met:
	// 1. Attributes unchanged
	// 2. Initial ballot
	// 3. All dependencies committed (for optimized recovery)
	if !attributesChanged && isInitialBallot && !uncommittedDeps {
		reply.IsPreAcceptOK = true
		reply.Seq = 0
		reply.Deps = nil
		log.Debugf("[PreAccept-RPC] SendingOK | Instance=%s | Replica=%d | AttributesUnchanged=%v | InitialBallot=%v | UncommittedDeps=%v",
			args.InstanceID, epaxosMgr.serverID, !attributesChanged, isInitialBallot, uncommittedDeps)
	} else {
		log.Debugf("[PreAccept-RPC] SendingReply | Instance=%s | Changed=%v | InitBallot=%v | UncommittedDeps=%v",
			args.InstanceID, attributesChanged, isInitialBallot, uncommittedDeps)
	}

	return nil
}

// Accept handles Accept phase requests
func (s *EPaxosService) Accept(args *AcceptArgs, reply *AcceptReply) error {
	log.Debugf("[Accept-RPC] Instance=%s | Seq=%d | Deps=%d | Ballot=%d",
		args.InstanceID, args.Seq, len(args.Deps), args.Ballot)

	inst := epaxosMgr.state.GetInstance(args.InstanceID)

	inst.Lock()
	defer inst.Unlock()

	// CRITICAL: Reject stale Accept ballots (safety requirement for recovery)
	if args.Ballot < inst.Ballot {
		reply.OK = false
		reply.ReplicaID = epaxosMgr.serverID
		log.Debugf("[Accept-RPC] Rejected stale ballot %d < %d | Instance=%s",
			args.Ballot, inst.Ballot, args.InstanceID)
		return nil
	}

	// Update to accepted
	inst.Ballot = args.Ballot
	if args.Command != nil {
		inst.Command = args.Command
	}
	inst.Seq = args.Seq
	inst.Deps = args.Deps
	inst.Status = ACCEPTED

	reply.OK = true
	reply.ReplicaID = epaxosMgr.serverID

	return nil
}

// Prepare handles Prepare phase for EPaxos recovery (EPaxos paper Figure 3)
// This is the first phase of Explicit Prepare recovery
func (s *EPaxosService) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	log.Debugf("[Prepare-RPC] Instance=%s | Ballot=%d", args.InstanceID, args.Ballot)

	inst := epaxosMgr.state.GetInstance(args.InstanceID)

	inst.Lock()
	defer inst.Unlock()

	// Reject if we've already promised to a higher ballot
	if args.Ballot <= inst.Ballot {
		reply.OK = false
		reply.Ballot = inst.Ballot
		reply.ReplicaID = epaxosMgr.serverID
		log.Debugf("[Prepare-RPC] Rejected ballot %d <= %d | Instance=%s",
			args.Ballot, inst.Ballot, args.InstanceID)
		return nil
	}

	// Update ballot (promise not to accept lower ballots)
	inst.Ballot = args.Ballot

	// Return current state
	reply.OK = true
	reply.Ballot = inst.Ballot
	reply.Status = inst.Status
	reply.Seq = inst.Seq
	reply.Deps = inst.Deps.Clone()
	reply.Command = inst.Command
	reply.ReplicaID = epaxosMgr.serverID

	log.Debugf("[Prepare-RPC] Accepted | Instance=%s | Status=%v | Seq=%d",
		args.InstanceID, inst.Status, inst.Seq)

	return nil
}

// Commit handles Commit phase requests
func (s *EPaxosService) Commit(args *CommitArgs, reply *CommitReply) error {
	log.Debugf("[Commit-RPC] Instance=%s | Seq=%d | Deps=%d",
		args.InstanceID, args.Seq, len(args.Deps))

	inst := epaxosMgr.state.GetInstance(args.InstanceID)

	inst.Lock()
	// Update to committed
	inst.Command = args.Command // Store command if not already set
	inst.Seq = args.Seq
	inst.Deps = args.Deps
	inst.Status = COMMITTED
	inst.Unlock() // CRITICAL FIX: Release inst lock BEFORE calling updateCommittedUpTo

	epaxosMgr.state.UpdateCommitIndex(1)
	epaxosMgr.updateCommittedUpTo(args.InstanceID.ReplicaID, args.InstanceID.InstanceNo)

	reply.OK = true

	log.Infof("[Commit-RPC] Committed | Instance=%s | Seq=%d",
		args.InstanceID, args.Seq)

	return nil
}

// Ping is a lightweight RPC for connection pre-warming (CORA fairness compatibility)
// This eliminates TCP handshake cost from first measured request
func (s *EPaxosService) Ping(args *ClientArgs, reply *ClientReply) error {
	reply.Success = true
	reply.PathUsed = "PING"
	reply.Latency = 0.0
	reply.ClientClock = args.ClientClock
	return nil
}

// ConsensusService is the main RPC entry point for clients
func (s *EPaxosService) ConsensusService(args *ClientArgs, reply *ClientReply) error {
	// Reject new requests during shutdown
	if shuttingDown.Load() {
		reply.Success = false
		reply.ErrorMsg = errors.New("server is shutting down")
		return reply.ErrorMsg
	}

	start := time.Now()
	activeRPCs.Add(1)
	defer activeRPCs.Add(-1)

	// Determine batch size
	batchSize := 0
	if args.Type == PlainMsg {
		batchSize = len(args.CmdPlain)
	} else if args.Type == MongoDB {
		batchSize = len(args.CmdMongo)
	}

	if batchSize == 0 {
		reply.ErrorMsg = errors.New("empty batch")
		return reply.ErrorMsg
	}

	// CRITICAL: Validate batch consistency
	if len(args.ObjIDs) != batchSize {
		err := fmt.Errorf("ObjIDs length (%d) != batch size (%d)",
			len(args.ObjIDs), batchSize)
		reply.ErrorMsg = err
		return err
	}

	if len(args.ObjTypes) != batchSize {
		err := fmt.Errorf("ObjTypes length (%d) != batch size (%d)",
			len(args.ObjTypes), batchSize)
		reply.ErrorMsg = err
		return err
	}

	log.Debugf("[Client-RPC] ClientID=%d | ClientClock=%d | BatchSize=%d | IsMixed=%v",
		args.ClientID, args.ClientClock, batchSize, args.IsMixed)

	// Validate object types
	for i, objType := range args.ObjTypes {
		if objType < IndependentObject || objType > DependentObject {
			err := fmt.Errorf("invalid object type at index %d: %d", i, objType)
			reply.ErrorMsg = err
			return err
		}
	}

	// Create command from args
	cmd := &Command{
		ClientID:    args.ClientID,
		ClientClock: args.ClientClock,
		CmdType:     args.CmdType,
		ObjID:       args.ObjID,
		ObjType:     args.ObjType,
		ObjIDs:      args.ObjIDs,
		ObjTypes:    args.ObjTypes,
		IsMixed:     args.IsMixed,
	}

	// Set payload based on type
	switch args.Type {
	case PlainMsg:
		cmd.Payload = args.CmdPlain
	case MongoDB:
		cmd.Payload = args.CmdMongo
	default:
		err := fmt.Errorf("unknown payload type: %d", args.Type)
		reply.ErrorMsg = err
		return err
	}

	// Log batch details
	if args.IsMixed {
		// Count object types in mixed batch
		indepCount := 0
		depCount := 0
		for _, objType := range args.ObjTypes {
			switch objType {
			case IndependentObject:
				indepCount++
			case DependentObject:
				depCount++
			}
		}
		log.Infof("[Client-RPC] Mixed batch | Indep=%d | Dependent=%d | Total=%d",
			indepCount, depCount, batchSize)
	} else {
		log.Infof("[Client-RPC] Single-type batch | Type=%d | Size=%d",
			args.ObjType, batchSize)
	}

	// Run consensus and get instance ID(s)
	primaryInstanceID, _, success, path := epaxosMgr.HandleCommand(cmd)

	if !success {
		reply.Success = false
		reply.PathUsed = path
		reply.ErrorMsg = errors.New("consensus failed")
		reply.Latency = time.Since(start).Seconds() * 1000
		reply.ClientClock = args.ClientClock
		log.Warnf("[Client-RPC] FAILED | ClientClock=%d | Path=%s | Latency=%.2fms",
			args.ClientClock, path, reply.Latency)
		return reply.ErrorMsg
	}

	reply.Success = true
	reply.PathUsed = path
	reply.Latency = time.Since(start).Seconds() * 1000
	reply.ClientClock = args.ClientClock

	log.Infof("[Client-RPC] SUCCESS (at commit) | Primary=%s | ClientClock=%d | Path=%s | Latency=%.2fms | BatchSize=%d",
		primaryInstanceID, args.ClientClock, path, reply.Latency, batchSize)
	return nil
}
