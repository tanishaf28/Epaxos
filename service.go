package main

import (
	"errors"
	"fmt"
	"time"
	"epaxos/mongodb"
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
	LeaderID   int
}

type PreAcceptReply struct {
	OK            bool
	Seq           int
	Deps          Dependencies
	ReplicaID     int
	CommittedUpTo []int32
}

type AcceptArgs struct {
	InstanceID InstanceID
	Seq        int
	Deps       Dependencies
	LeaderID   int
}

type AcceptReply struct {
	OK        bool
	ReplicaID int
}

type CommitArgs struct {
	InstanceID InstanceID
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
	Type        int     // PlainMsg, MongoDB
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
func (s *EPaxosService) PreAccept(args *PreAcceptArgs, reply *PreAcceptReply) error {
	inst := epaxosMgr.state.GetInstance(args.InstanceID)
	
	inst.Lock()
	defer inst.Unlock()
	
	// Update attributes based on local state
	localDeps := epaxosMgr.state.GetInterferingInstances(args.Command)
	localSeq := epaxosMgr.state.GetMaxSeq(localDeps) + 1
	
	// Merge with proposed attributes
	finalDeps := args.Deps.Union(localDeps)
	finalSeq := args.Seq
	if localSeq > finalSeq {
		finalSeq = localSeq
	}
	
	// Update instance
	inst.Command = args.Command
	inst.Seq = finalSeq
	inst.Deps = finalDeps
	inst.Status = PREACCEPTED
	
	// Register object access
	objIDs := args.Command.ObjIDs
	if len(objIDs) == 0 && args.Command.ObjID != "" {
		objIDs = []string{args.Command.ObjID}
	}
	epaxosMgr.state.RegisterObjectAccess(args.InstanceID, objIDs)
	
	// Build reply with CommittedUpTo
	reply.OK = true
	reply.Seq = finalSeq
	reply.Deps = finalDeps
	reply.ReplicaID = epaxosMgr.serverID
	
	// Include committed instances per replica
	epaxosMgr.RLock()
	reply.CommittedUpTo = make([]int32, numOfServers)
	copy(reply.CommittedUpTo, epaxosMgr.committedUpTo)
	epaxosMgr.RUnlock()
	
	return nil
}

// Accept handles Accept phase requests
func (s *EPaxosService) Accept(args *AcceptArgs, reply *AcceptReply) error {
	log.Debugf("[Accept-RPC] Instance=%s | Seq=%d | Deps=%d",
		args.InstanceID, args.Seq, len(args.Deps))
	
	inst := epaxosMgr.state.GetInstance(args.InstanceID)
	
	inst.Lock()
	defer inst.Unlock()
	
	// Update to accepted
	inst.Seq = args.Seq
	inst.Deps = args.Deps
	inst.Status = ACCEPTED
	
	reply.OK = true
	reply.ReplicaID = epaxosMgr.serverID
	
	return nil
}

// Commit handles Commit phase requests
func (s *EPaxosService) Commit(args *CommitArgs, reply *CommitReply) error {
	log.Debugf("[Commit-RPC] Instance=%s | Seq=%d | Deps=%d",
		args.InstanceID, args.Seq, len(args.Deps))
	
	inst := epaxosMgr.state.GetInstance(args.InstanceID)
	
	inst.Lock()
	defer inst.Unlock()
	
	// Update to committed
	inst.Seq = args.Seq
	inst.Deps = args.Deps
	inst.Status = COMMITTED
	
	epaxosMgr.state.UpdateCommitIndex(1)
	epaxosMgr.updateCommittedUpTo(args.InstanceID.ReplicaID, args.InstanceID.InstanceNo)
	
	reply.OK = true
	
	log.Infof("[Commit-RPC] Committed | Instance=%s | Seq=%d",
		args.InstanceID, args.Seq)
	
	return nil
}

// ConsensusService is the main RPC entry point for clients
func (s *EPaxosService) ConsensusService(args *ClientArgs, reply *ClientReply) error {
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
		if objType < IndependentObject || objType > HotObject {
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
		commonCount := 0
		hotCount := 0
		for _, objType := range args.ObjTypes {
			switch objType {
			case IndependentObject:
				indepCount++
			case CommonObject:
				commonCount++
			case HotObject:
				hotCount++
			}
		}
		log.Infof("[Client-RPC] Mixed batch | Indep=%d | Common=%d | Hot=%d | Total=%d",
			indepCount, commonCount, hotCount, batchSize)
	} else {
		log.Infof("[Client-RPC] Single-type batch | Type=%d | Size=%d",
			args.ObjType, batchSize)
	}
	
	// Run consensus
	success, path := epaxosMgr.HandleCommand(cmd)
	
	reply.Success = success
	reply.PathUsed = path
	reply.Latency = time.Since(start).Seconds() * 1000 // milliseconds
	reply.ClientClock = args.ClientClock
	
	if !success {
		reply.ErrorMsg = errors.New("consensus failed")
		log.Warnf("[Client-RPC] FAILED | ClientClock=%d | Path=%s | Latency=%.2fms",
			args.ClientClock, path, reply.Latency)
		return reply.ErrorMsg
	}
	
	log.Infof("[Client-RPC] SUCCESS | ClientClock=%d | Path=%s | Latency=%.2fms | BatchSize=%d",
		args.ClientClock, path, reply.Latency, batchSize)
	
	return nil
}