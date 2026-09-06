package main

import (
	"fmt"
	"time"

	"epaxos/mongodb"
)

// Server-side batching, ported from upstream efficient/epaxos's actual
// mechanism (src/epaxos/epaxos.go's run()/handlePropose/fastClock,
// src/genericsmr/genericsmr.go's ProposeChan) rather than an ad-hoc
// accumulator: a buffered propose channel per bucket, gated by an on/off
// switch that closes the instant a proposal arrives (letting more
// accumulate) and reopens on a fixed periodic tick -- fully adaptive to
// load, since a burst of arrivals during one closed window all land in the
// same batch, while a quiet period just waits out the tick with nothing to
// drain. This differs from WOC/Cabinet's window-since-first-arrival timer;
// see startEpaxosBatchAccumulators/epaxosProposeBucket.run below for the
// actual port of upstream's run()/handlePropose.
//
// Requests are bucketed by (CmdType, ObjType) rather than merged
// indiscriminately: GetInterferingInstances (state.go) picks its conflict
// table off a single scalar CmdType for the whole command, so merging a
// READ with a WRITE (or vice versa) into one Command would corrupt
// interference detection. ObjType is bucketed too because
// handleSingleTypeBatch's post-commit accounting (consensus.go) branches
// once on cmd.ObjType for the whole batch. Mixed-type batches (cmd.IsMixed)
// are never routed here at all -- see service.go -- they keep using
// handleMixedBatch's existing per-element path unmerged. Upstream doesn't
// need this bucketing (no equivalent per-object-type interference split),
// so each bucket here runs its own independent, faithful copy of
// upstream's single mechanism rather than one shared channel for the whole
// replica.
//
// EPaxos is also leaderless (any replica can propose any instance), unlike
// WOC/Cabinet's fixed-leader designs, so this batching applies uniformly on
// every replica, not gated behind an IsLeader() check.

type epaxosBatchKey struct {
	cmdType CmdType
	objType int
}

type epaxosBatchResult struct {
	instanceID InstanceID
	success    bool
	path       string
}

type epaxosPendingRequest struct {
	cmd    *Command
	result chan epaxosBatchResult
}

func epaxosServerBatchingEnabled() bool {
	return serverBatchWindowUs > 0 && serverMaxBatch > 1
}

// epaxosProposeChanBufferSize mirrors upstream's CHAN_BUFFER_SIZE (200000):
// large enough that a producer (ConsensusService's RPC goroutine) never
// realistically blocks sending into it, so submitForBatching's send is
// effectively non-blocking under any load this harness generates.
const epaxosProposeChanBufferSize = 100000

// epaxosProposeBucket is one (CmdType, ObjType) bucket's own copy of
// upstream's ProposeChan - a buffered channel every submitForBatching call
// for this bucket sends into, drained exclusively by this bucket's own
// run() goroutine.
type epaxosProposeBucket struct {
	ch chan *epaxosPendingRequest
}

var epaxosBuckets map[epaxosBatchKey]*epaxosProposeBucket

// startEpaxosBatchAccumulators spawns one goroutine per bucket, called once
// at server startup (main.go's runServer, right after NewEPaxosManager)
// when epaxosServerBatchingEnabled() - mirrors upstream's run() starting
// fastClock() once at replica startup. Must run before any
// submitForBatching call (i.e. before the RPC listener starts accepting
// client connections), since submitForBatching looks up epaxosBuckets
// without a lock -- safe because it's fully populated before any concurrent
// reader can exist.
func startEpaxosBatchAccumulators() {
	epaxosBuckets = make(map[epaxosBatchKey]*epaxosProposeBucket)
	for _, ct := range []CmdType{WRITE, READ} {
		for _, ot := range []int{IndependentObject, DependentObject} {
			b := &epaxosProposeBucket{ch: make(chan *epaxosPendingRequest, epaxosProposeChanBufferSize)}
			epaxosBuckets[epaxosBatchKey{cmdType: ct, objType: ot}] = b
			go b.run()
		}
	}
}

// run is this bucket's own copy of upstream's run()'s propose-handling
// select cases (epaxos.go's onOffProposeChan/fastClockChan): onOff gates
// whether new proposals are picked up. It starts open; the instant a
// proposal arrives, handlePropose's exact batchSize/drain logic runs
// (drain whatever else is already queued, up to serverMaxBatch), then the
// gate closes (onOff = nil) so no new proposal is picked up until the next
// tick reopens it - letting commands accumulate for the next batch exactly
// the way upstream's comment describes at epaxos.go's onOffProposeChan
// assignment.
func (b *epaxosProposeBucket) run() {
	onOff := b.ch
	ticker := time.NewTicker(time.Duration(serverBatchWindowUs) * time.Microsecond)
	defer ticker.Stop()
	for {
		if shuttingDown.Load() {
			return
		}
		select {
		case req := <-onOff:
			// batchSize := len(ProposeChan) + 1, capped at MAX_BATCH -
			// upstream's handlePropose, verbatim (epaxos.go:801-826).
			batchSize := len(b.ch) + 1
			if batchSize > serverMaxBatch {
				batchSize = serverMaxBatch
			}
			reqs := make([]*epaxosPendingRequest, batchSize)
			reqs[0] = req
			for i := 1; i < batchSize; i++ {
				reqs[i] = <-b.ch
			}
			processEpaxosBatch(reqs)
			onOff = nil
		case <-ticker.C:
			onOff = b.ch
		}
	}
}

// submitForBatching hands cmd to its bucket's propose channel and blocks
// for the shared result once that bucket's goroutine forms and commits a
// batch containing it - mirrors a client connection's blocking write into
// upstream's ProposeChan followed by waiting for its reply to be written
// back. Callers must only pass cmd where !cmd.IsMixed (see service.go).
func submitForBatching(cmd *Command) epaxosBatchResult {
	key := epaxosBatchKey{cmdType: cmd.CmdType, objType: cmd.ObjType}
	resultCh := make(chan epaxosBatchResult, 1)
	b := epaxosBuckets[key]
	b.ch <- &epaxosPendingRequest{cmd: cmd, result: resultCh}
	return <-resultCh
}

// processEpaxosBatch merges every request in reqs into one synthetic
// Command and commits it as a single Paxos instance via
// epaxosMgr.HandleCommand, then fans the shared result out to each
// request's own result channel -- they all share this instance's fate, the
// same all-or-nothing outcome a single client's own multi-op batch already
// had.
func processEpaxosBatch(reqs []*epaxosPendingRequest) {
	if len(reqs) == 0 {
		return
	}

	merged := reqs[0].cmd
	if len(reqs) > 1 {
		merged = mergeEpaxosCommands(reqs)
	}

	instanceID, _, success, path := epaxosMgr.HandleCommand(merged)

	// path's "HOT:%d" form (handleSingleTypeBatch) encodes the MERGED
	// batch's total element count, not any individual request's own share
	// of it. recordBatchMetrics (client.go) parses that count straight out
	// of the path string for HOT (and MIXED) replies rather than trusting
	// its own batchSize parameter, so forwarding this one shared path
	// verbatim to every request made each of N merged clients credit
	// itself with all N ops instead of its own ~1 - an N-fold overcount
	// whenever server-side batching actually merged multiple clients'
	// requests together. FAST/SLOW carry no embedded count and don't have
	// this problem (recordBatchMetrics uses its own batchSize parameter for
	// those), so only HOT needs rewriting per-request here.
	isHot := len(path) >= 4 && path[:4] == "HOT:"
	for _, r := range reqs {
		rres := epaxosBatchResult{instanceID: instanceID, success: success, path: path}
		if isHot {
			n := len(r.cmd.ObjIDs)
			if n == 0 {
				n = 1
			}
			rres.path = fmt.Sprintf("HOT:%d", n)
		}
		r.result <- rres
	}
}

// mergeEpaxosCommands concatenates N single-client Commands sharing one
// (CmdType, ObjType) bucket into one synthetic Command. Driven by each
// request's own ObjIDs length (falling back to 1, matching
// handleSingleTypeBatch's own batchSize convention), not payload length --
// a plain-message READ carries populated ObjIDs but a nil/empty payload
// (only MongoDB reads carry a real payload matching ObjIDs 1:1, see
// client.go), so iterating by payload length would silently drop every
// plain-message read's contribution to the merge.
func mergeEpaxosCommands(reqs []*epaxosPendingRequest) *Command {
	head := reqs[0].cmd
	merged := &Command{
		ClientID:    head.ClientID,
		ClientClock: head.ClientClock,
		CmdType:     head.CmdType,
		ObjID:       head.ObjID,
		ObjType:     head.ObjType,
	}

	isMongo := false
	switch head.Payload.(type) {
	case []mongodb.Query:
		isMongo = true
	}

	var plainPayload [][]byte
	var mongoPayload []mongodb.Query

	for _, r := range reqs {
		c := r.cmd
		n := len(c.ObjIDs)
		if n == 0 {
			n = 1
		}
		ids, types := epaxosObjIDsTypes(c, n)
		merged.ObjIDs = append(merged.ObjIDs, ids...)
		merged.ObjTypes = append(merged.ObjTypes, types...)
		for i := 0; i < n; i++ {
			merged.ClientIDs = append(merged.ClientIDs, c.ClientID)
			merged.ClientClocks = append(merged.ClientClocks, c.ClientClock)
		}

		if isMongo {
			if v, ok := c.Payload.([]mongodb.Query); ok {
				mongoPayload = append(mongoPayload, v...)
			}
		} else if v, ok := c.Payload.([][]byte); ok {
			plainPayload = append(plainPayload, v...)
		}
	}

	if isMongo {
		merged.Payload = mongoPayload
	} else {
		merged.Payload = plainPayload
	}

	return merged
}

// epaxosObjIDsTypes returns cmd's own ObjIDs/ObjTypes, extended to length n
// by falling back to its scalar ObjID/ObjType for any index beyond what
// ObjIDs/ObjTypes already cover -- matching handleSingleTypeBatch's own
// fallback convention for a lone, un-merged command.
func epaxosObjIDsTypes(cmd *Command, n int) ([]string, []int) {
	ids := make([]string, n)
	types := make([]int, n)
	for i := 0; i < n; i++ {
		if i < len(cmd.ObjIDs) {
			ids[i] = cmd.ObjIDs[i]
		} else {
			ids[i] = cmd.ObjID
		}
		if i < len(cmd.ObjTypes) {
			types[i] = cmd.ObjTypes[i]
		} else {
			types[i] = cmd.ObjType
		}
	}
	return ids, types
}
