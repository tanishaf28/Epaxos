package main

import (
	"flag"
)

const (
	Localhost = iota
	Distributed
)

const (
	PlainMsg = iota
	MongoDB
)

// Object types for workload characterization (same as WOC for comparison):
// a binary Independent/Dependent split, matching WOC's objectmap.go exactly.
// DependentObject is the type where EPaxos's interference detection actually
// produces conflicts (cross-client contention); Independent objects are
// namespaced per client so they never conflict by construction.
const (
	IndependentObject = iota
	DependentObject
)

type CmdType int

const (
	WRITE CmdType = iota
	READ
)

type InstanceStatus int

const (
	NONE InstanceStatus = iota
	PREACCEPTED
	ACCEPTED
	COMMITTED
	EXECUTED
)

var numOps int
var numOfServers int
var threshold int           // F (failures tolerated)
var fastQuorum int          // Remote PreAcceptOK/equal-attrs replies needed for fast path commit (F + floor((F+1)/2) total, minus the leader)
var slowQuorum int          // Remote AcceptOK replies needed for slow path commit (plain majority, F total remote)
var thriftyPreAcceptContact int // F + ⌊(F+1)/2⌋ (PreAccept contacts in thrifty mode)
var thriftyAcceptContact int    // F + 1 (Accept contacts in thrifty mode)
var myServerID int
var configPath string
var production bool
var logLevel string
var mode int
var thriftyMode bool // Thrifty mode: contact only quorum instead of all replicas
var evalType int

var batchsize int
var msgsize int

// Object distribution (for fair comparison with WOC's objectmap.go)
var indepRatio float64 // % of independent objects; remainder is DependentObject
var numObjects int     // total key-space size, split into indep/dependent pools by indepRatio
var readRatio float64  // % of ops that are reads (vs writes)

// Batch composition mode
var batchComposition string // "mixed" | "object-specific" | "single_obj"

// MongoDB parameters
var mongoLoadType string
var mongoClientNum int

// Crash test parameters
var crashTime int
var crashMode int

var role int

func loadCommandLineInputs() {
	flag.IntVar(&numOps, "ops", 1000, "number of operations")
	flag.IntVar(&numOfServers, "n", 5, "# of servers (N = 2F + 1)")
	flag.IntVar(&threshold, "t", 2, "# of failures tolerated (F)")

	flag.IntVar(&batchsize, "b", 1, "batch size")
	flag.IntVar(&myServerID, "id", 0, "this server ID")
	flag.StringVar(&configPath, "path", "./config/cluster_localhost.conf", "config file path")

	flag.BoolVar(&production, "pd", false, "production mode?")
	flag.StringVar(&logLevel, "log", "debug", "log level")
	flag.IntVar(&mode, "mode", 0, "0=localhost, 1=distributed")
	flag.IntVar(&evalType, "et", 0, "0=plain msg, 1=mongodb")
	flag.BoolVar(&thriftyMode, "thrifty", false, "thrifty mode: contact only quorum instead of all replicas")

	flag.IntVar(&msgsize, "ms", 512, "message size")

	// Object distribution parameters (same as WOC for comparison)
	flag.Float64Var(&indepRatio, "indep", 70.0, "% of independent objects; remainder is dependent")
	flag.IntVar(&numObjects, "numobjects", 100000, "total object key-space size (split into indep/dependent pools by -indep)")
	flag.Float64Var(&readRatio, "readratio", 0.0, "% of ops that are reads (vs writes)")

	// Batch composition mode
	flag.StringVar(&batchComposition, "bcomp", "object-specific",
		"batch composition: 'mixed' | 'object-specific' | 'single_obj'")

	// MongoDB parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb workload")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# mongodb clients")

	// Crash parameters
	flag.IntVar(&crashTime, "ct", 20, "rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "crash mode")

	flag.IntVar(&role, "role", 0, "0=server, 1=client")

	flag.Parse()
	// Published EPaxos formula (Moraru/Andersen/Kaminsky SOSP'13 §4.3,
	// confirmed by Tollman/Park/Ousterhout's "EPaxos Revisited", NSDI'21
	// §2.1): fast-path quorum is F + floor((F+1)/2) replicas TOTAL
	// (including the leader's own implicit vote); slow-path quorum is
	// always a plain majority (F+1 total). These two formulas only
	// coincide at N=3 and N=5 — for N>=7 the fast path genuinely needs
	// MORE replicas than a simple majority (the larger quorum is what
	// makes the no-second-round-trip safety argument hold). An earlier
	// pass at this code set fastQuorum=slowQuorum=F based on reading
	// github.com/efficient/epaxos's checked-in Go source, which uses a
	// flat N/2 check for both paths — that reference implementation is
	// NOT the same as the protocol's own published/peer-reviewed formula,
	// and matches it only by coincidence at N=3,5. Following the paper,
	// not that one reference implementation, here.
	fastQuorum = threshold + (threshold+1)/2 - 1 // remote count; -1 for the leader's implicit vote
	slowQuorum = threshold
	thriftyPreAcceptContact = threshold + (threshold+1)/2
	thriftyAcceptContact = threshold + 1

	log.Debugf("EPaxos Config: N=%d, F=%d, FastQ=%d, SlowQ=%d, ThriftyPreAccept=%d, ThriftyAccept=%d",
		numOfServers, threshold, fastQuorum, slowQuorum, thriftyPreAcceptContact, thriftyAcceptContact)
	log.Debugf("Object Distribution: Indep=%.0f%%, Dependent=%.0f%%, NumObjects=%d, ReadRatio=%.0f%%",
		indepRatio, 100.0-indepRatio, numObjects, readRatio)
}
