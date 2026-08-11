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
var fastQuorum int          // Remote PreAcceptOK/equal-attrs replies needed for fast path commit (paper's F+floor((F+1)/2) total quorum, minus the leader's implicit self-vote)
var slowQuorum int          // Remote AcceptOK replies needed for slow path commit (plain majority, F total remote)
var thriftyPreAcceptContact int // PreAccept contacts in thrifty mode (== fastQuorum, so thrifty mode can still reach the fast quorum)
var thriftyAcceptContact int    // Accept contacts in thrifty mode (== slowQuorum, plain majority)
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
var crashTarget int // replica ID to kill alone when crashMode == 4

var role int
var pinServer int // required, >=0: always send to this server ID (round-robin dispatch has been removed)

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
	flag.IntVar(&crashTarget, "crashtarget", -1, "replica ID to kill alone when -cm=4")

	flag.IntVar(&role, "role", 0, "0=server, 1=client")
	flag.IntVar(&pinServer, "pinserver", -1, "pin client to specific server ID (required; no round-robin default)")

	flag.Parse()
	// Fast-path quorum uses the paper's F+floor((F+1)/2) formula (Moraru/
	// Andersen/Kaminsky SOSP'13 §4.3), not github.com/efficient/epaxos's
	// checked-in `inst.lb.preAcceptOKs >= r.N/2` shortcut (epaxos.go:1050).
	// Since N=2F+1, integer division r.N/2 == F always, so that shortcut
	// collapses the fast-path quorum to a flat majority — identical to the
	// paper's formula at F=1,2 (N=3,5), but strictly smaller than it for
	// F>=3 (N=7,11,...), which is a known open bug in the reference
	// implementation (github.com/efficient/epaxos issue #10) that also
	// undersizes the quorum needed to safely skip the second round trip.
	// We use the paper's formula here to match both "EPaxos Revisited"
	// (Tollman/Park/Ousterhout, NSDI'21) and the rigorous correctness
	// proof for these exact quorum sizes in "Fixing and Simplifying
	// Egalitarian Paxos" (Ryabinin/Gotsman/Sutra, OPODIS'25) — both use
	// F+floor((F+1)/2) rather than efficient/epaxos's flat majority.
	//
	// fastQuorum below is expressed as *remote* replies (excluding the
	// leader's own implicit self-vote), so it's the paper's total quorum
	// minus one: (F + (F+1)/2) - 1, using Go's floor integer division.
	fastQuorum = threshold + (threshold+1)/2 - 1
	slowQuorum = threshold
	thriftyPreAcceptContact = fastQuorum // contact exactly enough replicas to reach the fast quorum
	thriftyAcceptContact = slowQuorum    // contact exactly enough replicas to reach the slow/majority quorum

	log.Debugf("EPaxos Config: N=%d, F=%d, FastQ=%d, SlowQ=%d, ThriftyPreAccept=%d, ThriftyAccept=%d",
		numOfServers, threshold, fastQuorum, slowQuorum, thriftyPreAcceptContact, thriftyAcceptContact)
	log.Debugf("Object Distribution: Indep=%.0f%%, Dependent=%.0f%%, NumObjects=%d, ReadRatio=%.0f%%",
		indepRatio, 100.0-indepRatio, numObjects, readRatio)
}
