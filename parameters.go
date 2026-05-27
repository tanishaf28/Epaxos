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

// Object types for workload characterization (same as WOC for comparison)
const (
	IndependentObject = iota
	CommonObject
	HotObject // High-conflict shared objects
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
var fastQuorum int          // Remote PreAcceptOK replies needed for fast path commit
var slowQuorum int          // Remote AcceptOK replies needed for slow path commit
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

// Object distribution ratios (for fair comparison with WOC)
var indepRatio float64  // % of independent objects (low interference)
var commonRatio float64 // % of common objects (medium interference)
var conflictRate int    // % of hot objects (high interference)

// Batch composition mode
var batchComposition string // "mixed" | "object-specific" | "single_obj"

// Max-in-flight control (for CORA-style pipelined execution)
var useFixedInflight bool // If true, use fixed max-in-flight; if false, use adaptive limiter

// MongoDB parameters
var mongoLoadType string
var mongoClientNum int

// Crash test parameters
var crashTime int
var crashMode int

var suffix string
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
	flag.Float64Var(&indepRatio, "indep", 70.0, "% of independent objects")
	flag.Float64Var(&commonRatio, "common", 20.0, "% of common objects")
	flag.IntVar(&conflictRate, "conflictrate", 10, "% of hot objects (high conflict)")

	// Batch composition mode
	flag.StringVar(&batchComposition, "bcomp", "object-specific",
		"batch composition: 'mixed' | 'object-specific' | 'single_obj'")

	// Max-in-flight control (CORA-style closed-loop execution)
	flag.BoolVar(&useFixedInflight, "fixed-inflight", false,
		"Use fixed max-in-flight (CORA closed-loop model) instead of adaptive limiter")

	// MongoDB parameters
	flag.StringVar(&mongoLoadType, "mload", "a", "mongodb workload")
	flag.IntVar(&mongoClientNum, "mcli", 16, "# mongodb clients")

	// Crash parameters
	flag.IntVar(&crashTime, "ct", 20, "rounds before crash")
	flag.IntVar(&crashMode, "cm", 0, "crash mode")

	flag.StringVar(&suffix, "suffix", "epaxos", "file suffix")
	flag.IntVar(&role, "role", 0, "0=server, 1=client")

	flag.Parse()
	fastQuorum = 2*threshold - 1
	slowQuorum = threshold
	thriftyPreAcceptContact = threshold + (threshold+1)/2
	thriftyAcceptContact = threshold + 1

	totalRatio := float64(conflictRate) + indepRatio + commonRatio

	log.Debugf("EPaxos Config: N=%d, F=%d, FastQ=%d, SlowQ=%d, ThriftyPreAccept=%d, ThriftyAccept=%d",
		numOfServers, threshold, fastQuorum, slowQuorum, thriftyPreAcceptContact, thriftyAcceptContact)
	log.Debugf("Object Distribution: Hot=%d%%, Indep=%.0f%%, Common=%.0f%% (Total=%.0f%%)",
		conflictRate, indepRatio, commonRatio, totalRatio)
}
