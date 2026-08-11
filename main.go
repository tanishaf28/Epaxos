package main

import (
	"github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"encoding/gob"
	"net/rpc"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"epaxos/config"
	"epaxos/eval"
	"epaxos/mongodb"
)

var log = logrus.New()
var perfM = &eval.PerfMeter{}
var epaxosMgr *EPaxosManager
var activeRPCs atomic.Int64
var shuttingDown atomic.Bool
var mongoDbFollower *mongodb.MongoFollower
func init() {
    gob.Register([][]byte{})
    gob.Register([]byte{})
    gob.Register(mongodb.Query{})
    gob.Register([]mongodb.Query{})
}

func main() {
	loadCommandLineInputs()
	logLabel := "server"
	if role == 1 {
		logLabel = "client"
	}
	SetLogger(logLevel, myServerID, production, logLabel)
	
	log.Infof("EPaxos starting | ServerID=%d | Role=%d | N=%d | F=%d",
		myServerID, role, numOfServers, threshold)
	
	if role == 0 {
		// Server mode
		runServer()
	} else {
		// Client mode
		RunClient(myServerID, configPath, numOps)
	}
}

func runServer() {
	// Initialize EPaxos manager
	epaxosMgr = NewEPaxosManager(myServerID, numOfServers)
	
	log.Infof("Server %d: EPaxos manager initialized | FastQ=%d | SlowQ=%d",
		myServerID, fastQuorum, slowQuorum)
	
	// Schedule crash if configured
	crashList := prepCrashList()
	scheduleCrash(crashList)
	if len(crashList) > 0 {
		log.Infof("Server %d: Crash mode %d | Crash list: %v | Crash time: %d",
			myServerID, crashMode, crashList, crashTime)
	}
	
	// Establish connections to other replicas
	go func() {
		time.Sleep(2 * time.Second) // Wait for others to start
		establishReplicaConnections()
	}()
	
	// Initialize MongoDB if needed
	switch evalType {
	case PlainMsg:
		// Nothing to initialize
	case MongoDB:
		go mongoDBCleanUp()
		initMongoDB()
	}
	
	// Register RPC service
	err := rpc.Register(NewEPaxosService())
	if err != nil {
		log.Fatalf("RPC Register failed: %v", err)
	}
	
	// Start RPC listener
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	myAddr := serverConfig[myServerID][config.ServerIP] + ":" +
		serverConfig[myServerID][config.ServerRPCListenerPort]
	
	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("Listen failed: %v", err)
	}
	
	log.Infof("Server %d: listening on %s", myServerID, myAddr)
	
	// Channel to signal Accept loop to stop
	acceptDone := make(chan struct{})
	
	// Signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Server %d: PANIC in shutdown goroutine: %v", myServerID, r)
				// Force save metrics even if panic
				if epaxosMgr != nil && epaxosMgr.perfM != nil {
					epaxosMgr.SaveServerMetrics()
				}
				os.Exit(1)
			}
		}()
		
		<-sigChan
		log.Infof("Server %d: shutdown signal received", myServerID)
		
		// Step 1: Stop accepting new connections immediately
		shuttingDown.Store(true)
		listener.Close()
		log.Infof("Server %d: listener closed, no longer accepting connections", myServerID)
		
		// Step 2: Wait for active RPCs with shorter deadline (5 seconds)
		deadline := time.Now().Add(5 * time.Second)
		lastActive := activeRPCs.Load()
		for activeRPCs.Load() > 0 && time.Now().Before(deadline) {
			current := activeRPCs.Load()
			if current != lastActive {
				log.Infof("Server %d: waiting for %d active RPCs... (down from %d)",
					myServerID, current, lastActive)
				lastActive = current
			}
			time.Sleep(200 * time.Millisecond)
		}
		
		remaining := activeRPCs.Load()
		if remaining > 0 {
			log.Warnf("Server %d: timeout reached with %d RPCs still active - forcibly shutting down", 
				myServerID, remaining)
		} else {
			log.Infof("Server %d: all active RPCs completed", myServerID)
		}
		
		// Step 3: Stop continuous execution goroutine
		log.Infof("Server %d: Stopping execution goroutine...", myServerID)
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Server %d: panic in StopExecution: %v", myServerID, r)
				}
			}()
			epaxosMgr.StopExecution()
		}()
		time.Sleep(3 * time.Second) // Extra safety margin for shutdown ordering
		log.Infof("Server %d: Execution goroutine stopped", myServerID)
		
		// Step 4: Save metrics (FINAL SAVE)
		log.Infof("Server %d: 💾 Saving final metrics...", myServerID)
		
		// Save server consensus metrics
		if err := epaxosMgr.SaveServerMetrics(); err != nil {
			log.Errorf("Server %d: ❌ Failed to save server metrics: %v", myServerID, err)
		} else {
			log.Infof("Server %d: ✓ Server consensus metrics saved", myServerID)
		}
		
		// Step 4: Brief flush period
		log.Infof("Server %d: Flushing files to disk...", myServerID)
		time.Sleep(1 * time.Second)
		
		// Cleanup connections with timeout protection
		done := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Server %d: panic in cleanupConnections: %v", myServerID, r)
				}
				close(done)
			}()
			cleanupConnections()
		}()
		
		select {
		case <-done:
			log.Infof("Server %d: Connections cleaned up", myServerID)
		case <-time.After(2 * time.Second):
			log.Warnf("Server %d: cleanupConnections timeout - proceeding anyway", myServerID)
		}
		
		log.Infof("Server %d: 👋 Shutdown complete", myServerID)
		
		// Signal Accept loop to stop checking for errors
		close(acceptDone)
		
		os.Exit(0)
	}()
	
	// Accept connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Check if we're shutting down (expected error)
				if shuttingDown.Load() {
					// Suppress error spam during shutdown
					return
				}
				log.Errorf("Accept error: %v", err)
				continue
			}
			
			go rpc.ServeConn(conn)
		}
	}()
	
	// Block until shutdown completes
	<-acceptDone
}

// prepCrashList builds the list of servers that will crash
// Matches WOC's crash injection pattern exactly
func prepCrashList() []int {
	var crashList []int
	switch crashMode {
	case 0:
		// No crashes
	case 1:
		// Crash servers 1..F (lowest IDs, excludes leader at 0)
		for i := 1; i <= threshold; i++ {
			crashList = append(crashList, i)
		}
	case 2:
		// Crash servers N-1..N-F (highest IDs)
		for i := 1; i <= threshold; i++ {
			crashList = append(crashList, numOfServers-i)
		}
	case 3:
		// Crash random F servers (any non-leader replica, including self —
		// each server independently computes its own list and only acts on
		// whether its own ID is in it, so excluding self here meant a
		// server's ID could never appear in its own list and it would
		// never crash).
		rand.Seed(time.Now().UnixNano())
		for len(crashList) < threshold {
			candidate := rand.Intn(numOfServers-1) + 1
			duplicate := false
			for _, id := range crashList {
				if id == candidate {
					duplicate = true
					break
				}
			}
			if !duplicate {
				crashList = append(crashList, candidate)
			}
		}
	case 4:
		// Crash exactly one explicit replica, regardless of F - lets you
		// compare a single failure against the usual F-replica crash test.
		if crashTarget >= 0 {
			crashList = append(crashList, crashTarget)
		}
	}
	return crashList
}

// scheduleCrash triggers os.Exit after crashTime consensus rounds
// This simulates a server crash mid-experiment
func scheduleCrash(crashList []int) {
	if crashMode == 0 || crashTime <= 0 {
		return
	}
	
	// Check if this server is in the crash list
	shouldCrash := false
	for _, id := range crashList {
		if id == myServerID {
			shouldCrash = true
			break
		}
	}
	
	if !shouldCrash {
		return
	}
	
	log.Warnf("[CRASH] Server %d scheduled to crash after %d commits", 
		myServerID, crashTime)
	
	go func() {
		// Poll commit index until crashTime reached
		for {
			// Check state commit index
			commitIndex := epaxosMgr.state.GetCommitIndex()
			if commitIndex >= crashTime {
				log.Warnf("[CRASH] Server %d crashing now (commitIndex=%d >= crashTime=%d)",
					myServerID, commitIndex, crashTime)
				log.Infof("Server %d: SIMULATED CRASH at commitIndex=%d", 
					myServerID, commitIndex)
				
				// Save metrics before dying (so you can analyze partial data)
				if epaxosMgr != nil && epaxosMgr.perfM != nil {
					epaxosMgr.SaveServerMetrics()
				}
				
				os.Exit(1) // Non-zero = crash, not clean shutdown
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
}