package main

import (
	"github.com/sirupsen/logrus"
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
var mongoDbFollower *mongodb.MongoFollower
func init() {
    gob.Register([][]byte{})
    gob.Register([]byte{})
    gob.Register(mongodb.Query{})
    gob.Register([]mongodb.Query{})
}

func main() {
	loadCommandLineInputs()
	SetLogger(logLevel, myServerID, production)
	
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
	
	// Signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		log.Infof("Server %d: shutdown signal received", myServerID)
		
		// Wait for active RPCs
		deadline := time.Now().Add(10 * time.Second)
		for activeRPCs.Load() > 0 && time.Now().Before(deadline) {
			log.Infof("Server %d: waiting for %d active RPCs...",
				myServerID, activeRPCs.Load())
			time.Sleep(500 * time.Millisecond)
		}
		
		// Save metrics (FINAL SAVE)
		log.Infof("Server %d: 💾 Saving final metrics...", myServerID)
		
		// Save server consensus metrics
		if err := epaxosMgr.SaveServerMetrics(); err != nil {
			log.Errorf("Server %d: ❌ Failed to save server metrics: %v", myServerID, err)
		} else {
			log.Infof("Server %d: ✓ Server consensus metrics saved", myServerID)
		}
		
		// Ensure file is fully written to disk
		log.Infof("Server %d: Flushing files to disk...", myServerID)
		time.Sleep(3 * time.Second)
		
		cleanupConnections()
		log.Infof("Server %d: 👋 Shutdown complete", myServerID)
		os.Exit(0)
	}()
	
	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Errorf("Accept error: %v", err)
			continue
		}
		
		go rpc.ServeConn(conn)
	}
}