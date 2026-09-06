package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"epaxos/config"
	"epaxos/mongodb"
)

// gob requires every concrete type ever assigned to an interface field
// crossing the RPC wire to be registered before it does so. service.go
// sets Reply.ErrorMsg (an error interface field) via errors.New/fmt.Errorf
// (both *errors.errorString) and fmt.Errorf with %w (*fmt.wrapError) -- the
// only other gob.Register call (initMongoDB) only runs in MongoDB mode, so
// plainmsg mode had no error-type registration at all, would fail to gob-
// encode as soon as any of the 3 ErrorMsg-setting error paths in service.go
// actually fired.
func init() {
	gob.Register(errors.New(""))
	gob.Register(fmt.Errorf("%w", errors.New("")))
}

// dialServerRPC creates an optimized RPC connection for server-to-server communication
func dialServerRPC(address string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	
	// CRITICAL: Enable TCP_NODELAY for low-latency RPCs
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			log.Warnf("Failed to set TCP_NODELAY: %v", err)
		}
		if err := tcpConn.SetKeepAlive(true); err != nil {
			log.Warnf("Failed to set KeepAlive: %v", err)
		}
		if err := tcpConn.SetKeepAlivePeriod(30 * time.Second); err != nil {
			log.Warnf("Failed to set KeepAlivePeriod: %v", err)
		}
	}
	
	return rpc.NewClient(conn), nil
}

// ServerConnection represents a connection to another EPaxos replica
type ServerConnection struct {
	replicaID int
	addr      string
	mu        sync.Mutex   // protects rpcClient during reconnection
	rpcClient *rpc.Client
}

var conns = struct {
	sync.RWMutex
	m map[int]*ServerConnection
}{
	m: make(map[int]*ServerConnection),
}

// establishReplicaConnections creates RPC connections to all other EPaxos replicas
func establishReplicaConnections() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	const maxRetries = 10
	const retryDelay = 1 * time.Second

	log.Infof("Server %d: establishing connections to %d peers...", myServerID, numOfServers-1)

	for replicaID := 0; replicaID < numOfServers; replicaID++ {
		if replicaID == myServerID {
			continue // Skip self
		}

		addr := serverConfig[replicaID][ipIndex] + ":" + serverConfig[replicaID][rpcPortIndex]

		var client *rpc.Client
		var err error

		// Retry logic for robust connection establishment
		for attempt := 1; attempt <= maxRetries; attempt++ {
			client, err = dialServerRPC(addr, 5*time.Second)
			if err == nil {
				log.Infof("Server %d → Replica %d: connected on attempt %d | addr=%s",
					myServerID, replicaID, attempt, addr)
				break
			}

			if attempt < maxRetries {
				log.Warnf("Server %d → Replica %d: connection attempt %d/%d failed: %v | Retrying in %v...",
					myServerID, replicaID, attempt, maxRetries, err, retryDelay)
				time.Sleep(retryDelay)
			} else {
				log.Errorf("Server %d → Replica %d: failed after %d attempts: %v | Skipping",
					myServerID, replicaID, maxRetries, err)
			}
		}

		if err != nil {
			log.Warnf("Server %d: could not connect to replica %d - continuing without it", myServerID, replicaID)
			continue
		}

		// Store connection
		conns.Lock()
		conns.m[replicaID] = &ServerConnection{
			replicaID: replicaID,
			addr:      addr,
			rpcClient: client,
		}
		conns.Unlock()

		log.Infof("Server %d: established connection to replica %d at %s", myServerID, replicaID, addr)
	}

	conns.RLock()
	connectedReplicas := len(conns.m)
	conns.RUnlock()

	log.Infof("Server %d: connection establishment complete | connected to %d/%d replicas",
		myServerID, connectedReplicas, numOfServers-1)

	// Verify quorum connectivity
	if connectedReplicas < threshold {
		log.Warnf("Server %d: WARNING - connected to %d replicas, but need %d for quorum (F=%d)",
			myServerID, connectedReplicas, threshold+1, threshold)
	}
}

// cleanupConnections closes all replica connections
func cleanupConnections() {
	conns.Lock()
	defer conns.Unlock()

	for id, conn := range conns.m {
		if conn.rpcClient != nil {
			conn.rpcClient.Close()
		}
		delete(conns.m, id)
	}

	log.Infof("Server %d: all connections closed", myServerID)
}

// initMongoDB returns an error instead of just logging and returning as it
// used to: its sole caller (runServer) discarded the return entirely, so a
// failed connection left mongoDbFollower nil and the server proceeded to
// register/accept RPCs anyway. Worse than that alone: with no nil check
// here, a nil mongoDbFollower (NewMongoFollower returns nil on a client
// construction failure - see mongodb/mgdb_follower.go) meant the very next
// line, ClearTable -> FollowerAPI, dereferenced a nil receiver and panicked
// the whole server process. Every real MongoDB request that DID reach a
// live-but-broken server would then fail before ever recording anything, so
// server-side metrics looked silently empty with no clear reason why - same
// symptom found and fixed in woc's initMongoDB.
func initMongoDB() error {
	gob.Register([]mongodb.Query{})

	if mode == Localhost {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), myServerID)
	} else {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), 0)
	}

	if mongoDbFollower == nil {
		return fmt.Errorf("mongodb follower initialization failed")
	}

	queriesToLoad, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "workload.dat")
	if err != nil {
		return fmt.Errorf("getting load data failed: %w", err)
	}

	err = mongoDbFollower.ClearTable("usertable")
	if err != nil {
		return fmt.Errorf("clean up table failed: %w", err)
	}

	log.Debugf("loading data to MongoDB")
	_, _, err = mongoDbFollower.FollowerAPI(queriesToLoad)
	if err != nil {
		return fmt.Errorf("load data failed: %w", err)
	}

	log.Infof("MongoDB initialization done")
	return nil
}

// mongoDBCleanUp cleans up MongoDB connections on shutdown
func mongoDBCleanUp() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Debugf("clean up MongoDB follower")
		err := mongoDbFollower.CleanUp()
		if err != nil {
			log.Errorf("clean up MongoDB follower failed | err: %v", err)
			return
		}
		log.Infof("clean up MongoDB follower succeeded")
		os.Exit(1)
	}()
}