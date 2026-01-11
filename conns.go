package main

import (
	"encoding/gob"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"epaxos/config"
	"epaxos/mongodb"
    "net/rpc"
)

// ServerConnection represents a connection to another EPaxos replica
type ServerConnection struct {
	replicaID int
	addr      string
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
			client, err = rpc.Dial("tcp", addr)
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

// MongoDB initialization (same as WOC)
func initMongoDB() {
	gob.Register([]mongodb.Query{})

	if mode == Localhost {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), myServerID)
	} else {
		mongoDbFollower = mongodb.NewMongoFollower(mongoClientNum, int(1), 0)
	}

	queriesToLoad, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "workload.dat")
	if err != nil {
		log.Errorf("getting load data failed | error: %v", err)
		return
	}

	err = mongoDbFollower.ClearTable("usertable")
	if err != nil {
		log.Errorf("clean up table failed | err: %v", err)
		return
	}

	log.Debugf("loading data to MongoDB")
	_, _, err = mongoDbFollower.FollowerAPI(queriesToLoad)
	if err != nil {
		log.Errorf("load data failed | error: %v", err)
		return
	}

	log.Infof("MongoDB initialization done")
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