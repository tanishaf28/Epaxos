package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	
	"epaxos/config"
	"epaxos/mongodb"
)

var (
	hotObjIDs     [10]string
	indepObjIDs   [100000]string
	commonObjIDs  [10000]string
	objIDPoolInit sync.Once
)


func initObjectIDPool(clientID int) {
	objIDPoolInit.Do(func() {
		for i := 0; i < 10; i++ {
			hotObjIDs[i] = fmt.Sprintf("obj-HOT-%d", i)
		}
		for i := 0; i < 100000; i++ {
			indepObjIDs[i] = fmt.Sprintf("obj-indep-%d-%d", clientID, i)
		}
		for i := 0; i < 10000; i++ {
			commonObjIDs[i] = fmt.Sprintf("obj-common-%d", i)
		}
		log.Infof("[Client %d] Pre-generated object ID pools (10 hot keys, 100k indep, 10k common)", clientID)
	})
}

// dialClientRPC creates an optimized RPC connection for clients
func dialClientRPC(address string, timeout time.Duration) (*rpc.Client, error) {
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


type ChannelLimiter struct {
	slots    chan struct{}
	clientID int
}

func NewChannelLimiter(maxInflight int, clientID int) *ChannelLimiter {
	cl := &ChannelLimiter{
		slots:    make(chan struct{}, maxInflight),
		clientID: clientID,
	}
	// Pre-fill slots
	for i := 0; i < maxInflight; i++ {
		cl.slots <- struct{}{}
	}
	return cl
}

func (cl *ChannelLimiter) Acquire() {
	<-cl.slots // blocks until a slot is free
}

func (cl *ChannelLimiter) Release() {
	cl.slots <- struct{}{}
}

// recordBatchMetrics updates perf counters based on RPC reply path
func recordBatchMetrics(reply *ClientReply, clockVal int, batchSize int) {
	// Handle MIXED batches (e.g., "MIXED(FAST:5,SLOW:3,HOT:2)")
	if len(reply.PathUsed) >= 5 && reply.PathUsed[:5] == "MIXED" {
		var fastOps, slowOps, hotOps int
		n, _ := fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d,HOT:%d)", &fastOps, &slowOps, &hotOps)
		if n < 3 {
			// Fallback for MIXED without HOT ops
			fmt.Sscanf(reply.PathUsed, "MIXED(FAST:%d,SLOW:%d)", &fastOps, &slowOps)
		}
		
		// Update global counters
		if fastOps > 0 {
			atomic.AddInt64(&perfM.FastCommits, int64(fastOps))
		}
		if slowOps > 0 {
			atomic.AddInt64(&perfM.SlowCommits, int64(slowOps))
		}
		if hotOps > 0 {
			atomic.AddInt64(&perfM.SlowCommits, int64(hotOps))
			atomic.AddInt64(&perfM.ConflictCommits, int64(hotOps))
		}
		
		// Update per-batch metrics
		for i := 0; i < fastOps && i < batchSize; i++ {
			perfM.IncFastPath(clockVal)
		}
		for i := 0; i < slowOps && i < batchSize; i++ {
			perfM.IncSlowPath(clockVal)
		}
		for i := 0; i < hotOps && i < batchSize; i++ {
			perfM.IncConflict(clockVal)
		}
		return
	}
	
	// Handle HOT-only batches (e.g., "HOT:10")
	if len(reply.PathUsed) >= 3 && reply.PathUsed[:3] == "HOT" {
		var hotOps int
		fmt.Sscanf(reply.PathUsed, "HOT:%d", &hotOps)
		atomic.AddInt64(&perfM.SlowCommits, int64(hotOps))
		atomic.AddInt64(&perfM.ConflictCommits, int64(hotOps))
		for i := 0; i < hotOps && i < batchSize; i++ {
			perfM.IncConflict(clockVal)
		}
		return
	}
	
	// Handle simple single-path batches (FAST, SLOW, etc.)
	switch reply.PathUsed {
	case "FAST":
		atomic.AddInt64(&perfM.FastCommits, int64(batchSize))
		for b := 0; b < batchSize; b++ {
			perfM.IncFastPath(clockVal)
		}
	case "SLOW":
		atomic.AddInt64(&perfM.SlowCommits, int64(batchSize))
		for b := 0; b < batchSize; b++ {
			perfM.IncSlowPath(clockVal)
		}
	default:
		// Unknown path or error - count as conflict
		atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
		for b := 0; b < batchSize; b++ {
			perfM.IncConflict(clockVal)
		}
	}
}

// RunClient sends operations to replicas
func RunClient(clientID int, configPath string, numOps int) {
	initObjectIDPool(clientID)
	
	pipelined := os.Getenv("PIPELINE_MODE") == "true"
	maxInflight := 4
	if val := os.Getenv("MAX_INFLIGHT"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxInflight); err != nil || n != 1 || maxInflight < 1 {
			maxInflight = 4
		}
	}

	var shuttingDown atomic.Bool
	var inflightOps atomic.Int64
	var metricsSaved atomic.Bool
	var limiter *ChannelLimiter
	if pipelined {
		limiter = NewChannelLimiter(maxInflight, clientID)
		log.Infof("Client %d: PIPELINED mode | max-in-flight=%d", clientID, maxInflight)
	} else {
		log.Infof("Client %d: SEQUENTIAL mode", clientID)
	}

	// Parse cluster configuration
	clusterConf := config.ParseClusterConfig(numOfServers, configPath)
	cluster := make(map[int]string)
	for sid, info := range clusterConf {
		cluster[sid] = info[config.ServerIP] + ":" + info[config.ServerRPCListenerPort]
	}

	// Connect to servers
	clientConns := make(map[int]*rpc.Client)
	for sid, addr := range cluster {
		if sid >= numOfServers {
			continue
		}
		c, err := dialClientRPC(addr, 5*time.Second)
		if err != nil {
			log.Warnf("Client %d: failed to connect to server %d (%s): %v", clientID, sid, addr, err)
			continue
		}
		clientConns[sid] = c
		log.Infof("Client %d: connected to server %d at %s [TCP_NODELAY enabled]", clientID, sid, addr)
		
		// FAIRNESS: Pre-warm connection with ping (eliminates first-request TCP handshake cost, like CORA)
		pingArgs := &ClientArgs{
			ClientID:    clientID,
			ClientClock: 0,
			CmdType:     READ,  // Use READ for ping (no-op)
			IsMixed:     false,
		}
		pingReply := &ClientReply{}
		pingStart := time.Now()
		if err := c.Call("EPaxosService.Ping", pingArgs, pingReply); err != nil {
			log.Warnf("Client %d: Server %d ping failed: %v (will retry during operations)", clientID, sid, err)
		} else {
			rtt := time.Since(pingStart)
			log.Infof("Client %d: Server %d connection pre-warmed (RTT=%v)", clientID, sid, rtt)
		}
	}
	if len(clientConns) == 0 {
		log.Fatalf("Client %d: no server connections", clientID)
	}

	// Signal handler — saves metrics on SIGTERM
	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Infof("Client %d: signal %v received — graceful shutdown", clientID, sig)
		shuttingDown.Store(true)
		// Individual RPC timeout is 15s. Give 20s max to drain.
		deadline := time.Now().Add(20 * time.Second)
		for inflightOps.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}

		remaining := inflightOps.Load()
		if remaining > 0 {
			log.Warnf("Client %d: %d ops still in-flight at deadline — saving partial metrics", clientID, remaining)
		}
		log.Infof("Client %d: saving metrics (Fast=%d Slow=%d Conflict=%d Timeout=%d)...",
			clientID, perfM.FastCommits, perfM.SlowCommits, perfM.ConflictCommits, perfM.TimeoutCommits)

		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Client %d: SaveToFile failed: %v", clientID, err)
			// Try writing a minimal error marker so we know it ran
			os.WriteFile(fmt.Sprintf("./eval/client%d/SAVE_FAILED.txt", clientID),
				[]byte(fmt.Sprintf("save failed: %v\n", err)), 0644)
		} else {
			metricsSaved.Store(true)
			log.Infof("Client %d: metrics saved", clientID)
		}

		time.Sleep(500 * time.Millisecond) // flush file buffers
		os.Exit(0)
	}()

	// Memory monitor goroutine — logs stats every 30s to help diagnose OOM
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				var ms runtime.MemStats
				runtime.ReadMemStats(&ms)
				numGoroutines := runtime.NumGoroutine()
				log.Infof("[Client %d] Memory: HeapAlloc=%.1fMB HeapSys=%.1fMB Goroutines=%d InFlight=%d",
					clientID,
					float64(ms.HeapAlloc)/1024/1024,
					float64(ms.HeapSys)/1024/1024,
					numGoroutines,
					inflightOps.Load())

				// SAFETY: if goroutine count is exploding, something is wrong
				if numGoroutines > 50000 {
					log.Errorf("[Client %d] GOROUTINE LEAK DETECTED: %d goroutines — forcing GC", clientID, numGoroutines)
					runtime.GC()
				}
			}
		}
	}()

	// Init perf meter
	var clientClock int
	var clockLock sync.Mutex
	incrementClock := func() int {
		clockLock.Lock()
		defer clockLock.Unlock()
		clientClock++
		return clientClock
	}

	fileSuffix := fmt.Sprintf("client%d_epaxos", clientID)
	perfM.Init(1, batchsize, fileSuffix)

	rand.Seed(time.Now().UnixNano() + int64(clientID))
	serverIDs := make([]int, 0, len(clientConns))
	for sid := range clientConns {
		serverIDs = append(serverIDs, sid)
	}

	// MongoDB preload
	var mongoDBQueries []mongodb.Query
	if evalType == MongoDB {
		filePath := fmt.Sprintf("%srun_workload%s.dat", mongodb.DataPath, mongoLoadType)
		var err error
		mongoDBQueries, err = mongodb.ReadQueryFromFile(filePath)
		if err != nil {
			log.Errorf("ReadQueryFromFile failed: %v", err)
			return
		}
	}

	jobQ := make(map[int]chan struct{}) // sequential mode only
	serverIdx := 0
	op := 0
	infinite := numOps <= 0

	// =========================================================================
	// MAIN OPERATION LOOP
	// =========================================================================
	for infinite || op < numOps {
		if shuttingDown.Load() {
			log.Infof("Client %d: Shutdown requested, stopping after %d operations", clientID, op)
			break
		}

		currentBatch := batchsize
		if !infinite && op+batchsize > numOps {
			currentBatch = numOps - op
		}

		CClock := incrementClock()

		cmd := &ClientArgs{
			ClientID:    clientID,
			ClientClock: CClock,
			CmdType:     WRITE,
			Type:        evalType,
		}

		// Batch composition logic (matching WOC pattern)
		if batchComposition == "mixed" {
			cmd.IsMixed = true
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				randVal := rand.Float64() * 100
				if randVal < float64(conflictRate) {
					cmd.ObjTypes[b] = HotObject
					// BUG FIX #9: Use pre-generated pool
					cmd.ObjIDs[b] = hotObjIDs[(op+b)%10]
				} else if randVal < float64(conflictRate)+indepRatio {
					cmd.ObjTypes[b] = IndependentObject
					// BUG FIX #9: Use pre-generated pool
					cmd.ObjIDs[b] = indepObjIDs[(op+b)%100000]
				} else {
					cmd.ObjTypes[b] = CommonObject
					// BUG FIX #9: Use pre-generated pool
					cmd.ObjIDs[b] = commonObjIDs[((op+b)/10)%10000]
				}
			}
			cmd.ObjID = cmd.ObjIDs[0]
			cmd.ObjType = cmd.ObjTypes[0]
		} else if batchComposition == "single_obj" {
			cmd.IsMixed = false
			randVal := rand.Float64() * 100
			var objType int
			var objID string
			if randVal < float64(conflictRate) {
				objType = HotObject
				// BUG FIX #9: Use pre-generated pool
				objID = hotObjIDs[op%10]
			} else if randVal < float64(conflictRate)+indepRatio {
				objType = IndependentObject
				// BUG FIX #9: Use pre-generated pool
				objID = indepObjIDs[op%100000]
			} else {
				objType = CommonObject
				// BUG FIX #9: Use pre-generated pool
				objID = commonObjIDs[(op/10)%10000]
			}
			cmd.ObjType = objType
			cmd.ObjID = objID
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				cmd.ObjTypes[b] = objType
				cmd.ObjIDs[b] = objID
			}
		} else { // "object-specific"
			cmd.IsMixed = false
			randVal := rand.Float64() * 100
			var objType int
			if randVal < float64(conflictRate) {
				objType = HotObject
			} else if randVal < float64(conflictRate)+indepRatio {
				objType = IndependentObject
			} else {
				objType = CommonObject
			}
			cmd.ObjType = objType
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				cmd.ObjTypes[b] = objType
				switch objType {
				case HotObject:
					cmd.ObjIDs[b] = hotObjIDs[(op+b)%10]
				case IndependentObject:
					cmd.ObjIDs[b] = indepObjIDs[(op+b)%100000]
				case CommonObject:
					cmd.ObjIDs[b] = commonObjIDs[((op+b)/10)%10000]
				}
			}
			cmd.ObjID = cmd.ObjIDs[0]
		}
		switch evalType {
		case PlainMsg:
			batch := make([][]byte, currentBatch)
			for b := 0; b < currentBatch; b++ {
				batch[b] = genRandomBytes(msgsize)
			}
			cmd.CmdPlain = batch
		case MongoDB:
			batch := make([]mongodb.Query, currentBatch)
			for b := 0; b < currentBatch; b++ {
				batch[b] = mongoDBQueries[(op+b)%len(mongoDBQueries)]
			}
			cmd.CmdMongo = batch
		}

		sid := serverIDs[serverIdx%len(serverIDs)]
		conn := clientConns[sid]

		if pipelined {
			limiter.Acquire() // BLOCKS main thread until a slot is free

			if shuttingDown.Load() {
				limiter.Release()
				break
			}

			inflightOps.Add(1)
			go func(clockVal int, connection *rpc.Client, command *ClientArgs, serverID int, bs int) {
				defer func() {
					limiter.Release()
					inflightOps.Add(-1)
				}()

				reply := &ClientReply{}

				// Timer starts immediately — limiter wait already done before goroutine launch
				perfM.RecordStarter(clockVal)

				done := make(chan error, 1)
				go func() {
					done <- connection.Call("EPaxosService.ConsensusService", command, reply)
				}()

				select {
				case err := <-done:
					if err != nil {
						log.Warnf("[Client %d] RPC error server=%d: %v", clientID, serverID, err)
						atomic.AddInt64(&perfM.NetworkErrors, int64(bs))
					} else {
						perfM.RecordFinisher(clockVal)
						recordBatchMetrics(reply, clockVal, bs)
					}
				case <-time.After(15 * time.Second):
					atomic.AddInt64(&perfM.TimeoutCommits, int64(bs))
					log.Warnf("[Client %d] RPC TIMEOUT (15s) batch=%d server=%d", clientID, clockVal, serverID)
				}

				if clockVal%500 == 0 {
					log.Infof("[Client %d] Batch %d | inflight=%d | fast=%d slow=%d",
						clientID, clockVal, inflightOps.Load(), perfM.FastCommits, perfM.SlowCommits)
				}
			}(CClock, conn, cmd, sid, currentBatch)

		} else {
			// Sequential mode — unchanged, correct
			stack := make(chan struct{}, 1)
			jobQ[CClock] = stack
			if prev, ok := jobQ[CClock-1]; ok && CClock > 1 {
				<-prev
				delete(jobQ, CClock-1)
			}

			reply := &ClientReply{}
			perfM.RecordStarter(CClock)
			err := conn.Call("EPaxosService.ConsensusService", cmd, reply)
			perfM.RecordFinisher(CClock)
			stack <- struct{}{}

			if err != nil {
				atomic.AddInt64(&perfM.NetworkErrors, int64(currentBatch))
			} else {
				recordBatchMetrics(reply, CClock, currentBatch)
			}
		}

		op += currentBatch
		serverIdx++
	}

	// Finite mode cleanup
	if !infinite {
		log.Infof("[Client %d] All %d ops sent, draining...", clientID, op)
		deadline := time.Now().Add(20 * time.Second)
		for inflightOps.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(1 * time.Second)

		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Client %d: SaveToFile failed: %v", clientID, err)
		} else {
			log.Infof("Client %d: metrics saved", clientID)
		}
	} else {
		// Infinite mode: block until signal handler saves and exits
		for !metricsSaved.Load() {
			time.Sleep(100 * time.Millisecond)
		}
	}
}