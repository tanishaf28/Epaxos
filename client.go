package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"epaxos/config"
	"epaxos/mongodb"
)

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

// AdaptiveLimiter manages dynamic concurrency control (PIPELINED mode only)
type AdaptiveLimiter struct {
	mu             sync.Mutex
	cond           *sync.Cond
	fastPathRatio  float64
	maxInflight    int
	minInflight    int
	currentLimit   int
	currentActive  int  // Actually enforced
	lastAdjustTime time.Time
	clientID       int
}

func NewAdaptiveLimiter(maxInflight int, clientID int) *AdaptiveLimiter {
	// WORKLOAD-ADAPTIVE initialization
	minInflight := 1
	initialLimit := maxInflight / 2  // Start conservative, ramp up
	initialFastRatio := 0.5          // Neutral assumption
	
	if maxInflight <= 5 {
		// SLOW WORKLOAD (conflict sweep 100%, fully slow path)
		minInflight = 1
		initialLimit = 2          // Start very low to avoid overload
		initialFastRatio = 0.1    // Expect slow path
	} else if maxInflight <= 15 {
		// MIXED WORKLOAD
		minInflight = 2
		initialLimit = maxInflight / 2
		initialFastRatio = 0.5
	} else {
		// FAST WORKLOAD (100% independent, fully fast path)
		minInflight = 5
		initialLimit = maxInflight * 3 / 4  // Start high
		initialFastRatio = 0.95  // Expect fast path
	}
	
	if maxInflight < minInflight {
		minInflight = maxInflight
	}
	if initialLimit < minInflight {
		initialLimit = minInflight
	}
	
	al := &AdaptiveLimiter{
		maxInflight:    maxInflight,
		minInflight:    minInflight,
		currentLimit:   initialLimit,
		currentActive:  0,
		fastPathRatio:  initialFastRatio,
		lastAdjustTime: time.Now(),
		clientID:       clientID,
	}
	al.cond = sync.NewCond(&al.mu)
	return al
}

func (al *AdaptiveLimiter) Acquire() {
	al.mu.Lock()
	for al.currentActive >= al.currentLimit {
		al.cond.Wait()  
	}
	al.currentActive++
	al.mu.Unlock()
}

func (al *AdaptiveLimiter) Release() {
	al.mu.Lock()
	if al.currentActive > 0 {
		al.currentActive--
	}
	al.mu.Unlock()
	al.cond.Signal()  // Wake one waiting goroutine
}

func (al *AdaptiveLimiter) AdjustLimit(pathUsed string, latencyMs int64) {
	al.mu.Lock()
	defer al.mu.Unlock()
	
	isFast := pathUsed == "FAST" || 
	          (len(pathUsed) >= 5 && pathUsed[:5] == "MIXED" && pathUsed != "MIXED(FAST:0,SLOW:0,HOT:0)")
	
	// Update fast path ratio with EMA
	if isFast {
		al.fastPathRatio = 0.85*al.fastPathRatio + 0.15*1.0
	} else {
		al.fastPathRatio = 0.85*al.fastPathRatio + 0.15*0.0
	}
	
	if time.Since(al.lastAdjustTime) < 100*time.Millisecond {
		return
	}
	al.lastAdjustTime = time.Now()
	
	oldLimit := al.currentLimit
	
	// WORKLOAD-ADAPTIVE adjustment strategy
	if al.maxInflight <= 5 {
		if al.fastPathRatio > 0.6 && latencyMs < 200 && al.currentLimit < al.maxInflight {
			al.currentLimit++  // Increment by 1 only
			al.cond.Signal()
		}
		// Aggressive backoff to protect serialized server
		if al.fastPathRatio < 0.4 || latencyMs > 300 {
			if al.currentLimit > al.minInflight {
				al.currentLimit--
			}
		}
		
	} else if al.maxInflight <= 15 {
		if al.fastPathRatio > 0.75 && latencyMs < 100 && al.currentLimit < al.maxInflight {
			increase := 2
			if al.currentLimit + increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		} else if al.fastPathRatio > 0.60 && latencyMs < 150 && al.currentLimit < al.maxInflight {
			al.currentLimit++
			al.cond.Signal()
		}
		// Normal backoff
		if (al.fastPathRatio < 0.4 || latencyMs > 250) && al.currentLimit > al.minInflight {
			al.currentLimit--
		}
		
	} else {
		if al.fastPathRatio > 0.85 && latencyMs < 50 && al.currentLimit < al.maxInflight {
			increase := 5  // Jump by 5
			if al.currentLimit + increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		} else if al.fastPathRatio > 0.70 && latencyMs < 100 && al.currentLimit < al.maxInflight {
			increase := 2
			if al.currentLimit + increase > al.maxInflight {
				increase = al.maxInflight - al.currentLimit
			}
			al.currentLimit += increase
			for i := 0; i < increase; i++ {
				al.cond.Signal()
			}
		}
		// Gentle backoff (fast path issues may be temporary)
		if (al.fastPathRatio < 0.3 || latencyMs > 200) && al.currentLimit > al.minInflight {
			decrease := 2
			for i := 0; i < decrease && al.currentLimit > al.minInflight; i++ {
				al.currentLimit--
			}
		}
	}
	
	if oldLimit != al.currentLimit {
		log.Debugf("[Client %d] Limit %d→%d (active=%d, fast=%.0f%%, lat=%dms)", 
			al.clientID, oldLimit, al.currentLimit, al.currentActive, 
			al.fastPathRatio*100, latencyMs)
	}
}

func (al *AdaptiveLimiter) GetStats() (int, float64) {
	al.mu.Lock()
	defer al.mu.Unlock()
	return al.currentLimit, al.fastPathRatio
}

// Helper function to record batch metrics (WOC-compatible)
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
	pipelined := os.Getenv("PIPELINE_MODE") == "true"
	maxInflight := 5
	if val := os.Getenv("MAX_INFLIGHT"); val != "" {
		if n, err := fmt.Sscanf(val, "%d", &maxInflight); err == nil && n == 1 && maxInflight > 0 {
			// Valid
		} else {
			maxInflight = 5
		}
	}

	var shuttingDown atomic.Bool
	var inflightOps atomic.Int64
	var metricsSaved atomic.Bool
	
	var limiter *AdaptiveLimiter
	
	if pipelined {
		limiter = NewAdaptiveLimiter(maxInflight, clientID)
		log.Infof("Client %d: PIPELINED mode with adaptive limiting (max %d concurrent batches)", 
			clientID, maxInflight)
	} else {
		log.Infof("Client %d: SEQUENTIAL mode enabled (ordered batches)", clientID)
	}

	// Parse cluster configuration
	clusterConf := config.ParseClusterConfig(numOfServers, configPath)
	cluster := make(map[int]string)
	for sid, info := range clusterConf {
		cluster[sid] = info[config.ServerIP] + ":" + info[config.ServerRPCListenerPort]
	}

	// Connect to servers
	conns := make(map[int]*rpc.Client)
	for sid, addr := range cluster {
		if sid >= numOfServers {
			continue
		}
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Warnf("Client %d: failed to connect to server %d (%s): %v", clientID, sid, addr, err)
			continue
		}
		conns[sid] = c
		log.Infof("Client %d: connected to server %d at %s", clientID, sid, addr)
	}
	if len(conns) == 0 {
		log.Fatalf("Client %d: no server connections available", clientID)
	}

	// Shutdown handler
	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	go func() {
		sig := <-sigChan
		log.Infof("Client %d: Received signal %v", clientID, sig)
		fmt.Printf("\nClient %d: Received signal %v - starting graceful shutdown...\n", clientID, sig)
		
		shuttingDown.Store(true)
		
		// Wait for in-flight ops
		deadline := time.Now().Add(30 * time.Second)
		lastReport := time.Now()
		for {
			inflight := inflightOps.Load()
			if inflight == 0 {
				log.Infof("Client %d: All in-flight operations completed", clientID)
				fmt.Printf("Client %d: All in-flight operations completed\n", clientID)
				break
			}
			
			if time.Now().After(deadline) {
				log.Warnf("Client %d: Timeout waiting for %d in-flight operations", clientID, inflight)
				fmt.Printf("Client %d: Timeout - %d operations still in flight\n", clientID, inflight)
				break
			}
			
			if time.Since(lastReport) >= 2*time.Second {
				log.Infof("Client %d: Waiting for %d in-flight operations...", clientID, inflight)
				fmt.Printf("Client %d: Waiting for %d in-flight operations...\n", clientID, inflight)
				lastReport = time.Now()
			}
			time.Sleep(100 * time.Millisecond)
		}
		
		// Save metrics
		log.Infof("Client %d: Saving metrics...", clientID)
		fmt.Printf("Client %d: Saving metrics (Fast=%d, Slow=%d, Conflicts=%d)...\n", 
			clientID, perfM.FastCommits, perfM.SlowCommits, perfM.ConflictCommits)
		
		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Client %d: Failed to save metrics: %v", clientID, err)
			fmt.Printf("Client %d: ERROR saving metrics: %v\n", clientID, err)
		} else {
			metricsSaved.Store(true)
			log.Infof("Client %d: Metrics saved successfully", clientID)
			fmt.Printf("Client %d: Metrics saved to ./eval/client%d/\n", clientID, clientID)
		}
		
		time.Sleep(2 * time.Second)
		
		// Close connections
		for _, c := range conns {
			c.Close()
		}
		
		log.Infof("Client %d: Shutdown complete", clientID)
		fmt.Printf("Client %d: Shutdown complete\n", clientID)
		
		os.Exit(0)
	}()

	// Initialize performance meter
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
	serverIDs := make([]int, 0, len(conns))
	for sid := range conns {
		serverIDs = append(serverIDs, sid)
	}

	opsLabel := fmt.Sprintf("%d", numOps)
	if numOps <= 0 {
		opsLabel = "infinite"
	}

	totalRatio := float64(conflictRate) + indepRatio + commonRatio
	if totalRatio != 100.0 {
		log.Warnf("Client %d: Object type ratios don't add to 100%% (Hot=%d%% + Indep=%.1f%% + Common=%.1f%% = %.1f%%)",
			clientID, conflictRate, indepRatio, commonRatio, totalRatio)
	}

	fmt.Printf("Client %d started | Total Ops: %s | Batch Size: %d | Composition: %s\n",
		clientID, opsLabel, batchsize, batchComposition)
	fmt.Printf("  Object Distribution: Hot=%d%% | Independent=%.0f%% | Common=%.0f%% (Total=%.0f%%)\n",
		conflictRate, indepRatio, commonRatio, totalRatio)

	log.Infof("Client %d: Batch composition mode: %s", clientID, batchComposition)

	// Preload MongoDB queries if needed
	var mongoDBQueries []mongodb.Query
	if evalType == MongoDB {
		filePath := fmt.Sprintf("%srun_workload%s.dat", mongodb.DataPath, mongoLoadType)
		var err error
		mongoDBQueries, err = mongodb.ReadQueryFromFile(filePath)
		if err != nil {
			log.Errorf("ReadQueryFromFile failed | err: %v", err)
			return
		}
	}

	// Job queue for sequential mode
	jobQ := make(map[int]map[int]chan struct{})
	for sid := range conns {
		jobQ[sid] = make(map[int]chan struct{})
	}

	serverIdx := 0
	op := 0

	// Main operation loop
	infinite := numOps <= 0
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
		perfM.RecordStarter(CClock)

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
					cmd.ObjIDs[b] = fmt.Sprintf("obj-HOT-%d", (op+b)%10)
				} else if randVal < float64(conflictRate)+indepRatio {
					cmd.ObjTypes[b] = IndependentObject
					cmd.ObjIDs[b] = fmt.Sprintf("obj-indep-%d-%d", clientID, op+b)
				} else {
					cmd.ObjTypes[b] = CommonObject
					cmd.ObjIDs[b] = fmt.Sprintf("obj-common-%d", (op+b)/10)
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
				objID = fmt.Sprintf("obj-HOT-%d", op%10)
			} else if randVal < float64(conflictRate)+indepRatio {
				objType = IndependentObject
				objID = fmt.Sprintf("obj-indep-%d-%d", clientID, op)
			} else {
				objType = CommonObject
				objID = fmt.Sprintf("obj-common-%d", op/10)
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
					cmd.ObjIDs[b] = fmt.Sprintf("obj-HOT-%d", (op+b)%10)
				case IndependentObject:
					cmd.ObjIDs[b] = fmt.Sprintf("obj-indep-%d-%d", clientID, op+b)
				case CommonObject:
					cmd.ObjIDs[b] = fmt.Sprintf("obj-common-%d", (op+b)/10)
				}
			}
			cmd.ObjID = cmd.ObjIDs[0]
		}

		// Populate payload
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

		// Select server (round-robin)
		sid := serverIDs[serverIdx%len(serverIDs)]
		conn := conns[sid]

		if pipelined {
			limiter.Acquire()

			go func(clockVal int, connection *rpc.Client, command *ClientArgs, serverID int, batchSize int) {
				inflightOps.Add(1)
				defer func() {
					inflightOps.Add(-1)
					limiter.Release()
					perfM.RecordFinisher(clockVal)
				}()

				if shuttingDown.Load() {
					return
				}

				reply := &ClientReply{}
				start := time.Now()
				
				done := make(chan error, 1)
				go func() {
					done <- connection.Call("EPaxosService.ConsensusService", command, reply)
				}()
				
				select {
				case err := <-done:
					latency := time.Since(start)
					if err != nil {
						atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
						for b := 0; b < batchSize; b++ {
							perfM.IncConflict(clockVal)
						}
					} else {
						recordBatchMetrics(reply, clockVal, batchSize)
						limiter.AdjustLimit(reply.PathUsed, latency.Milliseconds())
					}
				case <-time.After(10 * time.Second):
					atomic.AddInt64(&perfM.ConflictCommits, int64(batchSize))
					for b := 0; b < batchSize; b++ {
						perfM.IncConflict(clockVal)
					}
				}

				if clockVal%100 == 0 {
					limit, fastRatio := limiter.GetStats()
					log.Infof("[Client %d] Batch %d | limit=%d | fast=%.0f%% | size=%d",
						clientID, clockVal, limit, fastRatio*100, batchSize)
				}
			}(cmd.ClientClock, conn, cmd, sid, currentBatch)

		} else {
			// Sequential mode
			stack := make(chan struct{}, 1)
			jobQ[sid][cmd.ClientClock] = stack
			if prev, ok := jobQ[sid][cmd.ClientClock-1]; ok && cmd.ClientClock > 1 {
				<-prev
				delete(jobQ[sid], cmd.ClientClock-1)
			}

			reply := &ClientReply{}
			start := time.Now()
			err := conn.Call("EPaxosService.ConsensusService", cmd, reply)
			latency := time.Since(start)
			stack <- struct{}{}

			if err != nil {
				atomic.AddInt64(&perfM.ConflictCommits, int64(currentBatch))
				for b := 0; b < currentBatch; b++ {
					perfM.IncConflict(cmd.ClientClock)
				}
			} else {
				recordBatchMetrics(reply, cmd.ClientClock, currentBatch)
			}

			perfM.RecordFinisher(cmd.ClientClock)
			log.Infof("[Client %d] Batch %d | size=%d | server=%d | %s | lat=%dms",
				clientID, cmd.ClientClock, currentBatch, sid, reply.PathUsed, latency.Milliseconds())
		}

		op += currentBatch
		serverIdx++
	}

	// Cleanup for finite mode
	if !infinite {
		if pipelined {
			deadline := time.Now().Add(30 * time.Second)
			for inflightOps.Load() > 0 {
				if time.Now().After(deadline) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		
		if err := perfM.SaveToFile(); err != nil {
			log.Errorf("Client %d: failed to save metrics: %v", clientID, err)
		} else {
			log.Infof("Client %d: saved performance metrics", clientID)
		}
	} else {
		// Infinite mode: wait for signal handler
		log.Infof("Client %d: Main loop exited, waiting for signal handler...", clientID)
		for !metricsSaved.Load() {
			time.Sleep(100 * time.Millisecond)
		}
	}
}