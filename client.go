package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"strings"
	
	"epaxos/config"
	"epaxos/mongodb"
)

// hotDependentObjID is the single, fixed, cluster-wide-shared key every
// DependentObject-classified op targets — matching real efficient/epaxos's
// own -c workload generator exactly (client/client.go: karray[i] = 42 for
// the conflicting share, a literal single shared key), not a probabilistic
// pool. -indep's ratio still decides the conflict PROBABILITY (pickObjType
// below), same as before; only the DEPENDENT share's key selection changed,
// from "uniform-random pick within a 100-key pool" to "always this one
// key" - real, concurrent contention on one shared key is what actually
// drives EPaxos's own PreAccept interference detection to disagree and
// fall back to Accept, rather than a protocol-level override forcing it.
const hotDependentObjID = "obj-hot-0"

var (
	indepObjIDs   []string
	objIDPoolInit sync.Once
)

// initObjectIDPool builds the independent-object pool, sized by -numobjects
// and -indep, mirroring WOC's objectmap.go InitObjectRegistry: independent
// IDs are namespaced per client so they can never collide across clients
// (single-writer approximation). Dependent ops don't draw from a pool at
// all anymore - see hotDependentObjID.
func initObjectIDPool(clientID int) {
	objIDPoolInit.Do(func() {
		indepCount := int(float64(numObjects) * indepRatio / 100.0)
		if indepCount < 0 {
			indepCount = 0
		}
		if indepCount > numObjects {
			indepCount = numObjects
		}
		if indepCount < 1 {
			indepCount = 1 // always keep at least one independent key to pick from
		}

		indepObjIDs = make([]string, indepCount)
		for i := 0; i < indepCount; i++ {
			indepObjIDs[i] = fmt.Sprintf("obj-indep-%d-%d", clientID, i)
		}
		log.Infof("[Client %d] Pre-generated independent object ID pool (numobjects=%d, indep=%d) | dependent share -> single hot key %q",
			clientID, numObjects, indepCount, hotDependentObjID)
	})
}

// pickObjType rolls IndependentObject with probability indepRatio%, DependentObject otherwise.
func pickObjType() int {
	if rand.Float64()*100 < indepRatio {
		return IndependentObject
	}
	return DependentObject
}

// pickObjID returns hotDependentObjID for DependentObject (every dependent
// op targets the same single shared key, matching real EPaxos's -c
// mechanism), or a uniform-random pick within the independent pool
// otherwise.
func pickObjID(objType int) string {
	if objType == IndependentObject {
		return indepObjIDs[rand.Intn(len(indepObjIDs))]
	}
	return hotDependentObjID
}

// pickCmdType rolls READ with probability readRatio%, WRITE otherwise — matches
// WOC's per-iteration read/write coin flip (client.go:860-968 in WOC).
func pickCmdType() CmdType {
	if rand.Float64()*100 < readRatio {
		return READ
	}
	return WRITE
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

// isConnectionError distinguishes a transport-level failure (server
// unreachable/down) from a normal application-level rejection. Go's
// net/rpc returns a non-nil error from Call in both cases, but a
// server-side handler returning a non-nil error (e.g. a conflict under
// contention — a perfectly healthy server) comes back wrapped as
// rpc.ServerError; anything else (broken pipe, connection reset,
// rpc.ErrShutdown, timeout) means the connection itself is bad. Only the
// latter should evict a server from the client's rotation.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(rpc.ServerError); ok {
		return false
	}
	return true
}

// ServerPool tracks which servers this client currently considers
// reachable. A client is always pinned to one server ID, but if that
// server dies mid-run this lets it fail over to another currently-healthy
// server instead of routing requests into a dead connection forever.
type ServerPool struct {
	mu       sync.RWMutex
	conns    map[int]*rpc.Client // every server this client ever connected to
	healthy  []int
	idx      atomic.Uint64
	clientID int
}

func NewServerPool(conns map[int]*rpc.Client, clientID int) *ServerPool {
	p := &ServerPool{conns: conns, clientID: clientID}
	for sid := range conns {
		p.healthy = append(p.healthy, sid)
	}
	sort.Ints(p.healthy)
	return p
}

// Conn returns the (possibly stale) connection for a specific server,
// regardless of whether it's currently marked healthy.
func (p *ServerPool) Conn(sid int) (*rpc.Client, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.conns[sid]
	return c, ok
}

// Pick round-robins among currently-healthy servers — used only as the
// failover target when a pinned server is down, never as a top-level
// dispatch mode.
func (p *ServerPool) Pick() (int, *rpc.Client, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.healthy) == 0 {
		return 0, nil, false
	}
	i := p.idx.Add(1)
	sid := p.healthy[i%uint64(len(p.healthy))]
	return sid, p.conns[sid], true
}

// PickPinned returns the connection to serverID if it is currently healthy,
// falling back to round-robin among healthy servers if that server is
// unreachable.
func (p *ServerPool) PickPinned(serverID int) (int, *rpc.Client, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, id := range p.healthy {
		if id == serverID {
			return serverID, p.conns[serverID], true
		}
	}
	// Pinned server not healthy — fall back to round-robin
	if len(p.healthy) == 0 {
		return 0, nil, false
	}
	i := p.idx.Add(1)
	sid := p.healthy[i%uint64(len(p.healthy))]
	return sid, p.conns[sid], true
}

func (p *ServerPool) MarkDown(sid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, id := range p.healthy {
		if id == sid {
			p.healthy = append(p.healthy[:i:i], p.healthy[i+1:]...)
			log.Warnf("[Client %d] Server %d unreachable, removed from rotation (%d healthy remain)",
				p.clientID, sid, len(p.healthy))
			return
		}
	}
}

func (p *ServerPool) MarkUp(sid int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range p.healthy {
		if id == sid {
			return
		}
	}
	if _, ok := p.conns[sid]; ok {
		p.healthy = append(p.healthy, sid)
		log.Infof("[Client %d] Server %d reachable again, re-added to rotation", p.clientID, sid)
	}
}

// startReviver periodically pings every server not currently in rotation
// and re-admits any that respond, so a recovered server rejoins instead of
// staying evicted for the rest of the run.
func (p *ServerPool) startReviver(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			p.mu.RLock()
			down := make([]int, 0)
			for sid := range p.conns {
				found := false
				for _, h := range p.healthy {
					if h == sid {
						found = true
						break
					}
				}
				if !found {
					down = append(down, sid)
				}
			}
			p.mu.RUnlock()

			for _, sid := range down {
				conn := p.conns[sid]
				reply := &ClientReply{}
				if err := conn.Call("EPaxosService.Ping", &ClientArgs{CmdType: READ}, reply); err == nil {
					p.MarkUp(sid)
				}
			}
		}
	}()
}

// callWithFailover issues the RPC against startSID, and on a
// connection-level failure (isConnectionError), evicts that server and
// retries on another currently-healthy one — bounded to one attempt per
// known server so a fully-dead cluster still returns instead of looping
// forever. Returns the server actually used, for logging/metrics.
func callWithFailover(pool *ServerPool, startSID int, cmd *ClientArgs, reply *ClientReply) (usedSID int, err error) {
	sid := startSID
	conn, ok := pool.Conn(sid)
	if !ok {
		sid, conn, ok = pool.Pick()
		if !ok {
			return startSID, fmt.Errorf("no servers available")
		}
	}

	tried := make(map[int]bool)
	for {
		tried[sid] = true
		*reply = ClientReply{}
		err = conn.Call("EPaxosService.ConsensusService", cmd, reply)
		if !isConnectionError(err) {
			return sid, err
		}

		pool.MarkDown(sid)
		nextSID, nextConn, ok := pool.Pick()
		if !ok || tried[nextSID] {
			return sid, err
		}
		sid, conn = nextSID, nextConn
	}
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

func readTimelineEvent(clientID int) string {
	eventPath := fmt.Sprintf("./eval/client%d/.event", clientID)
	data, err := os.ReadFile(eventPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func mongoQueryObjectID(q mongodb.Query) string {
	if q.Table == "" {
		return "mongo:unknown"
	}

	switch q.Op {
	case mongodb.DROP:
		return fmt.Sprintf("%s:*", q.Table)
	case mongodb.SCAN:
		if q.Key == "" {
			return fmt.Sprintf("%s:scan:*", q.Table)
		}
		return fmt.Sprintf("%s:scan:%s", q.Table, q.Key)
	default:
		if q.Key == "" {
			return fmt.Sprintf("%s:*", q.Table)
		}
		return fmt.Sprintf("%s:%s", q.Table, q.Key)
	}
}

func timelineInterval() time.Duration {
	intervalMs, err := strconv.Atoi(strings.TrimSpace(os.Getenv("TPS_TIMELINE_INTERVAL_MS")))
	if err != nil || intervalMs <= 0 {
		intervalMs = 500
	}
	return time.Duration(intervalMs) * time.Millisecond
}

func startTimelineSampler(clientID int, stop <-chan struct{}) {
	if os.Getenv("ENABLE_TIMESERIES") != "true" && os.Getenv("TIMESERIES_ENABLED") != "true" {
		return
	}

	dirPath := fmt.Sprintf("./eval/client%d", clientID)
	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		log.Warnf("Client %d: failed to create timeline dir: %v", clientID, err)
		return
	}

	filePath := fmt.Sprintf("%s/tps_timeline_%s.csv", dirPath, time.Now().Format("20060102_150405"))
	file, err := os.Create(filePath)
	if err != nil {
		log.Warnf("Client %d: failed to create timeline file: %v", clientID, err)
		return
	}

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"elapsed_ms", "tps", "lat_avg_ms", "event"}); err != nil {
		log.Warnf("Client %d: failed to write timeline header: %v", clientID, err)
		_ = file.Close()
		return
	}
	writer.Flush()

	start := time.Now()
	lastTick := start
	lastOps := atomic.LoadInt64(&globalClientMetrics.OpsTotal)
	lastLatSum := atomic.LoadInt64(&globalClientMetrics.LatSumMs)
	lastLatCount := atomic.LoadInt64(&globalClientMetrics.LatCount)
	ticker := time.NewTicker(timelineInterval())
	defer ticker.Stop()
	defer func() {
		writer.Flush()
		_ = file.Close()
	}()

	writeSample := func(now time.Time) {
		elapsedMs := now.Sub(start).Milliseconds()
		deltaSecs := now.Sub(lastTick).Seconds()
		if deltaSecs <= 0 {
			deltaSecs = 0.5
		}

		opsNow := atomic.LoadInt64(&globalClientMetrics.OpsTotal)
		latSumNow := atomic.LoadInt64(&globalClientMetrics.LatSumMs)
		latCountNow := atomic.LoadInt64(&globalClientMetrics.LatCount)

		deltaOps := opsNow - lastOps
		deltaLatSum := latSumNow - lastLatSum
		deltaLatCount := latCountNow - lastLatCount

		tps := float64(deltaOps) / deltaSecs
		latAvg := 0.0
		if deltaLatCount > 0 {
			latAvg = float64(deltaLatSum) / float64(deltaLatCount)
		}

		event := readTimelineEvent(clientID)
		if event == "" {
			event = "stable"
		}

		if err := writer.Write([]string{
			fmt.Sprintf("%d", elapsedMs),
			fmt.Sprintf("%.3f", tps),
			fmt.Sprintf("%.3f", latAvg),
			event,
		}); err != nil {
			log.Warnf("Client %d: failed to write timeline row: %v", clientID, err)
			return
		}
		writer.Flush()
		lastTick = now
		lastOps = opsNow
		lastLatSum = latSumNow
		lastLatCount = latCountNow
	}

	for {
		select {
		case now := <-ticker.C:
			writeSample(now)
		case <-stop:
			writeSample(time.Now())
			return
		}
	}
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

	RecordBatch(batchSize, reply.Latency, reply.PathUsed, false)
}

// RunClient sends operations to replicas
func RunClient(clientID int, configPath string, numOps int) {
	initObjectIDPool(clientID)

	if pinServer < 0 {
		log.Fatalf("Client %d: -pinserver is required (round-robin dispatch has been removed; pass -pinserver=<serverID>)", clientID)
	}

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

	timelineStop := make(chan struct{})
	var stopTimelineOnce sync.Once
	stopTimeline := func() {
		stopTimelineOnce.Do(func() {
			close(timelineStop)
		})
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
		stopTimeline()

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
	go startMetricsServer(clientID)
	go startTimelineSampler(clientID, timelineStop)

	rand.Seed(time.Now().UnixNano() + int64(clientID))
	pool := NewServerPool(clientConns, clientID)
	pool.startReviver(5 * time.Second)

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
		if len(mongoDBQueries) == 0 {
			log.Errorf("No MongoDB queries loaded from %s", filePath)
			return
		}
	}

	jobQ := make(map[int]chan struct{}) // sequential mode only
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
			CmdType:     pickCmdType(), // -readratio: one read/write coin flip per client iteration, matching WOC
			Type:        evalType,
		}

		// Batch composition logic (matching WOC pattern): mixed re-rolls type
		// and object per op; single_obj reuses one object for the whole batch
		// (max contention); object-specific picks one type for the batch but
		// spreads across different objects of that type. Object IDs are a
		// uniform-random pick within the chosen type's pool (no Zipfian),
		// matching WOC's per-op generation loop.
		if batchComposition == "mixed" {
			cmd.IsMixed = true
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				objType := pickObjType()
				cmd.ObjTypes[b] = objType
				cmd.ObjIDs[b] = pickObjID(objType)
			}
			cmd.ObjID = cmd.ObjIDs[0]
			cmd.ObjType = cmd.ObjTypes[0]
		} else if batchComposition == "single_obj" {
			cmd.IsMixed = false
			objType := pickObjType()
			objID := pickObjID(objType)
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
			objType := pickObjType()
			cmd.ObjType = objType
			cmd.ObjIDs = make([]string, currentBatch)
			cmd.ObjTypes = make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				cmd.ObjTypes[b] = objType
				cmd.ObjIDs[b] = pickObjID(objType)
			}
			cmd.ObjID = cmd.ObjIDs[0]
		}
		// Reads carry no payload -- matching WOC/Cabinet's clients and, per
		// efficient/epaxos's reference state.Command{Op, K, V}, real EPaxos
		// GETs never carry a V either (only the key matters; Op flips to
		// GET/READ and the value field goes unused). Attaching a full
		// msgsize random payload to a read would send bytes a real read
		// never needs, without changing consensus cost at all (PreAccept/
		// Accept/Commit run identically for reads and writes either way --
		// see state.go's GetInterferingInstances, which is the one place
		// reads and writes actually diverge, via the writeConflicts table).
		isRead := cmd.CmdType == READ
		switch evalType {
		case PlainMsg:
			if isRead {
				break
			}
			batch := make([][]byte, currentBatch)
			for b := 0; b < currentBatch; b++ {
				batch[b] = genRandomBytes(msgsize)
			}
			cmd.CmdPlain = batch
		case MongoDB:
			batch := make([]mongodb.Query, currentBatch)
			mongoObjIDs := make([]string, currentBatch)
			mongoObjTypes := make([]int, currentBatch)
			for b := 0; b < currentBatch; b++ {
				q := mongoDBQueries[(op+b)%len(mongoDBQueries)]
				batch[b] = q
				mongoObjIDs[b] = mongoQueryObjectID(q)
				// Real key-based classification, driven by -indep, mirroring
				// woc/objectmap.go's classifyRealKey exactly: hash the real
				// key against indepRatio instead of rolling a fresh random
				// number (which would be unrelated to which key is actually
				// touched). The real key is never substituted, so YCSB's
				// Zipfian access distribution and scan semantics are intact.
				mongoObjTypes[b] = mongodb.ClassifyRealKey(q.Key, indepRatio)
			}
			cmd.CmdMongo = batch
			cmd.ObjIDs = mongoObjIDs
			cmd.ObjTypes = mongoObjTypes
			cmd.ObjID = mongoObjIDs[0]
			cmd.ObjType = mongoObjTypes[0]
		}

		targetSID, _, ok := pool.PickPinned(pinServer)
		if !ok {
			log.Warnf("[Client %d] No healthy servers available, retrying shortly...", clientID)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if pipelined {
			limiter.Acquire() // BLOCKS main thread until a slot is free

			if shuttingDown.Load() {
				limiter.Release()
				break
			}

			inflightOps.Add(1)
			go func(clockVal int, command *ClientArgs, startSID int, bs int) {
				defer func() {
					limiter.Release()
					inflightOps.Add(-1)
				}()

				reply := &ClientReply{}

				// Timer starts immediately — limiter wait already done before goroutine launch
				perfM.RecordStarter(clockVal)

				done := make(chan error, 1)
				var usedSID int
				go func() {
					var e error
					usedSID, e = callWithFailover(pool, startSID, command, reply)
					done <- e
				}()

				select {
				case err := <-done:
					if err != nil {
						log.Warnf("[Client %d] RPC error server=%d: %v", clientID, usedSID, err)
						atomic.AddInt64(&perfM.NetworkErrors, int64(bs))
						RecordBatch(bs, 0, "", true)
					} else {
						perfM.RecordFinisher(clockVal)
						recordBatchMetrics(reply, clockVal, bs)
					}
				case <-time.After(15 * time.Second):
					atomic.AddInt64(&perfM.TimeoutCommits, int64(bs))
					RecordBatch(bs, 0, "", true)
					log.Warnf("[Client %d] RPC TIMEOUT (15s) batch=%d server=%d", clientID, clockVal, startSID)
				}

				if clockVal%500 == 0 {
					log.Infof("[Client %d] Batch %d | inflight=%d | fast=%d slow=%d",
						clientID, clockVal, inflightOps.Load(), perfM.FastCommits, perfM.SlowCommits)
				}
			}(CClock, cmd, targetSID, currentBatch)

		} else {
			// Sequential mode
			stack := make(chan struct{}, 1)
			jobQ[CClock] = stack
			if prev, ok := jobQ[CClock-1]; ok && CClock > 1 {
				<-prev
				delete(jobQ, CClock-1)
			}

			reply := &ClientReply{}
			perfM.RecordStarter(CClock)
			_, err := callWithFailover(pool, targetSID, cmd, reply)
			perfM.RecordFinisher(CClock)
			stack <- struct{}{}

			if err != nil {
				atomic.AddInt64(&perfM.NetworkErrors, int64(currentBatch))
				RecordBatch(currentBatch, 0, "", true)
			} else {
				recordBatchMetrics(reply, CClock, currentBatch)
			}
		}

		op += currentBatch
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
		stopTimeline()
	} else {
		// Infinite mode: block until signal handler saves and exits
		for !metricsSaved.Load() {
			time.Sleep(100 * time.Millisecond)
		}
	}
}