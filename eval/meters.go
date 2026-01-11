package eval

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type serverID = int
type prioClock = int
type priority = float64

type BatchMetrics struct {
	FastPathCount int
	SlowPathCount int
	ConflictCount int
}

type PerfMeter struct {
	sync.RWMutex
	numOfTotalTx   int
	batchSize      int
	sampleInterval prioClock
	lastPClock     prioClock
	fileName       string
	meters         map[prioClock]*RecordInstance
	FastCommits     int64
	SlowCommits     int64
	ConflictCommits int64
    FastPathFallbacks int64 // Track fast path failures to slow path
}

type RecordInstance struct {
	StartTime   time.Time
	TimeElapsed float64 // Changed from int64 to float64 to store precise values
	Metrics     BatchMetrics
}


// ---------------- Initialization ----------------
func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
	m.meters = make(map[prioClock]*RecordInstance)
}

// ---------------- Record start/end ----------------
func (m *PerfMeter) RecordStarter(globalClock int) {
	m.Lock()
	defer m.Unlock()

	m.meters[globalClock] = &RecordInstance{
		StartTime:   time.Now(),
		TimeElapsed: 0.0,
		Metrics:     BatchMetrics{},
	}
}

func (m *PerfMeter) RecordFinisher(globalClock int) error {
	m.Lock()
	defer m.Unlock()

	_, exist := m.meters[globalClock]
	if !exist {
		return errors.New("globalClock has not been recorded with starter")
	}

	start := m.meters[globalClock].StartTime
	m.meters[globalClock].TimeElapsed = float64(time.Now().Sub(start).Microseconds()) / 1000.0 // Record precise milliseconds

	return nil
}

// ---------------- Increment counters ----------------
func (m *PerfMeter) IncFastPath(globalClock int) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[globalClock]; ok {
		rec.Metrics.FastPathCount++
	}
}

func (m *PerfMeter) IncSlowPath(globalClock int) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[globalClock]; ok {
		rec.Metrics.SlowPathCount++
	}
}

func (m *PerfMeter) IncConflict(globalClock int) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[globalClock]; ok {
		rec.Metrics.ConflictCount++
	}
}

func (pm *PerfMeter) RecordFastCommit() {
	atomic.AddInt64(&pm.FastCommits, 1)
}

func (pm *PerfMeter) RecordSlowCommit() {
	atomic.AddInt64(&pm.SlowCommits, 1)
}

// Add n commits to the global counters
func (pm *PerfMeter) AddFastCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.FastCommits, int64(n))
}

func (pm *PerfMeter) AddSlowCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.SlowCommits, int64(n))
}

func (pm *PerfMeter) AddConflictCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.ConflictCommits, int64(n))
}

// Track when fast path fails and falls back to slow path
func (pm *PerfMeter) RecordFastPathFallback() {
    atomic.AddInt64(&pm.FastPathFallbacks, 1)
}

// ---------------- Save to file ----------------
func (m *PerfMeter) SaveToFile() error {
	var folderName string
	if len(m.fileName) >= 6 && m.fileName[:6] == "client" {
		var clientPart string
		for i := 6; i < len(m.fileName); i++ {
			if m.fileName[i] == '_' {
				clientPart = m.fileName[:i]
				break
			}
		}
		if clientPart == "" {
			clientPart = m.fileName
		}
		folderName = clientPart
	} else if len(m.fileName) >= 1 && m.fileName[0] == 's' {
		folderName = fmt.Sprintf("server%d", m.fileName[1]-'0')
	} else {
		folderName = "default"
	}

	dirPath := fmt.Sprintf("./eval/%s", folderName)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}

	// Add timestamp to filename
	timestamp := time.Now().Format("20060102_150405")
	filePath := fmt.Sprintf("%s/%s_%s.csv", dirPath, m.fileName, timestamp)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	
	// Ensure we flush AND sync to disk before returning
	defer func() {
		writer.Flush()
		if err := file.Sync(); err != nil {
			fmt.Printf("Warning: failed to sync file to disk: %v\n", err)
		}
	}()

	m.RLock()
	defer m.RUnlock()

	var keys []int
	for key := range m.meters {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx/sec)", "fast path ops", "slow path ops", "conflict ops"})
	if err != nil {
		return err
	}

	counter := 0
	var latSum int64 = 0
	var tptSum float64 = 0 
	var fastSum, slowSum, conflictSum int = 0, 0, 0

	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}

		latSum += int64(value.TimeElapsed)
		counter++
		fastSum += value.Metrics.FastPathCount
		slowSum += value.Metrics.SlowPathCount
		conflictSum += value.Metrics.ConflictCount

		lat := value.TimeElapsed
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
		tptSum += tpt 

		err = writer.Write([]string{
			strconv.Itoa(key),
			strconv.FormatFloat(lat, 'f', 3, 64),
			strconv.FormatFloat(tpt, 'f', 3, 64),
			strconv.Itoa(value.Metrics.FastPathCount),
			strconv.Itoa(value.Metrics.SlowPathCount),
			strconv.Itoa(value.Metrics.ConflictCount),
		})
		if err != nil {
			return err
		}
	}

	if counter == 0 {
		// No completed batches recorded. Write a NO_DATA row and still emit global totals
		_ = writer.Write([]string{"NO_DATA", "", "", "", "", ""})
		// Still write global totals (likely zeros)
		err = writer.Write([]string{
			"GLOBAL_TOTALS",
			"",
			"",
			strconv.FormatInt(m.FastCommits, 10) + " ops",
			strconv.FormatInt(m.SlowCommits, 10) + " ops",
			strconv.FormatInt(m.ConflictCommits, 10) + " ops",
		})
		if err != nil {
			return err
		}
		return nil
	}

	
	avgLatency := float64(latSum) / float64(counter)
	// Calculate actual wall-clock throughput
	firstBatch := m.meters[keys[0]]
	lastBatch := m.meters[keys[len(keys)-1]]
	lastEndTime := lastBatch.StartTime.Add(time.Duration(lastBatch.TimeElapsed) * time.Millisecond)
	totalWallClockSeconds := lastEndTime.Sub(firstBatch.StartTime).Seconds()

	// This is your TRUE throughput (Tx/sec)
	actualThroughput := float64(m.batchSize*counter) / totalWallClockSeconds

	avgFast := float64(fastSum) / float64(counter)
	avgSlow := float64(slowSum) / float64(counter)
	avgConflict := float64(conflictSum) / float64(counter)

	// Calculate tail latencies (p50, p95, p99)
	var latencies []int64
	var fastPathLatencies []int64
	var slowPathLatencies []int64

	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}
		latencies = append(latencies, int64(value.TimeElapsed))

		// Track fast path vs slow path latencies
		if value.Metrics.FastPathCount > value.Metrics.SlowPathCount {
			fastPathLatencies = append(fastPathLatencies, int64(value.TimeElapsed))
		} else if value.Metrics.SlowPathCount > 0 {
			slowPathLatencies = append(slowPathLatencies, int64(value.TimeElapsed))
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	p50Latency := latencies[len(latencies)*50/100]
	p95Latency := latencies[len(latencies)*95/100]
	p99Latency := latencies[len(latencies)*99/100]

	// Calculate latency per transaction
	avgLatencyPerTx := avgLatency / float64(m.batchSize)
	var avgFastPathLatency float64
	var avgSlowPathLatency float64

	if len(fastPathLatencies) > 0 {
		var fastSum int64
		for _, lat := range fastPathLatencies {
			fastSum += lat
		}
		avgFastPathLatency = float64(fastSum) / float64(len(fastPathLatencies))
	}

	if len(slowPathLatencies) > 0 {
		var slowSum int64
		for _, lat := range slowPathLatencies {
			slowSum += lat
		}
		avgSlowPathLatency = float64(slowSum) / float64(len(slowPathLatencies))
	}

	// Write summary row with averages
	err = writer.Write([]string{
		"AVERAGE",
		strconv.FormatFloat(avgLatency, 'f', 3, 64) + " ms",
		strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
		strconv.FormatFloat(avgFast, 'f', 3, 64),
		strconv.FormatFloat(avgSlow, 'f', 3, 64),
		strconv.FormatFloat(avgConflict, 'f', 3, 64),
	})
	if err != nil {
		return err
	}

	// Write overall throughput (total ops / total time)
	err = writer.Write([]string{
		"THROUGHPUT",
		"",
		strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	// Write tail latency metrics
	err = writer.Write([]string{
		"P50_LATENCY",
		strconv.FormatInt(p50Latency, 10) + " ms",
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	err = writer.Write([]string{
		"P95_LATENCY",
		strconv.FormatInt(p95Latency, 10) + " ms",
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	err = writer.Write([]string{
		"P99_LATENCY",
		strconv.FormatInt(p99Latency, 10) + " ms",
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	// Write latency per transaction
	err = writer.Write([]string{
		"AVG_LATENCY_PER_TX",
		strconv.FormatFloat(avgLatencyPerTx, 'f', 3, 64) + " ms/Tx",
		"",
		"",
		"",
		"",
	})
	if err != nil {
		return err
	}

	// Write fast path specific latency
	if len(fastPathLatencies) > 0 {
		err = writer.Write([]string{
			"AVG_FAST_PATH_LATENCY",
			strconv.FormatFloat(avgFastPathLatency, 'f', 3, 64) + " ms",
			"",
			strconv.Itoa(len(fastPathLatencies)) + " batches",
			"",
			"",
		})
		if err != nil {
			return err
		}
        // Write fast path latency per transaction
        err = writer.Write([]string{
            "AVG_FAST_PATH_LATENCY_PER_TX",
            strconv.FormatFloat(avgFastPathLatency/float64(m.batchSize), 'f', 3, 64) + " ms/Tx",
            "",
            "",
            "",
            "",
        })
        if err != nil {
            return err
        }
	}

	// Write slow path specific latency
	if len(slowPathLatencies) > 0 {
		err = writer.Write([]string{
			"AVG_SLOW_PATH_LATENCY",
			strconv.FormatFloat(avgSlowPathLatency, 'f', 3, 64) + " ms",
			"",
			"",
			strconv.Itoa(len(slowPathLatencies)) + " batches",
			"",
		})
		if err != nil {
			return err
		}
        // Write slow path latency per transaction
        err = writer.Write([]string{
            "AVG_SLOW_PATH_LATENCY_PER_TX",
            strconv.FormatFloat(avgSlowPathLatency/float64(m.batchSize), 'f', 3, 64) + " ms/Tx",
            "",
            "",
            "",
            "",
        })
        if err != nil {
            return err
        }
	}

	// Write global totals row (operation counts, not batch counts)
	err = writer.Write([]string{
		"GLOBAL_TOTALS",
		"",
		"",
		strconv.FormatInt(m.FastCommits, 10) + " ops",
		strconv.FormatInt(m.SlowCommits, 10) + " ops",
		strconv.FormatInt(m.ConflictCommits, 10) + " ops",
	})
	if err != nil {
		return err
	}

    // Write fast path fallback count
    err = writer.Write([]string{"TOTAL_FAST_PATH_FALLBACKS", strconv.FormatInt(m.FastPathFallbacks, 10), "ops"})
    if err != nil {
        return err
    }
	
	err = writer.Write([]string{"TOTAL_FAST_COMMITS", strconv.FormatInt(m.FastCommits, 10), "ops"})
	if err != nil {
		return err
	}
	err = writer.Write([]string{"TOTAL_SLOW_COMMITS", strconv.FormatInt(m.SlowCommits, 10), "ops"})
	if err != nil {
		return err
	}
	err = writer.Write([]string{"TOTAL_CONFLICT_COMMITS", strconv.FormatInt(m.ConflictCommits, 10), "ops"})
	if err != nil {
		return err
	}
	return nil
}

