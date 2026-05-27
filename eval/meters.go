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
	numOfTotalTx      int
	batchSize         int
	sampleInterval    prioClock
	lastPClock        prioClock
	fileName          string
	meters            sync.Map 
	FastCommits       int64
	SlowCommits       int64
	ConflictCommits   int64
	TimeoutCommits    int64
	NetworkErrors     int64
	FastPathFallbacks int64

	// Internal atomic latency tracking (kept private, matches CORA's private fields)
	fastPathLatencySum int64
	fastPathCount      int64
	slowPathLatencySum int64
	slowPathCount      int64
}

type RecordInstance struct {
	StartTime   time.Time
	TimeElapsed float64 // precise milliseconds (microseconds / 1000), matches CORA
	Metrics     BatchMetrics
}

// ── Initialization ────────────────────────────────────────────────────────────

func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
	// sync.Map needs no Init — matches CORA
}


func (m *PerfMeter) RecordStarter(globalClock int) {
	m.meters.Store(globalClock, &RecordInstance{
		StartTime:   time.Now(),
		TimeElapsed: 0.0,
		Metrics:     BatchMetrics{},
	})
}

func (m *PerfMeter) RecordFinisher(globalClock int) error {
	val, exist := m.meters.Load(globalClock)
	if !exist {
		return errors.New("globalClock has not been recorded with starter")
	}
	rec := val.(*RecordInstance)
	rec.TimeElapsed = float64(time.Since(rec.StartTime).Microseconds()) / 1000.0
	return nil
}


func (m *PerfMeter) IncFastPath(globalClock int) {
	if val, ok := m.meters.Load(globalClock); ok {
		val.(*RecordInstance).Metrics.FastPathCount++
	}
}

func (m *PerfMeter) IncSlowPath(globalClock int) {
	if val, ok := m.meters.Load(globalClock); ok {
		val.(*RecordInstance).Metrics.SlowPathCount++
	}
}

func (m *PerfMeter) IncConflict(globalClock int) {
	if val, ok := m.meters.Load(globalClock); ok {
		val.(*RecordInstance).Metrics.ConflictCount++
	}
}


func (pm *PerfMeter) RecordFastCommit()  { atomic.AddInt64(&pm.FastCommits, 1) }
func (pm *PerfMeter) RecordSlowCommit()  { atomic.AddInt64(&pm.SlowCommits, 1) }

func (pm *PerfMeter) AddFastCommits(n int) {
	if n > 0 {
		atomic.AddInt64(&pm.FastCommits, int64(n))
	}
}
func (pm *PerfMeter) AddSlowCommits(n int) {
	if n > 0 {
		atomic.AddInt64(&pm.SlowCommits, int64(n))
	}
}
func (pm *PerfMeter) AddConflictCommits(n int) {
	if n > 0 {
		atomic.AddInt64(&pm.ConflictCommits, int64(n))
	}
}

func (pm *PerfMeter) RecordFastPathFallback() {
	atomic.AddInt64(&pm.FastPathFallbacks, 1)
}

func (m *PerfMeter) MarkSlowPath(_ int) {}
func (m *PerfMeter) IncTimeout(_ int) {
	atomic.AddInt64(&m.TimeoutCommits, 1)
}

func (m *PerfMeter) RecordFastPathLatency(latencyMs float64) {
	atomic.AddInt64(&m.fastPathLatencySum, int64(latencyMs*1000))
	atomic.AddInt64(&m.fastPathCount, 1)
}
func (m *PerfMeter) RecordSlowPathLatency(latencyMs float64) {
	atomic.AddInt64(&m.slowPathLatencySum, int64(latencyMs*1000))
	atomic.AddInt64(&m.slowPathCount, 1)
}



func (m *PerfMeter) SaveToFile() error {
	var folderName string
	if len(m.fileName) >= 6 && m.fileName[:6] == "client" {
		clientPart := m.fileName
		for i := 6; i < len(m.fileName); i++ {
			if m.fileName[i] == '_' {
				clientPart = m.fileName[:i]
				break
			}
		}
		folderName = clientPart
	} else if len(m.fileName) >= 1 && m.fileName[0] == 's' {
		folderName = fmt.Sprintf("server%d", m.fileName[1]-'0')
	} else {
		folderName = "default"
	}

	dirPath := fmt.Sprintf("./eval/%s", folderName)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	timestamp := time.Now().Format("20060102_150405")
	filePath := fmt.Sprintf("%s/%s_%s.csv", dirPath, m.fileName, timestamp)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var keys []int
	m.meters.Range(func(key, _ interface{}) bool {
		keys = append(keys, key.(int))
		return true
	})
	sort.Ints(keys)

	if err := writer.Write([]string{
		"pclock", "latency (ms) per batch", "throughput (Tx/sec)",
		"fast path ops", "slow path ops", "conflict ops",
	}); err != nil {
		return err
	}

	// ── Per-batch data rows ──────────────────────────────────────────────────
	counter := 0
	var latSum, tptSum float64
	var fastSum, slowSum, conflictSum int
	var latencies, fastPathLatencies, slowPathLatencies []float64

	for _, key := range keys {
		val, ok := m.meters.Load(key)
		if !ok {
			continue
		}
		rec := val.(*RecordInstance)
		if rec.TimeElapsed == 0 {
			continue
		}

		latSum += rec.TimeElapsed
		counter++
		latencies = append(latencies, rec.TimeElapsed)
		fastSum += rec.Metrics.FastPathCount
		slowSum += rec.Metrics.SlowPathCount
		conflictSum += rec.Metrics.ConflictCount

		tpt := (float64(m.batchSize) / rec.TimeElapsed) * 1000
		tptSum += tpt

		if rec.Metrics.SlowPathCount > 0 || rec.Metrics.ConflictCount > 0 {
			slowPathLatencies = append(slowPathLatencies, rec.TimeElapsed)
		} else if rec.Metrics.FastPathCount > 0 {
			fastPathLatencies = append(fastPathLatencies, rec.TimeElapsed)
		}

		if err := writer.Write([]string{
			strconv.Itoa(key),
			strconv.FormatFloat(rec.TimeElapsed, 'f', 3, 64),
			strconv.FormatFloat(tpt, 'f', 3, 64),
			strconv.Itoa(rec.Metrics.FastPathCount),
			strconv.Itoa(rec.Metrics.SlowPathCount),
			strconv.Itoa(rec.Metrics.ConflictCount),
		}); err != nil {
			return err
		}
	}

	if counter == 0 {
		_ = writer.Write([]string{"NO_DATA", "", "", "", "", ""})
		if err := writer.Write([]string{
			"GLOBAL_TOTALS", "", "",
			strconv.FormatInt(m.FastCommits, 10) + " ops",
			strconv.FormatInt(m.SlowCommits, 10) + " ops",
			strconv.FormatInt(m.ConflictCommits, 10) + " ops",
		}); err != nil {
			return err
		}
		return writer.Write([]string{
			"TOTAL_NETWORK_ERRORS",
			strconv.FormatInt(m.NetworkErrors, 10), "ops", "", "", "",
		})
	}

	avgLatency := latSum / float64(counter)
	avgLatencyPerTx := avgLatency / float64(m.batchSize)

	firstVal, _ := m.meters.Load(keys[0])
	lastVal, _ := m.meters.Load(keys[len(keys)-1])
	firstBatch := firstVal.(*RecordInstance)
	lastBatch := lastVal.(*RecordInstance)
	lastEndTime := lastBatch.StartTime.Add(
		time.Duration(lastBatch.TimeElapsed) * time.Millisecond)
	wallClockSecs := lastEndTime.Sub(firstBatch.StartTime).Seconds()
	var actualThroughput float64
	if wallClockSecs > 0 {
		actualThroughput = float64(m.batchSize*counter) / wallClockSecs
	}

	avgFast := float64(fastSum) / float64(counter)
	avgSlow := float64(slowSum) / float64(counter)
	avgConflict := float64(conflictSum) / float64(counter)

	sort.Float64s(latencies)
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]

	var avgFastPathLatency, avgSlowPathLatency float64
	if len(fastPathLatencies) > 0 {
		var s float64
		for _, l := range fastPathLatencies {
			s += l
		}
		avgFastPathLatency = s / float64(len(fastPathLatencies))
	}
	if len(slowPathLatencies) > 0 {
		var s float64
		for _, l := range slowPathLatencies {
			s += l
		}
		avgSlowPathLatency = s / float64(len(slowPathLatencies))
	}

	rows := [][]string{
		{"AVERAGE",
			strconv.FormatFloat(avgLatency, 'f', 3, 64) + " ms",
			strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
			strconv.FormatFloat(avgFast, 'f', 3, 64),
			strconv.FormatFloat(avgSlow, 'f', 3, 64),
			strconv.FormatFloat(avgConflict, 'f', 3, 64)},

		{"THROUGHPUT", "",
			strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
			"", "", ""},

		{"P50_LATENCY",
			strconv.FormatFloat(p50, 'f', 0, 64) + " ms",
			"", "", "", ""},
		{"P95_LATENCY",
			strconv.FormatFloat(p95, 'f', 0, 64) + " ms",
			"", "", "", ""},
		{"P99_LATENCY",
			strconv.FormatFloat(p99, 'f', 0, 64) + " ms",
			"", "", "", ""},

		{"AVG_LATENCY_PER_TX",
			strconv.FormatFloat(avgLatencyPerTx, 'f', 3, 64) + " ms/Tx",
			"", "", "", ""},
	}

	if len(fastPathLatencies) > 0 {
		rows = append(rows,
			[]string{"AVG_FAST_PATH_LATENCY",
				strconv.FormatFloat(avgFastPathLatency, 'f', 3, 64) + " ms",
				"", strconv.Itoa(len(fastPathLatencies)) + " batches", "", ""},
			[]string{"AVG_FAST_PATH_LATENCY_PER_TX",
				strconv.FormatFloat(avgFastPathLatency/float64(m.batchSize), 'f', 3, 64) + " ms/Tx",
				"", "", "", ""},
		)
	}

	if len(slowPathLatencies) > 0 {
		rows = append(rows,
			[]string{"AVG_SLOW_PATH_LATENCY",
				strconv.FormatFloat(avgSlowPathLatency, 'f', 3, 64) + " ms",
				"", "", strconv.Itoa(len(slowPathLatencies)) + " batches", ""},
			[]string{"AVG_SLOW_PATH_LATENCY_PER_TX",
				strconv.FormatFloat(avgSlowPathLatency/float64(m.batchSize), 'f', 3, 64) + " ms/Tx",
				"", "", "", ""},
		)
	}

	rows = append(rows, []string{
		"GLOBAL_TOTALS", "", "",
		strconv.FormatInt(m.FastCommits, 10) + " ops",
		strconv.FormatInt(m.SlowCommits, 10) + " ops",
		strconv.FormatInt(m.ConflictCommits, 10) + " ops",
	})

	rows = append(rows,
		[]string{"TOTAL_FAST_PATH_FALLBACKS",
			strconv.FormatInt(m.FastPathFallbacks, 10), "ops", "", "", ""},
		[]string{"TOTAL_FAST_COMMITS",
			strconv.FormatInt(m.FastCommits, 10), "ops", "", "", ""},
		[]string{"TOTAL_SLOW_COMMITS",
			strconv.FormatInt(m.SlowCommits, 10), "ops", "", "", ""},
		[]string{"TOTAL_CONFLICT_COMMITS",
			strconv.FormatInt(m.ConflictCommits, 10), "ops", "", "", ""},
		[]string{"TOTAL_TIMEOUT_COMMITS",
			strconv.FormatInt(m.TimeoutCommits, 10), "ops", "", "", ""},
		[]string{"TOTAL_NETWORK_ERRORS",
			strconv.FormatInt(m.NetworkErrors, 10), "ops", "", "", ""},
	)

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

