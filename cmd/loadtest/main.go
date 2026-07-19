// loadtest — concurrent read/write load tester for VeltrixDB.
//
// Usage:
//
//	go run ./cmd/loadtest [flags]
//
// Examples:
//
//	# 30-second mixed 70% reads / 30% writes, 64 workers, 1M keyspace
//	go run ./cmd/loadtest --mode=mixed --read-ratio=0.7 --concurrency=64 --duration=30
//
//	# Write-only warmup then read-only benchmark (single Put / Get path)
//	go run ./cmd/loadtest --mode=write --duration=10
//	go run ./cmd/loadtest --mode=read  --duration=30
//
//	# Batched writes via MPut-1024 — the path that engages block packing
//	# and gives ~25× density gain for tiny values. Latency reported is
//	# per-batch (not per-key); per-key amortized = P99 / batch-size.
//	go run ./cmd/loadtest --mode=write --batch-size=1024 \
//	                      --concurrency=8 --duration=30 --value-size=128
//
//	# Batched reads via MGet-256
//	go run ./cmd/loadtest --mode=read --batch-size=256 \
//	                      --concurrency=64 --duration=30
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VeltrixDB/veltrixdb/client"
)

// ── Flags ─────────────────────────────────────────────────────────────────────

var (
	flagAddr        = flag.String("addr", "127.0.0.1:9000", "VeltrixDB TCP address")
	flagMode        = flag.String("mode", "mixed", "Workload mode: write | read | mixed")
	flagConcurrency = flag.Int("concurrency", 50, "Number of parallel worker goroutines")
	flagDuration    = flag.Int("duration", 30, "Test duration in seconds")
	flagWarmup      = flag.Int("warmup", 5, "Warmup seconds before recording stats (write-only)")
	flagNumKeys     = flag.Int("num-keys", 1_000_000, "Keyspace size (keys are key:0 … key:N-1)")
	flagValueSize   = flag.Int("value-size", 64, "Value payload size in bytes")
	flagReadRatio   = flag.Float64("read-ratio", 0.7, "Fraction of reads in mixed mode (0.0–1.0)")
	flagDialTimeout = flag.Int("dial-timeout", 5, "TCP dial timeout in seconds")
	flagReportEvery = flag.Int("report-every", 1, "Live stats interval in seconds")
	flagSafeReads   = flag.Bool("safe-reads", false,
		"Only read keys that have already been written.\n"+
			"\tWriters use a sequential global counter; readers pick from [0, written_count).\n"+
			"\tEliminates key-not-found errors during mixed ingestion.")
	flagKeyOffset = flag.Int64("key-offset", 0,
		"Start key index offset. Use to continue from a previous run without overlap.\n"+
			"\tKeys generated as key:(offset) … key:(offset+num-keys-1).")
	flagBatchSize = flag.Int("batch-size", 1,
		"Entries per MPut/MGet batch. Default 1 = single-frame Put/Get path.\n"+
			"\tValues > 1 switch to MPut/MGet — engages server-side block packing\n"+
			"\tfor tiny values (~25× disk density gain at 128 B). Reported latency\n"+
			"\tis per-batch; per-key amortized = batch-P99 / batch-size.")
	flagLogErrors = flag.Int("log-errors", 5,
		"Log the first N distinct errors per worker to stderr. 0 = silent.")
)

// errLogger logs up to flagLogErrors errors per worker. After the cap is
// reached it stays silent so the output doesn't get flooded.
type errLogger struct {
	workerID int
	logged   int
}

func (e *errLogger) log(op, detail string, err error) {
	if *flagLogErrors <= 0 || e.logged >= *flagLogErrors {
		return
	}
	e.logged++
	fmt.Fprintf(os.Stderr, "[worker %d] %s error #%d: %v  (%s)\n",
		e.workerID, op, e.logged, err, detail)
}

// writtenHWM is the global count of keys written so far.
// Active only when --safe-reads is set.
// Writers atomically increment it to claim a key index; readers pick from [0, hwm).
var writtenHWM atomic.Int64

// ── Per-worker result ─────────────────────────────────────────────────────────

type workerResult struct {
	writeOps   int64
	readOps    int64
	writeErrs  int64
	readErrs   int64
	readMisses int64 // GET returned not-found (not counted as error)
	wLatNs     []int64
	rLatNs     []int64
}

// ── Live counters (shared, updated via atomic) ────────────────────────────────

var (
	liveWriteOps  atomic.Int64
	liveReadOps   atomic.Int64
	liveWriteErrs atomic.Int64
	liveReadErrs  atomic.Int64
)

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	mode := strings.ToLower(*flagMode)
	if mode != "write" && mode != "read" && mode != "mixed" {
		fmt.Fprintf(os.Stderr, "error: --mode must be write, read, or mixed\n")
		os.Exit(1)
	}
	if *flagConcurrency < 1 {
		fmt.Fprintf(os.Stderr, "error: --concurrency must be >= 1\n")
		os.Exit(1)
	}

	// Fixed value payload: all 'v' bytes.  No spaces or newlines so the
	// line-based protocol stays intact regardless of value size.
	value := []byte(strings.Repeat("v", *flagValueSize))

	printHeader(mode)

	// ── Warmup ────────────────────────────────────────────────────────────────
	if *flagWarmup > 0 && mode != "read" {
		fmt.Printf("  Warming up for %ds (writes only, stats not recorded)…\n", *flagWarmup)
		wCtx, wCancel := context.WithTimeout(context.Background(), time.Duration(*flagWarmup)*time.Second)
		runWorkers(wCtx, "write", value)
		wCancel()
		fmt.Println("  Warmup done.")
	}

	// ── Test ──────────────────────────────────────────────────────────────────
	liveWriteOps.Store(0)
	liveReadOps.Store(0)
	liveWriteErrs.Store(0)
	liveReadErrs.Store(0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*flagDuration)*time.Second)
	defer cancel()

	// Live reporter goroutine.
	reportDone := make(chan struct{})
	go liveReporter(ctx, reportDone)

	start := time.Now()
	results := runWorkers(ctx, mode, value)
	elapsed := time.Since(start)

	<-reportDone

	printReport(results, elapsed, mode)
}

// ── Worker launcher ───────────────────────────────────────────────────────────

func runWorkers(ctx context.Context, mode string, value []byte) []workerResult {
	results := make([]workerResult, *flagConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < *flagConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			results[workerID] = runWorker(ctx, workerID, mode, value)
		}(i)
	}

	wg.Wait()
	return results
}

// ── Single worker ─────────────────────────────────────────────────────────────

func runWorker(ctx context.Context, id int, mode string, value []byte) workerResult {
	if *flagBatchSize > 1 {
		return runBatchedWorker(ctx, id, mode, value)
	}
	return runUnbatchedWorker(ctx, id, mode, value)
}

func runUnbatchedWorker(ctx context.Context, id int, mode string, value []byte) workerResult {
	dialTimeout := time.Duration(*flagDialTimeout) * time.Second
	el := &errLogger{workerID: id}
	conn, err := client.DialTCP(*flagAddr, dialTimeout)
	if err != nil {
		el.log("dial", *flagAddr, err)
		liveWriteErrs.Add(1)
		return workerResult{writeErrs: 1}
	}
	defer conn.Close()

	// Each worker gets its own RNG (no global lock).
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1_000_003))

	var res workerResult
	// Pre-allocate latency slices to avoid repeated growth allocations.
	res.wLatNs = make([]int64, 0, 1<<17) // 128K slots
	res.rLatNs = make([]int64, 0, 1<<17)

	for {
		select {
		case <-ctx.Done():
			return res
		default:
		}

		doRead := shouldRead(mode, *flagReadRatio, rng)

		key := pickKey(doRead, rng)
		if key == "" {
			continue // safe-reads: nothing written yet, skip this read
		}

		if doRead {
			start := time.Now()
			val, err := conn.Get(key)
			ns := time.Since(start).Nanoseconds()

			if err != nil {
				el.log("GET", key, err)
				res.readErrs++
				liveReadErrs.Add(1)
				if rerr := conn.Redial(dialTimeout); rerr != nil {
					el.log("redial", *flagAddr, rerr)
					return res
				}
				continue
			}
			if val == nil {
				res.readMisses++
			}
			res.readOps++
			liveReadOps.Add(1)
			res.rLatNs = append(res.rLatNs, ns)
		} else {
			start := time.Now()
			err := conn.Put(key, value)
			ns := time.Since(start).Nanoseconds()

			if err != nil {
				el.log("PUT", key, err)
				res.writeErrs++
				liveWriteErrs.Add(1)
				if rerr := conn.Redial(dialTimeout); rerr != nil {
					el.log("redial", *flagAddr, rerr)
					return res
				}
				continue
			}
			res.writeOps++
			liveWriteOps.Add(1)
			res.wLatNs = append(res.wLatNs, ns)
		}
	}
}

// runBatchedWorker uses MPut / MGet with batch-size entries per round-trip.
// Latency samples are per-batch wall-clock; throughput counters tally per-entry
// so live reporting and the final report match the unbatched code path.
//
// Each batch round-trip is one frame send + one frame receive over a single
// BinaryConn — this is the path that engages server-side block packing on
// MPut and avoids per-entry network overhead on MGet.
func runBatchedWorker(ctx context.Context, id int, mode string, value []byte) workerResult {
	dialTimeout := time.Duration(*flagDialTimeout) * time.Second
	el := &errLogger{workerID: id}
	conn, err := client.DialBinary(*flagAddr, dialTimeout)
	if err != nil {
		el.log("dial", *flagAddr, err)
		liveWriteErrs.Add(1)
		return workerResult{writeErrs: 1}
	}
	defer conn.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1_000_003))
	bsz := *flagBatchSize

	var res workerResult
	// Pre-allocate latency slices: one sample per batch.
	res.wLatNs = make([]int64, 0, 1<<14) // 16K batches
	res.rLatNs = make([]int64, 0, 1<<14)

	// Pre-allocated per-iteration buffers so we don't reallocate per batch.
	mputEntries := make([]client.MPutEntry, 0, bsz)
	mgetKeys := make([]string, 0, bsz)

	for {
		select {
		case <-ctx.Done():
			return res
		default:
		}

		doRead := shouldRead(mode, *flagReadRatio, rng)

		if doRead {
			mgetKeys = mgetKeys[:0]
			for i := 0; i < bsz; i++ {
				k := pickKey(true, rng)
				if k == "" {
					break // safe-reads: nothing written yet
				}
				mgetKeys = append(mgetKeys, k)
			}
			if len(mgetKeys) == 0 {
				continue
			}
			start := time.Now()
			results, err := conn.MGet(mgetKeys)
			ns := time.Since(start).Nanoseconds()
			if err != nil {
				el.log("MGET", fmt.Sprintf("batch=%d", len(mgetKeys)), err)
				res.readErrs++
				liveReadErrs.Add(1)
				if rerr := conn.Redial(dialTimeout); rerr != nil {
					el.log("redial", *flagAddr, rerr)
					return res
				}
				continue
			}
			misses := int64(0)
			for _, r := range results {
				if r.Err != nil {
					// Treat per-key error as miss for stats — clearer than aborting batch.
					misses++
				} else if r.NotFound {
					misses++
				}
			}
			res.readMisses += misses
			res.readOps += int64(len(mgetKeys))
			liveReadOps.Add(int64(len(mgetKeys)))
			res.rLatNs = append(res.rLatNs, ns)
		} else {
			mputEntries = mputEntries[:0]
			for i := 0; i < bsz; i++ {
				k := pickKey(false, rng)
				mputEntries = append(mputEntries, client.MPutEntry{
					Key:   k,
					Value: value,
					TTL:   -1,
				})
			}
			start := time.Now()
			perErrs, err := conn.MPut(mputEntries)
			ns := time.Since(start).Nanoseconds()
			if err != nil {
				el.log("MPUT", fmt.Sprintf("batch=%d", len(mputEntries)), err)
				res.writeErrs++
				liveWriteErrs.Add(1)
				if rerr := conn.Redial(dialTimeout); rerr != nil {
					el.log("redial", *flagAddr, rerr)
					return res
				}
				continue
			}
			perEntryFailed := int64(0)
			for _, e := range perErrs {
				if e != nil {
					perEntryFailed++
				}
			}
			res.writeErrs += perEntryFailed
			liveWriteErrs.Add(perEntryFailed)
			succeeded := int64(len(mputEntries)) - perEntryFailed
			res.writeOps += succeeded
			liveWriteOps.Add(succeeded)
			res.wLatNs = append(res.wLatNs, ns)
		}
	}
}

// pickKey returns the key string for this operation.
//
// Default (--safe-reads=false): random key from the full keyspace.
//
// Safe-reads mode (--safe-reads):
//   - Write: claims the next sequential index via writtenHWM. Once all N keys
//     are written the counter wraps so rewrites distribute across the keyspace.
//   - Read: picks uniformly from [0, min(writtenHWM, numKeys)). Returns "" if
//     nothing has been written yet (caller skips the operation).
func pickKey(doRead bool, rng *rand.Rand) string {
	numKeys := int64(*flagNumKeys)
	offset := *flagKeyOffset
	if !*flagSafeReads {
		return fmt.Sprintf("key:%d", offset+rng.Int63n(numKeys))
	}
	if doRead {
		hwm := writtenHWM.Load()
		if hwm == 0 {
			return "" // nothing written yet
		}
		limit := hwm
		if limit > numKeys {
			limit = numKeys
		}
		return fmt.Sprintf("key:%d", offset+rng.Int63n(limit))
	}
	// Write: claim the next sequential slot.
	idx := writtenHWM.Add(1) - 1
	return fmt.Sprintf("key:%d", offset+idx%numKeys)
}

func shouldRead(mode string, readRatio float64, rng *rand.Rand) bool {
	switch mode {
	case "read":
		return true
	case "write":
		return false
	default: // mixed
		return rng.Float64() < readRatio
	}
}

// ── Live reporter ─────────────────────────────────────────────────────────────

func liveReporter(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(time.Duration(*flagReportEvery) * time.Second)
	defer ticker.Stop()

	var prevW, prevR int64

	for {
		select {
		case <-ctx.Done():
			fmt.Println() // newline after the last \r line
			return
		case <-ticker.C:
			curW := liveWriteOps.Load()
			curR := liveReadOps.Load()
			wErrs := liveWriteErrs.Load()
			rErrs := liveReadErrs.Load()

			wQPS := curW - prevW
			rQPS := curR - prevR
			prevW, prevR = curW, curR

			totalOps := curW + curR
			totalErrs := wErrs + rErrs
			errPct := 0.0
			if totalOps+totalErrs > 0 {
				errPct = float64(totalErrs) / float64(totalOps+totalErrs) * 100
			}
			fmt.Printf("  writes/s: %-8d  reads/s: %-8d  total_w: %-10d  total_r: %-10d  errs: %d (%.1f%%)\n",
				wQPS, rQPS, curW, curR, totalErrs, errPct)
		}
	}
}

// ── Final report ──────────────────────────────────────────────────────────────

func printReport(results []workerResult, elapsed time.Duration, mode string) {
	var totalW, totalR, errW, errR, misses int64
	var allWLat, allRLat []int64

	for _, r := range results {
		totalW += r.writeOps
		totalR += r.readOps
		errW += r.writeErrs
		errR += r.readErrs
		misses += r.readMisses
		allWLat = append(allWLat, r.wLatNs...)
		allRLat = append(allRLat, r.rLatNs...)
	}

	elapsedSec := elapsed.Seconds()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    Load Test Results                        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Printf("  Mode:        %s\n", strings.ToUpper(mode))
	fmt.Printf("  Duration:    %.2fs\n", elapsedSec)
	fmt.Printf("  Workers:     %d\n", *flagConcurrency)
	fmt.Printf("  Keyspace:    %d keys\n", *flagNumKeys)
	fmt.Printf("  Value size:  %d bytes\n", *flagValueSize)
	fmt.Println()

	if mode != "read" && (totalW > 0 || errW > 0) {
		printOpReport("WRITES", totalW, errW, 0, elapsedSec, allWLat)
	}
	if mode != "write" && (totalR > 0 || errR > 0) {
		printOpReport("READS", totalR, errR, misses, elapsedSec, allRLat)
	}
	if mode == "mixed" {
		total := totalW + totalR
		fmt.Printf("  Combined throughput: %.0f ops/s  (%d total ops)\n\n",
			float64(total)/elapsedSec, total)
	}
}

func printOpReport(label string, ops, errs, misses int64, elapsedSec float64, latNs []int64) {
	qps := float64(ops) / elapsedSec
	errPct := 0.0
	if ops+errs > 0 {
		errPct = float64(errs) / float64(ops+errs) * 100
	}

	fmt.Printf("  ── %s ─────────────────────────────────────────\n", label)
	fmt.Printf("  Total ops:    %d\n", ops)
	fmt.Printf("  Throughput:   %.0f ops/s\n", qps)
	fmt.Printf("  Errors:       %d (%.2f%%)\n", errs, errPct)
	if misses > 0 {
		fmt.Printf("  Cache misses: %d (key not found — not an error)\n", misses)
	}
	fmt.Println()

	if len(latNs) == 0 {
		fmt.Println("  No latency samples collected.")
		fmt.Println()
		return
	}

	sort.Slice(latNs, func(i, j int) bool { return latNs[i] < latNs[j] })

	if *flagBatchSize > 1 {
		fmt.Printf("  Latency percentiles (per batch of %d entries):\n", *flagBatchSize)
	} else {
		fmt.Println("  Latency percentiles:")
	}
	for _, p := range []struct {
		label string
		pct   float64
	}{
		{"P50  ", 0.50},
		{"P90  ", 0.90},
		{"P95  ", 0.95},
		{"P99  ", 0.99},
		{"P99.9", 0.999},
		{"Max  ", 1.0},
	} {
		ns := latNs[int(float64(len(latNs)-1)*p.pct)]
		if *flagBatchSize > 1 {
			perKey := ns / int64(*flagBatchSize)
			fmt.Printf("    %s  %-12s  (%s/key amortized)\n", p.label, fmtDuration(ns), fmtDuration(perKey))
		} else {
			fmt.Printf("    %s  %s\n", p.label, fmtDuration(ns))
		}
	}
	fmt.Println()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func fmtDuration(ns int64) string {
	switch {
	case ns < 1_000:
		return fmt.Sprintf("%d ns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.2f µs", float64(ns)/1e3)
	default:
		return fmt.Sprintf("%.3f ms", float64(ns)/1e6)
	}
}

func printHeader(mode string) {
	fmt.Printf(`
╔══════════════════════════════════════════════════════════════╗
║              VeltrixDB Load Tester                          ║
╚══════════════════════════════════════════════════════════════╝
  target:      %s
  mode:        %s
  workers:     %d
  duration:    %ds
  keyspace:    %d keys
  value size:  %d bytes
  batch size:  %d %s
`, *flagAddr, strings.ToUpper(mode), *flagConcurrency, *flagDuration, *flagNumKeys, *flagValueSize,
		*flagBatchSize, batchPathLabel())

	if mode == "mixed" {
		fmt.Printf("  read ratio:  %.0f%%\n", *flagReadRatio*100)
	}
	fmt.Println()
}

// batchPathLabel describes which client-side code path the run uses, so the
// operator immediately knows whether server-side block packing is engaged.
func batchPathLabel() string {
	if *flagBatchSize > 1 {
		return "(MPut/MGet — engages packing)"
	}
	return "(single Put/Get — unpacked)"
}
