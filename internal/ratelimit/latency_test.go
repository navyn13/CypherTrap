package ratelimit

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// single-key script matching the pre-chunking Check path
var fixedWindowSingleScript = redis.NewScript(`
	local current = redis.call('INCR', KEYS[1])
	if current == 1 then
		redis.call('PEXPIRE', KEYS[1], ARGV[2])
	end
	if current > tonumber(ARGV[1]) then
		return 0
	end
	return 1
`)

func TestCompareCheckLatency(t *testing.T) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379"
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	rdb := redis.NewClient(opts)
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	defer rdb.Close()

	const (
		limit    = 1_000_000
		windowMs = 60_000
		perWorker = 200
	)
	concurrencies := []int{1, 8, 64, 256}

	fmt.Println()
	fmt.Println("=== Check latency: without chunking vs with chunking ===")
	fmt.Println("(end-to-end wait from caller submit → result)")
	fmt.Println()

	for _, n := range concurrencies {
		without := measureDirect(t, rdb, n, perWorker, limit, windowMs)
		with := measureBatched(t, rdb, n, perWorker, limit, windowMs)

		fmt.Printf("concurrency=%-3d  samples=%d\n", n, n*perWorker)
		fmt.Printf("  WITHOUT chunk (1 EVAL/call):  p50=%6s  p95=%6s  p99=%6s  mean=%6s\n",
			fmtDur(without.p50), fmtDur(without.p95), fmtDur(without.p99), fmtDur(without.mean))
		fmt.Printf("  WITH    chunk (batcher):      p50=%6s  p95=%6s  p99=%6s  mean=%6s\n",
			fmtDur(with.p50), fmtDur(with.p95), fmtDur(with.p99), fmtDur(with.mean))
		fmt.Printf("  Redis RTTs (approx):          without=%d  with≈%d  (chunk=%d)\n",
			n*perWorker, (n*perWorker+checkChunkSize-1)/checkChunkSize, checkChunkSize)
		fmt.Println()
	}
}

type latStats struct {
	p50, p95, p99, mean time.Duration
}

func summarize(samples []time.Duration) latStats {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var sum time.Duration
	for _, d := range samples {
		sum += d
	}
	pct := func(p float64) time.Duration {
		if len(samples) == 0 {
			return 0
		}
		i := int(float64(len(samples)-1) * p)
		return samples[i]
	}
	return latStats{
		p50:  pct(0.50),
		p95:  pct(0.95),
		p99:  pct(0.99),
		mean: sum / time.Duration(len(samples)),
	}
}

func measureDirect(t *testing.T, rdb *redis.Client, workers, perWorker, limit, windowMs int) latStats {
	t.Helper()
	samples := make([]time.Duration, workers*perWorker)
	var wg sync.WaitGroup
	ctx := context.Background()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("lat:direct:%d:%d", w, i)
				start := time.Now()
				_, err := fixedWindowSingleScript.Run(ctx, rdb, []string{key}, limit, windowMs).Int()
				samples[w*perWorker+i] = time.Since(start)
				if err != nil {
					t.Errorf("direct eval: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()
	cleanupPrefix(rdb, "lat:direct:")
	return summarize(samples)
}

func measureBatched(t *testing.T, rdb *redis.Client, workers, perWorker, limit, windowMs int) latStats {
	t.Helper()
	b := newCheckBatcher(rdb)
	samples := make([]time.Duration, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("lat:batch:%d:%d", w, i)
				start := time.Now()
				_ = b.submit(key, limit, windowMs)
				samples[w*perWorker+i] = time.Since(start)
			}
		}(w)
	}
	wg.Wait()
	cleanupPrefix(rdb, "lat:batch:")
	return summarize(samples)
}

func cleanupPrefix(rdb *redis.Client, prefix string) {
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, prefix+"*", 500).Iterator()
	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= 200 {
			_ = rdb.Del(ctx, keys...).Err()
			keys = keys[:0]
		}
	}
	if len(keys) > 0 {
		_ = rdb.Del(ctx, keys...).Err()
	}
}

func fmtDur(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Nanoseconds())/1e3)
	}
	return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
}
