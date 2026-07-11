//go:build integration

package stress

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Defaults match stress.py.
const (
	defaultServer    = "localhost:7878"
	defaultCompany   = "admin"
	defaultKeyName   = "login"
	defaultAPIKey    = "rls_6780eb36cd5328b90272d5d4c7212250fbc4e60385b3ed87"
	defaultPolicy    = "free"
	defaultConns     = 20
	defaultBaseReqs  = 100
	defaultDurationS = 10
	defaultMinRPS    = 10000.0
	secondGrowth     = 2
	maxReqsPerSecond = 6400
	ioTimeout        = time.Second
	pipelineDepth    = 1024 // in-flight ALLOW requests per connection
)

type config struct {
	server      string
	company     string
	keyName     string
	apiKey      string
	policy      string
	connections int
	baseReqs    int
	duration    time.Duration
	minRPS      float64
}

type metrics struct {
	mu      sync.Mutex
	start   time.Time
	totals  map[string]int64
	buckets map[int]map[string]int64
}

func newMetrics(start time.Time) *metrics {
	return &metrics{
		start:   start,
		totals:  make(map[string]int64),
		buckets: make(map[int]map[string]int64),
	}
}

// flush merges per-worker local counters into shared totals/buckets.
func (m *metrics) flush(local map[string]int64) {
	var n int64
	for _, v := range local {
		n += v
	}
	if n == 0 {
		return
	}
	sec := int(time.Since(m.start).Seconds())
	if sec < 0 {
		sec = 0
	}
	m.mu.Lock()
	b, ok := m.buckets[sec]
	if !ok {
		b = make(map[string]int64)
		m.buckets[sec] = b
	}
	for k, v := range local {
		if v == 0 {
			continue
		}
		m.totals[k] += v
		b[k] += v
		local[k] = 0
	}
	m.mu.Unlock()
}

func (m *metrics) bucketCounts(second int) map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.buckets[second]
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func (m *metrics) cumulativeThrough(second int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for s := 0; s <= second; s++ {
		for _, n := range m.buckets[s] {
			total += n
		}
	}
	return total
}

func (m *metrics) snapshot() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.totals))
	for k, v := range m.totals {
		out[k] = v
	}
	return out
}

func sumCounts(counts map[string]int64) int64 {
	var n int64
	for _, v := range counts {
		n += v
	}
	return n
}

func formatSecondLogLine(second int, counts map[string]int64, cumulative int64) string {
	thisSec := sumCounts(counts)
	line := fmt.Sprintf("[%4ds] +%d this second (%.0f rps) | total %d", second, thisSec, float64(thisSec), cumulative)
	for _, kind := range []string{"allowed", "blocked", "unauthorized", "server_error", "invalid", "other"} {
		if counts[kind] > 0 {
			line += fmt.Sprintf(" | %s=%d", kind, counts[kind])
		}
	}
	return line
}

func secondLogLoop(start time.Time, durationSec int, m *metrics, stop *atomic.Bool) {
	fmt.Fprintln(os.Stderr, "=== Per-second log ===")
	fmt.Fprintln(os.Stderr, "  second | calls this sec (rps) | running total | breakdown")
	fmt.Fprintln(os.Stderr, "  -------+---------------------+---------------+------------------")

	for second := 0; second < durationSec; second++ {
		if stop.Load() {
			return
		}
		wakeAt := start.Add(time.Duration(second+1) * time.Second)
		for {
			if stop.Load() {
				return
			}
			remaining := time.Until(wakeAt)
			if remaining <= 0 {
				break
			}
			time.Sleep(min(remaining, 50*time.Millisecond))
		}
		if stop.Load() {
			return
		}
		counts := m.bucketCounts(second)
		cumulative := m.cumulativeThrough(second)
		fmt.Fprintf(os.Stderr, "  %s\n", formatSecondLogLine(second, counts, cumulative))
	}
	fmt.Fprintln(os.Stderr)
}

var (
	ttyReaderOnce sync.Once
	ttyReader     *bufio.Reader
	ttyReaderErr  error
	cfgOnce       sync.Once
	sharedCfg     config
	cfgErr        error
)

// inputReader reads from the real terminal. go test often leaves os.Stdin
// disconnected/empty, so prompts would otherwise skip instantly.
func inputReader() (*bufio.Reader, error) {
	ttyReaderOnce.Do(func() {
		f, err := os.OpenFile("/dev/tty", os.O_RDONLY, 0)
		if err != nil {
			ttyReader = bufio.NewReader(os.Stdin)
			ttyReaderErr = err
			return
		}
		ttyReader = bufio.NewReader(f)
	})
	if ttyReader == nil {
		return nil, fmt.Errorf("no interactive input available")
	}
	return ttyReader, nil
}

func readLine() (string, error) {
	r, err := inputReader()
	if err != nil {
		return "", err
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func prompt(label, defaultVal string) string {
	if defaultVal != "" {
		fmt.Fprintf(os.Stderr, "%s [%s]: ", label, defaultVal)
	} else {
		fmt.Fprintf(os.Stderr, "%s: ", label)
	}
	line, err := readLine()
	if err != nil {
		fmt.Fprintln(os.Stderr)
		return defaultVal
	}
	if line == "" {
		return defaultVal
	}
	return line
}

func promptSecret(label, preset string) string {
	if preset != "" {
		fmt.Fprintf(os.Stderr, "%s [********]: ", label)
		line, err := readLine()
		if err != nil || line == "" {
			if err != nil {
				fmt.Fprintln(os.Stderr)
			}
			return preset
		}
		return line
	}
	for {
		fmt.Fprintf(os.Stderr, "%s: ", label)
		line, err := readLine()
		if err != nil {
			fmt.Fprintln(os.Stderr)
			return ""
		}
		if line != "" {
			return line
		}
	}
}

func promptPositiveInt(label string, defaultVal int) int {
	for {
		raw := prompt(label, strconv.Itoa(defaultVal))
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			return n
		}
		fmt.Fprintln(os.Stderr, "Enter a positive integer.")
	}
}

func promptFloat(label string, defaultVal float64) float64 {
	for {
		raw := prompt(label, strconv.FormatFloat(defaultVal, 'f', -1, 64))
		n, err := strconv.ParseFloat(raw, 64)
		if err == nil && n > 0 {
			return n
		}
		fmt.Fprintln(os.Stderr, "Enter a positive number.")
	}
}

func promptYesNo(label string, defaultYes bool) bool {
	hint := "y/N"
	if defaultYes {
		hint = "Y/n"
	}
	fmt.Fprintf(os.Stderr, "%s [%s]: ", label, hint)
	line, err := readLine()
	if err != nil {
		fmt.Fprintln(os.Stderr)
		return defaultYes
	}
	if line == "" {
		return defaultYes
	}
	line = strings.ToLower(line)
	return line == "y" || line == "yes"
}

func loadConfigInteractive() (config, error) {
	fmt.Fprintln(os.Stderr, "=== CypherTrap ALLOW stress test ===")
	fmt.Fprintln(os.Stderr, "Connects over TLS and sends ALLOW lines.")
	fmt.Fprintln(os.Stderr)

	cfg := config{
		server:      prompt("Server address", defaultServer),
		company:     prompt("Company", defaultCompany),
		keyName:     prompt("Key name", defaultKeyName),
		apiKey:      promptSecret("API key", defaultAPIKey),
		policy:      prompt("Policy", defaultPolicy),
		connections: promptPositiveInt("Parallel connections", defaultConns),
		baseReqs:    promptPositiveInt("Starting requests per second (per connection)", defaultBaseReqs),
		duration:    time.Duration(promptPositiveInt("Duration seconds", defaultDurationS)) * time.Second,
		minRPS:      promptFloat("Minimum RPS", defaultMinRPS),
	}

	durationSec := int(cfg.duration.Seconds())
	perConn := 0
	for s := 0; s < durationSec; s++ {
		perConn += requestsForSecond(s, cfg.baseReqs)
	}
	target := cfg.connections * perConn

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Configuration:")
	fmt.Fprintf(os.Stderr, "  Duration:        %s\n", cfg.duration)
	fmt.Fprintf(os.Stderr, "  Server:          %s\n", cfg.server)
	fmt.Fprintf(os.Stderr, "  Company:         %s\n", cfg.company)
	fmt.Fprintf(os.Stderr, "  Key name:        %s\n", cfg.keyName)
	fmt.Fprintf(os.Stderr, "  Policy:          %s\n", cfg.policy)
	fmt.Fprintf(os.Stderr, "  Connections:     %d\n", cfg.connections)
	fmt.Fprintf(os.Stderr, "  Second 0 batch:  %d req/conn\n", cfg.baseReqs)
	fmt.Fprintf(os.Stderr, "  Growth:          ×%d each second\n", secondGrowth)
	fmt.Fprintf(os.Stderr, "  Per-conn total:  %d requests\n", perConn)
	fmt.Fprintf(os.Stderr, "  Target calls:    %d\n", target)
	fmt.Fprintf(os.Stderr, "  Min RPS:         %.0f\n", cfg.minRPS)
	fmt.Fprintln(os.Stderr)

	if !promptYesNo("Start stress test?", false) {
		return config{}, fmt.Errorf("aborted by user")
	}
	return cfg, nil
}

func sharedConfig(t *testing.T) config {
	t.Helper()
	cfgOnce.Do(func() {
		sharedCfg, cfgErr = loadConfigInteractive()
	})
	if cfgErr != nil {
		t.Skip(cfgErr.Error())
	}
	return sharedCfg
}

func requestsForSecond(second, base int) int {
	batch := base
	for i := 0; i < second; i++ {
		batch *= secondGrowth
		if batch >= maxReqsPerSecond {
			return maxReqsPerSecond
		}
	}
	if batch > maxReqsPerSecond {
		return maxReqsPerSecond
	}
	return batch
}

func clientIPForWorker(workerID int) string {
	return fmt.Sprintf("10.%d.%d.%d",
		(workerID/65536)%256,
		(workerID/256)%256,
		workerID%256,
	)
}

func classifyResponse(line string) string {
	switch strings.TrimSpace(line) {
	case "ALLOWED":
		return "allowed"
	case "BLOCKED":
		return "blocked"
	case "UNAUTHORIZED":
		return "unauthorized"
	case "INVALID MESSAGE":
		return "invalid"
	case "INTERNAL SERVER ERROR":
		return "server_error"
	case "UNKNOWN COMMAND":
		return "unknown"
	default:
		return "other"
	}
}

func allowLine(clientIP, company, keyName, apiKey, policy string) string {
	return fmt.Sprintf("ALLOW %s %s %s %s %s\n", clientIP, company, keyName, apiKey, policy)
}

func openTLS(serverAddr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", serverAddr, &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	})
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(ioTimeout))
	return conn, nil
}

func serverReachable(serverAddr string) bool {
	conn, err := openTLS(serverAddr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func sendAllow(conn net.Conn, clientIP, company, keyName, apiKey, policy string) (string, error) {
	_ = conn.SetWriteDeadline(time.Now().Add(ioTimeout))
	if _, err := conn.Write([]byte(allowLine(clientIP, company, keyName, apiKey, policy))); err != nil {
		return "", err
	}
	_ = conn.SetReadDeadline(time.Now().Add(ioTimeout))
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "", err
	}
	return line, nil
}

func runWorker(
	workerID int,
	testStart time.Time,
	duration time.Duration,
	baseReqs int,
	serverAddr, company, keyName, apiKey, policy string,
	stop *atomic.Bool,
	m *metrics,
) {
	clientIP := clientIPForWorker(workerID)
	deadline := testStart.Add(duration)
	durationSec := int(duration.Seconds())
	if durationSec < 1 {
		durationSec = 1
	}
	reqLine := []byte(allowLine(clientIP, company, keyName, apiKey, policy))
	writeBuf := make([]byte, 0, len(reqLine)*pipelineDepth)
	local := map[string]int64{}

	var conn net.Conn
	var rbuf *bufio.Reader
	ensure := func() (net.Conn, *bufio.Reader, error) {
		if conn != nil {
			return conn, rbuf, nil
		}
		c, err := openTLS(serverAddr)
		if err != nil {
			return nil, nil, err
		}
		conn = c
		rbuf = bufio.NewReaderSize(c, 1<<20) // 1 MB — holds ~128k pipelined responses
		return conn, rbuf, nil
	}
	defer func() {
		m.flush(local)
		if conn != nil {
			_ = conn.Close()
		}
	}()

	writeBurst := func(c net.Conn, n int) error {
		writeBuf = writeBuf[:0]
		for i := 0; i < n; i++ {
			writeBuf = append(writeBuf, reqLine...)
		}
		_, err := c.Write(writeBuf)
		return err
	}

	for second := 0; second < durationSec; second++ {
		if stop.Load() {
			return
		}
		secondStart := testStart.Add(time.Duration(second) * time.Second)
		if wait := time.Until(secondStart); wait > 0 {
			time.Sleep(wait)
		}
		if stop.Load() || time.Now().After(deadline) {
			return
		}

		remaining := requestsForSecond(second, baseReqs)
		for remaining > 0 {
			if stop.Load() || time.Now().After(deadline) {
				return
			}
			c, r, err := ensure()
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			burst := remaining
			if burst > pipelineDepth {
				burst = pipelineDepth
			}

			_ = c.SetWriteDeadline(time.Now().Add(ioTimeout))
			if err := writeBurst(c, burst); err != nil {
				_ = c.Close()
				conn = nil
				rbuf = nil
				time.Sleep(50 * time.Millisecond)
				continue
			}

			_ = c.SetReadDeadline(time.Now().Add(ioTimeout * time.Duration(burst/500+2)))
			failed := false
			for i := 0; i < burst; i++ {
				line, err := r.ReadString('\n')
				if err != nil {
					_ = c.Close()
					conn = nil
					rbuf = nil
					failed = true
					break
				}
				local[classifyResponse(line)]++
			}
			m.flush(local)
			if failed {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			remaining -= burst
		}
	}
}

func TestAllowSingleRequest(t *testing.T) {
	cfg := sharedConfig(t)
	if !serverReachable(cfg.server) {
		t.Skipf("CypherTrap server not reachable at %s", cfg.server)
	}

	conn, err := openTLS(cfg.server)
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer conn.Close()

	line, err := sendAllow(conn, clientIPForWorker(0), cfg.company, cfg.keyName, cfg.apiKey, cfg.policy)
	if err != nil {
		t.Fatalf("send ALLOW: %v", err)
	}
	kind := classifyResponse(line)
	if kind != "allowed" && kind != "blocked" {
		t.Fatalf("expected ALLOWED/BLOCKED, got %q (%s)", line, kind)
	}
}

func TestAllowRPSThroughput(t *testing.T) {
	cfg := sharedConfig(t)
	if !serverReachable(cfg.server) {
		t.Skipf("CypherTrap server not reachable at %s", cfg.server)
	}

	var stop atomic.Bool
	start := time.Now()
	m := newMetrics(start)
	durationSec := int(cfg.duration.Seconds())
	if durationSec < 1 {
		durationSec = 1
	}

	var logWG sync.WaitGroup
	logWG.Add(1)
	go func() {
		defer logWG.Done()
		secondLogLoop(start, durationSec, m, &stop)
	}()

	var wg sync.WaitGroup
	wg.Add(cfg.connections)
	for id := 0; id < cfg.connections; id++ {
		id := id
		go func() {
			defer wg.Done()
			runWorker(id, start, cfg.duration, cfg.baseReqs, cfg.server, cfg.company, cfg.keyName, cfg.apiKey, cfg.policy, &stop, m)
		}()
	}
	wg.Wait()
	stop.Store(true)
	logWG.Wait()

	elapsed := time.Since(start).Seconds()
	totals := m.snapshot()
	var responses int64
	for _, n := range totals {
		responses += n
	}
	rps := 0.0
	if elapsed > 0 {
		rps = float64(responses) / elapsed
	}

	errors := totals["unauthorized"] + totals["server_error"] + totals["invalid"] + totals["unknown"] + totals["other"]
	allowPath := totals["allowed"] + totals["blocked"]

	t.Logf("=== ALLOW throughput ===")
	t.Logf("  Server:       %s", cfg.server)
	t.Logf("  Company/key:  %s/%s policy=%s", cfg.company, cfg.keyName, cfg.policy)
	t.Logf("  Connections:  %d", cfg.connections)
	t.Logf("  Duration:     %.2fs (target %s)", elapsed, cfg.duration)
	t.Logf("  Responses:    %d", responses)
	t.Logf("  Throughput:   %.1f req/s (min %.0f)", rps, cfg.minRPS)
	t.Logf("  ALLOWED:      %d", totals["allowed"])
	t.Logf("  BLOCKED:      %d", totals["blocked"])
	t.Logf("  Errors:       %d", errors)

	if responses == 0 {
		t.Fatal("no responses received")
	}
	if errors != 0 {
		t.Fatalf("unexpected error responses: %+v", totals)
	}
	if allowPath != responses {
		t.Fatalf("all responses should be ALLOWED or BLOCKED; got allowPath=%d responses=%d totals=%+v", allowPath, responses, totals)
	}
	if rps < cfg.minRPS {
		t.Fatalf("throughput %.1f req/s below minimum %.0f req/s", rps, cfg.minRPS)
	}
}
