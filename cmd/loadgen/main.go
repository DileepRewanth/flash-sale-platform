// Command loadgen drives a spike against the API, then audits correctness.
//
// Users arrive on a fixed schedule over the ramp, independent of how fast the
// server answers, so slow responses show up as latency instead of quietly
// lowering the offered load. The concurrency cap only bounds this process.
// While the spike runs it samples the in-flight metric and computes the replica
// count an HPA would request, to show how that signal drives autoscaling.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DileepRewanth/flash-sale-platform/internal/autoscale"
)

type opStats struct {
	mu        sync.Mutex
	latencies []time.Duration
	codes     map[int]int
}

type opSummary struct {
	Count int            `json:"count"`
	P50Ms float64        `json:"p50_ms"`
	P95Ms float64        `json:"p95_ms"`
	P99Ms float64        `json:"p99_ms"`
	MaxMs float64        `json:"max_ms"`
	Codes map[string]int `json:"codes"`
}

func (s *opStats) add(d time.Duration, code int) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.codes[code]++
	s.mu.Unlock()
}

func (s *opStats) summarize() opSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := opSummary{Count: len(s.latencies), Codes: map[string]int{}}
	for c, n := range s.codes {
		out.Codes[strconv.Itoa(c)] = n // code 0 means a transport error
	}
	if len(s.latencies) == 0 {
		return out
	}
	sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })
	q := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(s.latencies)))) - 1
		if i < 0 {
			i = 0
		}
		return float64(s.latencies[i].Microseconds()) / 1000
	}
	out.P50Ms, out.P95Ms, out.P99Ms = q(0.50), q(0.95), q(0.99)
	out.MaxMs = float64(s.latencies[len(s.latencies)-1].Microseconds()) / 1000
	return out
}

type point struct {
	AtMs     int64   `json:"at_ms"`
	InFlight float64 `json:"inflight"`
	Desired  int     `json:"desired_replicas"`
}

type scaleSummary struct {
	TargetPerReplica float64 `json:"target_inflight_per_replica"`
	Samples          int     `json:"samples"`
	MaxInFlight      float64 `json:"max_inflight"`
	MaxDesired       int     `json:"max_desired_replicas"`
	Timeline         []point `json:"timeline,omitempty"`
}

func main() {
	target := flag.String("target", "http://localhost:8080", "API base URL")
	sale := flag.String("sale", "drop-1", "sale ID")
	users := flag.Int("users", 20000, "number of virtual users")
	ramp := flag.Duration("ramp", 10*time.Second, "time over which users arrive")
	pay := flag.Float64("pay", 0.9, "fraction of holders who start payment")
	conc := flag.Int("concurrency", 256, "maximum requests in flight from this generator")
	perReplica := flag.Float64("target-inflight", 50, "autoscaling target: in-flight requests per replica")
	settle := flag.Duration("settle", 3*time.Second, "wait for background payment processing before the audit")
	timeline := flag.Bool("timeline", false, "include the sampled autoscaling timeline in the output")
	flag.Parse()
	if *users < 1 {
		fmt.Fprintln(os.Stderr, "users must be at least 1")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConns: *conc, MaxIdleConnsPerHost: *conc}}
	stats := map[string]*opStats{"reserve": {codes: map[int]int{}}, "order": {codes: map[int]int{}}}
	sem := make(chan struct{}, *conc)
	stopSampling := make(chan struct{})
	scaleOut := make(chan scaleSummary, 1)
	go sample(client, *target, *perReplica, stopSampling, scaleOut)

	var wg sync.WaitGroup
	start := time.Now()
	gap := *ramp / time.Duration(*users)
	for i := 0; i < *users; i++ {
		if d := time.Until(start.Add(time.Duration(i) * gap)); d > 0 {
			time.Sleep(d)
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			runUser(client, *target, *sale, fmt.Sprintf("user%07d", i), rand.Float64() < *pay, stats)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(stopSampling)
	scale := <-scaleOut
	if !*timeline {
		scale.Timeline = nil
	}

	time.Sleep(*settle)
	audit, auditErr := fetchAudit(client, *target, *sale, os.Getenv("ADMIN_TOKEN"))
	reserve, order := stats["reserve"].summarize(), stats["order"].summarize()
	total := reserve.Count + order.Count
	out := map[string]any{
		"users": *users, "ramp_s": ramp.Seconds(), "elapsed_s": elapsed.Seconds(),
		"requests": total, "throughput_rps": float64(total) / elapsed.Seconds(),
		"reserve": reserve, "order": order, "autoscaling": scale,
	}
	if auditErr != nil {
		out["audit_error"] = auditErr.Error()
	} else {
		out["audit"] = audit
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	if auditErr == nil && (audit["oversold_units"] != float64(0) || audit["duplicate_charges"] != float64(0)) {
		fmt.Fprintln(os.Stderr, "CORRECTNESS FAILURE: oversold units or duplicate charges detected")
		os.Exit(1)
	}
}

func runUser(c *http.Client, target, sale, user string, pay bool, stats map[string]*opStats) {
	code, body, d := do(c, http.MethodPost, target+"/v1/sales/"+sale+"/holds", user, "", nil)
	stats["reserve"].add(d, code)
	if code != http.StatusCreated || !pay {
		return
	}
	var hold struct {
		HoldToken string `json:"hold_token"`
	}
	if json.Unmarshal(body, &hold) != nil {
		return
	}
	payload, _ := json.Marshal(map[string]string{"hold_token": hold.HoldToken})
	code, _, d = do(c, http.MethodPost, target+"/v1/orders", user, user+"-order", payload)
	stats["order"].add(d, code)
}

func do(c *http.Client, method, url, user, idem string, body []byte) (int, []byte, time.Duration) {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, 0
	}
	req.Header.Set("X-User-ID", user)
	req.Header.Set("Content-Type", "application/json")
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	start := time.Now()
	resp, err := c.Do(req)
	d := time.Since(start)
	if err != nil {
		return 0, nil, d
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, d
}

func sample(c *http.Client, target string, perReplica float64, stop <-chan struct{}, out chan<- scaleSummary) {
	s := scaleSummary{TargetPerReplica: perReplica}
	start := time.Now()
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			out <- s
			return
		case <-t.C:
			v, ok := scrapeInFlight(c, target)
			if !ok {
				continue
			}
			d := autoscale.Desired(v, perReplica, 1, 100)
			s.Samples++
			if v > s.MaxInFlight {
				s.MaxInFlight = v
			}
			if d > s.MaxDesired {
				s.MaxDesired = d
			}
			if len(s.Timeline) < 400 {
				s.Timeline = append(s.Timeline, point{AtMs: time.Since(start).Milliseconds(), InFlight: v, Desired: d})
			}
		}
	}
}

func scrapeInFlight(c *http.Client, target string) (float64, bool) {
	resp, err := c.Get(target + "/metrics")
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	const name = "flashsale_http_inflight_requests "
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		if line := sc.Text(); strings.HasPrefix(line, name) {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name)), 64)
			return v, err == nil
		}
	}
	return 0, false
}

func fetchAudit(c *http.Client, target, sale, admin string) (map[string]any, error) {
	if admin == "" {
		return nil, fmt.Errorf("ADMIN_TOKEN not set, so the audit was skipped")
	}
	req, err := http.NewRequest(http.MethodGet, target+"/v1/admin/sales/"+sale+"/audit", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+admin)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("audit returned %d", resp.StatusCode)
	}
	var m map[string]any
	return m, json.NewDecoder(resp.Body).Decode(&m)
}
