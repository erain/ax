// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package puller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

// fakeRunner is an httptest runner serving GET /usage with a settable body.
type fakeRunner struct {
	srv  *httptest.Server
	ip   string
	port int

	mu      sync.Mutex
	body    string
	status  int
	delay   time.Duration
	release chan struct{}
	hits    atomic.Int64
	paths   []string
}

func newFakeRunner(t *testing.T) *fakeRunner {
	t.Helper()
	f := &fakeRunner{status: http.StatusOK, release: make(chan struct{})}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	t.Cleanup(func() { close(f.release) }) // runs before srv.Close (LIFO)
	host, port, err := net.SplitHostPort(f.srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	f.ip = host
	f.port, _ = strconv.Atoi(port)
	return f
}

func (f *fakeRunner) serve(w http.ResponseWriter, r *http.Request) {
	f.hits.Add(1)
	f.mu.Lock()
	body, status, delay := f.body, f.status, f.delay
	f.paths = append(f.paths, r.Method+" "+r.URL.Path)
	f.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-f.release:
		case <-r.Context().Done():
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func (f *fakeRunner) set(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.body = status, body
}

func (f *fakeRunner) setDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

// reportJSON renders a runner GET /usage body in the T05 wire format.
func reportJSON(t *testing.T, session string, at time.Time, tokens map[string]map[string]any) string {
	t.Helper()
	m := map[string]any{
		"session":        session,
		"reportTime":     at.Format(time.RFC3339Nano),
		"commandExited":  false,
		"exitCode":       0,
		"workspaceBytes": 0,
		"tokens":         tokens,
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

type staticLister struct {
	mu    sync.Mutex
	tasks []*v1alpha1.Task
	err   error
}

func (l *staticLister) ListRunningTasks(context.Context) ([]*v1alpha1.Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tasks, l.err
}

func task(name, phase, ip string) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "default"},
		Status:   &v1alpha1.TaskStatus{Phase: phase, WorkerIp: ip},
	}
}

// tokenPricer charges $1 per 1000 input tokens and $2 per 1000 output tokens;
// it is linear as usage.Pricer requires. It records the pools it was asked for.
type tokenPricer struct {
	mu    sync.Mutex
	pools []string
}

func (p *tokenPricer) Price(d usage.Delta, pool string) usage.Cost {
	p.mu.Lock()
	p.pools = append(p.pools, pool)
	p.mu.Unlock()
	var c usage.Cost
	c.Currency = "USD"
	for _, tc := range d.Sum().Tokens {
		c.Tokens += float64(tc.Input)/1000 + 2*float64(tc.Output)/1000
	}
	c.Total = c.Tokens
	return c
}

type hookCall struct {
	key  usage.Key
	sum  usage.DayDelta
	cost usage.Cost
	tot  usage.Cost
}

type recHooks struct {
	mu    sync.Mutex
	calls []hookCall
}

func (h *recHooks) OnDelta(_ context.Context, key usage.Key, d usage.Delta, c usage.Cost, u *usage.TaskUsage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, hookCall{key: key, sum: d.Sum(), cost: c, tot: u.Cost})
}

func (h *recHooks) snapshot() []hookCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]hookCall(nil), h.calls...)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestPuller(f *fakeRunner, lister TaskLister, store usage.Store, pricer usage.Pricer, hooks usage.Hooks) *Puller {
	return New(Options{
		Tasks:   lister,
		Store:   store,
		Pricer:  pricer,
		Hooks:   hooks,
		Port:    f.port,
		Timeout: 2 * time.Second,
		Now:     func() time.Time { return t0 },
		Logger:  quietLogger(),
	})
}

func approx(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}

func TestSweep_CumulativeReportsCountedOncePerIncrement(t *testing.T) {
	ctx := context.Background()
	f := newFakeRunner(t)
	store := usage.NewMemoryStore()
	pricer := &tokenPricer{}
	hooks := &recHooks{}
	lister := &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, f.ip)}}
	key := usage.Key{Atespace: "default", Name: "t1"}

	// The ingestor already recorded the pool; pricing must use it.
	if err := store.Update(ctx, key, func(u *usage.TaskUsage) error {
		u.State.Pool = "pool-a"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	p := newTestPuller(f, lister, store, pricer, hooks)

	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{
		"gemini-2.5-flash": {"input": 1000, "output": 500, "cacheRead": 0, "cacheWrite": 0, "reasoning": 0},
	}))
	p.Sweep(ctx)

	f.set(http.StatusOK, reportJSON(t, "s1", t0.Add(time.Minute), map[string]map[string]any{
		"gemini-2.5-flash": {"input": 3000, "output": 1000, "cacheRead": 200, "cacheWrite": 0, "reasoning": 100},
	}))
	p.Sweep(ctx)

	// A repeated identical report changes nothing.
	p.Sweep(ctx)

	u, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	want := usage.TokenCounts{Input: 3000, Output: 1000, CacheRead: 200, Reasoning: 100}
	if got := u.Tokens["gemini-2.5-flash"]; got != want {
		t.Errorf("lifetime tokens = %+v, want %+v", got, want)
	}
	// cost: (1000/1000 + 2*500/1000) + (2000/1000 + 2*500/1000) = 2 + 3 = 5
	if !approx(u.Cost.Total, 5) || !approx(u.Cost.Tokens, 5) {
		t.Errorf("lifetime cost = %+v, want total/tokens 5", u.Cost)
	}
	if day := u.Days[usage.DateOf(t0)]; day == nil || !approx(day.Cost.Total, 5) {
		t.Errorf("day cost = %+v, want 5", day)
	}

	calls := hooks.snapshot()
	if len(calls) != 2 {
		t.Fatalf("hook calls = %d, want 2: %+v", len(calls), calls)
	}
	if got := calls[0].sum.Tokens["gemini-2.5-flash"]; got != (usage.TokenCounts{Input: 1000, Output: 500}) {
		t.Errorf("first delta = %+v", got)
	}
	if got := calls[1].sum.Tokens["gemini-2.5-flash"]; got != (usage.TokenCounts{Input: 2000, Output: 500, CacheRead: 200, Reasoning: 100}) {
		t.Errorf("second delta = %+v", got)
	}
	if !approx(calls[0].cost.Total, 2) || !approx(calls[1].cost.Total, 3) {
		t.Errorf("hook costs = %v, %v; want 2, 3", calls[0].cost.Total, calls[1].cost.Total)
	}
	if !approx(calls[1].tot.Total, 5) {
		t.Errorf("hook usage total = %v, want 5 (cost added before hook)", calls[1].tot.Total)
	}
	if calls[0].key != key {
		t.Errorf("hook key = %+v, want %+v", calls[0].key, key)
	}

	pricer.mu.Lock()
	for _, pool := range pricer.pools {
		if pool != "pool-a" {
			t.Errorf("Price called with pool %q, want pool-a", pool)
		}
	}
	pricer.mu.Unlock()

	st := p.Stats()
	if st.Sweeps != 3 || st.Pulls != 3 || st.Failures != 0 || st.Applied != 2 {
		t.Errorf("stats = %+v, want 3 sweeps, 3 pulls, 0 failures, 2 applied", st)
	}
	f.mu.Lock()
	for _, path := range f.paths {
		if path != "GET /usage" {
			t.Errorf("runner request %q, want GET /usage", path)
		}
	}
	f.mu.Unlock()
}

func TestSweep_RunnerRestartNewSessionNotDoubleCounted(t *testing.T) {
	ctx := context.Background()
	f := newFakeRunner(t)
	store := usage.NewMemoryStore()
	lister := &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, f.ip)}}
	p := newTestPuller(f, lister, store, &tokenPricer{}, nil)

	tok := map[string]map[string]any{"m": {"input": 100, "output": 10}}
	f.set(http.StatusOK, reportJSON(t, "s1", t0, tok))
	p.Sweep(ctx)
	// Runner restarted: new session, same persisted counters, then growth.
	f.set(http.StatusOK, reportJSON(t, "s2", t0.Add(time.Minute), tok))
	p.Sweep(ctx)
	f.set(http.StatusOK, reportJSON(t, "s2", t0.Add(2*time.Minute), map[string]map[string]any{"m": {"input": 150, "output": 20}}))
	p.Sweep(ctx)

	u, err := store.Get(ctx, usage.Key{Atespace: "default", Name: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := u.Tokens["m"], (usage.TokenCounts{Input: 150, Output: 20}); got != want {
		t.Errorf("tokens = %+v, want %+v", got, want)
	}
}

func TestSweep_NeverContactsNonRunningTasks(t *testing.T) {
	ctx := context.Background()
	f := newFakeRunner(t)
	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 1}}))
	store := usage.NewMemoryStore()
	lister := &staticLister{tasks: []*v1alpha1.Task{
		task("running-no-ip", PhaseRunning, ""),
		task("suspended", "Suspended", f.ip),
		task("pending", "Pending", f.ip),
		task("completed", "Completed", f.ip),
		task("no-phase", "", f.ip),
		{Metadata: &v1alpha1.ObjectMeta{Name: "no-status"}},
		nil,
	}}
	p := newTestPuller(f, lister, store, nil, nil)

	p.Sweep(ctx)
	p.Sweep(ctx)
	for _, tk := range lister.tasks {
		if err := p.Flush(ctx, tk); err != nil {
			t.Errorf("Flush(%s) = %v, want nil", tk.GetMetadata().GetName(), err)
		}
	}
	if n := f.hits.Load(); n != 0 {
		t.Fatalf("runner contacted %d times, want 0", n)
	}
	all, err := store.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Errorf("store has %d records, want 0", len(all))
	}

	// Adding one Running task with an IP: exactly one hit per sweep.
	lister.mu.Lock()
	lister.tasks = append(lister.tasks, task("running", PhaseRunning, f.ip))
	lister.mu.Unlock()
	p.Sweep(ctx)
	p.Sweep(ctx)
	if n := f.hits.Load(); n != 2 {
		t.Errorf("runner contacted %d times, want 2", n)
	}
}

func TestPull_FailuresDoNotChangeData(t *testing.T) {
	good := map[string]map[string]any{"m": {"input": 100, "output": 10}}
	tests := []struct {
		name   string
		status int
		body   func(t *testing.T) string
		delay  time.Duration
	}{
		{name: "500", status: http.StatusInternalServerError, body: func(*testing.T) string { return "boom" }},
		{name: "404", status: http.StatusNotFound, body: func(*testing.T) string { return "" }},
		{name: "garbage json", status: http.StatusOK, body: func(*testing.T) string { return "{not json" }},
		{name: "html", status: http.StatusOK, body: func(*testing.T) string { return "<html></html>" }},
		{name: "wrong types", status: http.StatusOK, body: func(*testing.T) string {
			return `{"session":"s1","reportTime":"2026-10-16T12:05:00Z","tokens":{"m":{"input":"lots"}}}`
		}},
		{name: "tokens not an object", status: http.StatusOK, body: func(*testing.T) string {
			return `{"session":"s1","reportTime":"2026-10-16T12:05:00Z","tokens":[1,2]}`
		}},
		{name: "negative counter", status: http.StatusOK, body: func(t *testing.T) string {
			return reportJSON(t, "s1", t0.Add(5*time.Minute), map[string]map[string]any{"m": {"input": -5}})
		}},
		{name: "oversized body", status: http.StatusOK, body: func(*testing.T) string {
			return `{"session":"` + strings.Repeat("x", maxReportBytes) + `"}`
		}},
		{name: "timeout", status: http.StatusOK, delay: time.Minute, body: func(t *testing.T) string {
			return reportJSON(t, "s1", t0.Add(5*time.Minute), map[string]map[string]any{"m": {"input": 999}})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFakeRunner(t)
			store := usage.NewMemoryStore()
			hooks := &recHooks{}
			lister := &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, f.ip)}}
			p := New(Options{
				Tasks: lister, Store: store, Pricer: &tokenPricer{}, Hooks: hooks,
				Port: f.port, Timeout: 200 * time.Millisecond,
				Now: func() time.Time { return t0 }, Logger: quietLogger(),
			})
			key := usage.Key{Atespace: "default", Name: "t1"}

			f.set(http.StatusOK, reportJSON(t, "s1", t0, good))
			p.Sweep(ctx)
			before, err := store.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			beforeJSON, _ := json.Marshal(before)

			f.set(tc.status, tc.body(t))
			f.setDelay(tc.delay)
			start := time.Now()
			p.Sweep(ctx)
			if el := time.Since(start); el > 2*time.Second {
				t.Errorf("sweep took %v, want bounded by timeout", el)
			}

			after, err := store.Get(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			afterJSON, _ := json.Marshal(after)
			if string(beforeJSON) != string(afterJSON) {
				t.Errorf("usage changed after failed pull:\nbefore %s\nafter  %s", beforeJSON, afterJSON)
			}
			if n := len(hooks.snapshot()); n != 1 {
				t.Errorf("hook calls = %d, want 1 (only the good pull)", n)
			}
			if st := p.Stats(); st.Failures != 1 || st.Pulls != 2 {
				t.Errorf("stats = %+v, want 1 failure of 2 pulls", st)
			}
		})
	}
}

func TestPull_ConnectionRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	store := usage.NewMemoryStore()
	p := New(Options{Store: store, Port: port, Timeout: time.Second, Logger: quietLogger()})
	if err := p.Flush(context.Background(), task("t1", PhaseRunning, "127.0.0.1")); err == nil {
		t.Fatal("Flush to closed port = nil, want error")
	}
	if all, _ := store.List(context.Background(), ""); len(all) != 0 {
		t.Errorf("store has %d records, want 0", len(all))
	}
}

func TestFlush(t *testing.T) {
	ctx := context.Background()
	f := newFakeRunner(t)
	store := usage.NewMemoryStore()
	hooks := &recHooks{}
	p := newTestPuller(f, nil, store, &tokenPricer{}, hooks)
	tk := task("t1", PhaseRunning, f.ip)

	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 2000, "output": 0}}))
	if err := p.Flush(ctx, tk); err != nil {
		t.Fatalf("Flush = %v", err)
	}
	u, err := store.Get(ctx, usage.Key{Atespace: "default", Name: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Tokens["m"].Input; got != 2000 {
		t.Errorf("input tokens = %d, want 2000", got)
	}
	if !approx(u.Cost.Total, 2) {
		t.Errorf("cost = %v, want 2", u.Cost.Total)
	}
	if n := len(hooks.snapshot()); n != 1 {
		t.Errorf("hook calls = %d, want 1", n)
	}
	// Sweep with a nil lister is a no-op.
	p.Sweep(ctx)
	if n := f.hits.Load(); n != 1 {
		t.Errorf("hits = %d, want 1", n)
	}
}

func TestFlush_RespectsTimeout(t *testing.T) {
	f := newFakeRunner(t)
	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 1}}))
	f.setDelay(time.Minute)
	store := usage.NewMemoryStore()
	p := New(Options{Store: store, Port: f.port, Timeout: 150 * time.Millisecond, Logger: quietLogger()})

	start := time.Now()
	err := p.Flush(context.Background(), task("t1", PhaseRunning, f.ip))
	el := time.Since(start)
	if err == nil {
		t.Fatal("Flush = nil, want timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "deadline") && !strings.Contains(err.Error(), "Timeout") {
		t.Errorf("Flush error = %v, want a deadline error", err)
	}
	if el > time.Second {
		t.Errorf("Flush took %v, want ~150ms", el)
	}
	if _, err := store.Get(context.Background(), usage.Key{Atespace: "default", Name: "t1"}); !errors.Is(err, usage.ErrNotFound) {
		t.Errorf("store Get = %v, want ErrNotFound", err)
	}

	// A caller deadline shorter than Timeout also bounds Flush.
	p2 := New(Options{Store: store, Port: f.port, Timeout: time.Minute, Logger: quietLogger()})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	if err := p2.Flush(ctx, task("t1", PhaseRunning, f.ip)); err == nil {
		t.Error("Flush with expired caller ctx = nil, want error")
	}
	if el := time.Since(start); el > time.Second {
		t.Errorf("Flush took %v with a 100ms caller deadline", el)
	}
}

func TestSweep_BoundedConcurrency(t *testing.T) {
	var inflight, maxInflight atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		fmt.Fprintf(w, `{"session":"s","reportTime":%q,"tokens":{"m":{"input":1}}}`, t0.Format(time.RFC3339))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().(*net.TCPAddr)

	var tasks []*v1alpha1.Task
	for i := range 20 {
		tasks = append(tasks, task(fmt.Sprintf("t%d", i), PhaseRunning, addr.IP.String()))
	}
	store := usage.NewMemoryStore()
	p := New(Options{
		Tasks: &staticLister{tasks: tasks}, Store: store, Port: addr.Port,
		Concurrency: 3, Logger: quietLogger(),
	})
	p.Sweep(context.Background())
	if m := maxInflight.Load(); m > 3 || m < 1 {
		t.Errorf("max in-flight pulls = %d, want 1..3", m)
	}
	all, err := store.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 20 {
		t.Errorf("records = %d, want 20", len(all))
	}
	if st := p.Stats(); st.Pulls != 20 || st.Failures != 0 || st.Applied != 20 {
		t.Errorf("stats = %+v", st)
	}
}

func TestSweep_ListerErrorIsNotFatal(t *testing.T) {
	p := New(Options{Tasks: &staticLister{err: errors.New("redis down")}, Store: usage.NewMemoryStore(), Logger: quietLogger()})
	p.Sweep(context.Background())
	if st := p.Stats(); st.Pulls != 0 {
		t.Errorf("stats = %+v, want no pulls", st)
	}
}

func TestRun_SweepsPeriodicallyUntilCanceled(t *testing.T) {
	f := newFakeRunner(t)
	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 1}}))
	p := New(Options{
		Tasks:    &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, f.ip)}},
		Store:    usage.NewMemoryStore(),
		Port:     f.port,
		Interval: 10 * time.Millisecond,
		Logger:   quietLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for f.hits.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if n := f.hits.Load(); n < 3 {
		t.Errorf("hits = %d, want >= 3", n)
	}
}

func TestDecodeReport(t *testing.T) {
	// Exact T05 wire format, including a model with provider cost.
	body := `{"session":"abc","reportTime":"2026-10-16T12:00:00.123456789Z","commandExited":true,"exitCode":3,` +
		`"workspaceBytes":4096,"tokens":{"gemini-2.5-pro":{"input":10,"output":20,"cacheRead":3,"cacheWrite":4,` +
		`"reasoning":5,"costAmount":0.25,"costCurrency":"USD"},"gemini-2.5-flash":{"input":1,"output":2,` +
		`"cacheRead":0,"cacheWrite":0,"reasoning":0}}}`
	got, err := decodeReport([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if got.Session != "abc" || !got.CommandExited || got.ExitCode != 3 || got.WorkspaceBytes != 4096 {
		t.Errorf("report = %+v", got)
	}
	if want := time.Date(2026, 10, 16, 12, 0, 0, 123456789, time.UTC); !got.ReportTime.Equal(want) {
		t.Errorf("reportTime = %v, want %v", got.ReportTime, want)
	}
	if want := (usage.RunnerTokens{Input: 10, Output: 20, CacheRead: 3, CacheWrite: 4, Reasoning: 5, CostAmount: 0.25, CostCurrency: "USD"}); got.Tokens["gemini-2.5-pro"] != want {
		t.Errorf("pro = %+v, want %+v", got.Tokens["gemini-2.5-pro"], want)
	}
	if want := (usage.RunnerTokens{Input: 1, Output: 2}); got.Tokens["gemini-2.5-flash"] != want {
		t.Errorf("flash = %+v, want %+v", got.Tokens["gemini-2.5-flash"], want)
	}

	empty, err := decodeReport([]byte(`{"session":"s","reportTime":"2026-10-16T12:00:00Z","tokens":{}}`))
	if err != nil || empty.Tokens != nil {
		t.Errorf("empty tokens: %+v, %v", empty, err)
	}
	if _, err := decodeReport([]byte(`{"workspaceBytes":-1}`)); !errors.Is(err, errBadReport) {
		t.Errorf("negative workspaceBytes err = %v", err)
	}
}

func TestPull_RedirectIsFailureAndNotFollowed(t *testing.T) {
	var targetHits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer target.Close()

	for _, status := range []int{http.StatusFound, http.StatusMovedPermanently, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, callerClient := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/callerClient=%v", status, callerClient), func(t *testing.T) {
				targetHits.Store(0)
				var runnerHits atomic.Int64
				runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					runnerHits.Add(1)
					http.Redirect(w, r, target.URL+"/internal-secret", status)
				}))
				defer runner.Close()
				addr := runner.Listener.Addr().(*net.TCPAddr)

				opts := Options{Store: usage.NewMemoryStore(), Port: addr.Port, Timeout: time.Second, Logger: quietLogger()}
				if callerClient {
					// A plain client follows redirects by default; New must
					// still enforce the no-redirect policy.
					opts.Client = &http.Client{}
				}
				p := New(opts)
				err := p.Flush(context.Background(), task("t1", PhaseRunning, addr.IP.String()))
				if err == nil {
					t.Fatal("Flush with redirecting runner = nil, want error")
				}
				if n := targetHits.Load(); n != 0 {
					t.Errorf("redirect target hit %d times, want 0", n)
				}
				if n := runnerHits.Load(); n != 1 {
					t.Errorf("runner hit %d times, want 1", n)
				}
				if st := p.Stats(); st.Failures != 1 {
					t.Errorf("stats = %+v, want 1 failure", st)
				}
				if opts.Client != nil && opts.Client.CheckRedirect != nil {
					t.Error("caller's client was mutated")
				}
			})
		}
	}
}

// countingTransport records every request that reaches the transport.
type countingTransport struct{ n atomic.Int64 }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.n.Add(1)
	return nil, errors.New("countingTransport: no network")
}

func TestPull_RejectsInvalidWorkerIP(t *testing.T) {
	for _, ip := range []string{
		"localhost",
		"example.com",
		"metadata.google.internal",
		"169.254.169.254",
		"169.254.1.1",
		"::ffff:169.254.169.254",
		"fe80::1",
		"fe80::1%eth0",
		"0.0.0.0",
		"::",
		"224.0.0.1",
		"ff02::1",
		"10.0.0.1:80",
		"10.0.0.1/24",
		"http://10.0.0.1",
	} {
		t.Run(ip, func(t *testing.T) {
			rt := &countingTransport{}
			store := usage.NewMemoryStore()
			p := New(Options{
				Tasks:  &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, ip)}},
				Store:  store,
				Client: &http.Client{Transport: rt},
				Logger: quietLogger(),
			})
			p.Sweep(context.Background())
			err := p.Flush(context.Background(), task("t1", PhaseRunning, ip))
			if !errors.Is(err, errBadWorkerIP) {
				t.Errorf("Flush = %v, want errBadWorkerIP", err)
			}
			if n := rt.n.Load(); n != 0 {
				t.Errorf("transport saw %d requests, want 0", n)
			}
			if st := p.Stats(); st.Failures != 2 || st.Pulls != 2 {
				t.Errorf("stats = %+v, want 2 failed pulls", st)
			}
			if all, _ := store.List(context.Background(), ""); len(all) != 0 {
				t.Errorf("store has %d records, want 0", len(all))
			}
		})
	}
}

func TestCheckWorkerIP_Allowed(t *testing.T) {
	for _, ip := range []string{"10.8.0.12", "127.0.0.1", "::1", "2600:1900::1", "fd00::5", "::ffff:10.0.0.1"} {
		if _, err := checkWorkerIP(ip); err != nil {
			t.Errorf("checkWorkerIP(%q) = %v, want nil", ip, err)
		}
	}
}

func TestPull_FutureReportTimeIsClamped(t *testing.T) {
	ctx := context.Background()
	f := newFakeRunner(t)
	store := usage.NewMemoryStore()
	lister := &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, f.ip)}}
	now := t0
	p := New(Options{
		Tasks: lister, Store: store, Port: f.port, Timeout: 2 * time.Second,
		Now: func() time.Time { return now }, Logger: quietLogger(),
	})
	key := usage.Key{Atespace: "default", Name: "t1"}

	// A sandbox claiming a reportTime a year ahead.
	f.set(http.StatusOK, reportJSON(t, "s1", t0.Add(365*24*time.Hour), map[string]map[string]any{"m": {"input": 100}}))
	p.Sweep(ctx)
	u, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got := u.State.RunnerReportTime; !got.Equal(t0) {
		t.Errorf("RunnerReportTime = %v, want clamped to now %v", got, t0)
	}

	// Later honest reports are still applied.
	now = t0.Add(time.Minute)
	f.set(http.StatusOK, reportJSON(t, "s1", now, map[string]map[string]any{"m": {"input": 250}}))
	p.Sweep(ctx)
	if u, err = store.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got := u.Tokens["m"].Input; got != 250 {
		t.Errorf("input tokens = %d, want 250", got)
	}

	// Within the allowed skew the runner's time is kept.
	now = t0.Add(2 * time.Minute)
	skewed := now.Add(MaxClockSkew - time.Second)
	f.set(http.StatusOK, reportJSON(t, "s1", skewed, map[string]map[string]any{"m": {"input": 300}}))
	p.Sweep(ctx)
	if u, err = store.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got := u.State.RunnerReportTime; !got.Equal(skewed) {
		t.Errorf("RunnerReportTime = %v, want %v", got, skewed)
	}
}

// ctxHooks records whether the hook context was usable.
type ctxHooks struct {
	mu       sync.Mutex
	err      error
	deadline time.Duration
}

func (h *ctxHooks) OnDelta(ctx context.Context, _ usage.Key, _ usage.Delta, _ usage.Cost, _ *usage.TaskUsage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.err = ctx.Err()
	if dl, ok := ctx.Deadline(); ok {
		h.deadline = time.Until(dl)
	}
}

func TestPull_HooksGetFreshContext(t *testing.T) {
	f := newFakeRunner(t)
	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 1}}))
	hooks := &ctxHooks{}
	p := New(Options{Store: usage.NewMemoryStore(), Hooks: hooks, Port: f.port, Timeout: time.Second,
		Now: func() time.Time { return t0 }, Logger: quietLogger()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Flush(ctx, task("t1", PhaseRunning, f.ip)); err != nil {
		t.Fatal(err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.err != nil {
		t.Errorf("hook ctx err = %v, want nil", hooks.err)
	}
	if hooks.deadline <= time.Second || hooks.deadline > hookTimeout {
		t.Errorf("hook ctx deadline in %v, want (Timeout, hookTimeout]", hooks.deadline)
	}
}

func TestPull_StateOnlyChangeIsPersisted(t *testing.T) {
	ctx := context.Background()
	f := newFakeRunner(t)
	store := usage.NewMemoryStore()
	hooks := &recHooks{}
	lister := &staticLister{tasks: []*v1alpha1.Task{task("t1", PhaseRunning, f.ip)}}
	p := newTestPuller(f, lister, store, nil, hooks)
	key := usage.Key{Atespace: "default", Name: "t1"}

	tok := map[string]map[string]any{"m": {"input": 10}}
	f.set(http.StatusOK, reportJSON(t, "s1", t0, tok))
	p.Sweep(ctx)
	// Same tokens, but the command exited: zero Delta, state advanced.
	f.set(http.StatusOK, strings.Replace(reportJSON(t, "s1", t0.Add(time.Minute), tok), `"commandExited":false`, `"commandExited":true`, 1))
	p.Sweep(ctx)
	u, err := store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !u.State.CommandExited || !u.State.RunnerReportTime.Equal(t0.Add(time.Minute)) {
		t.Errorf("state = %+v, want commandExited and advanced reportTime persisted", u.State)
	}
	if n := len(hooks.snapshot()); n != 1 {
		t.Errorf("hook calls = %d, want 1 (zero deltas do not fire hooks)", n)
	}
}

func TestSweep_LogsSummaryWithLastError(t *testing.T) {
	ok := newFakeRunner(t)
	ok.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 1}}))
	bad := newFakeRunner(t)
	bad.set(http.StatusInternalServerError, "boom")
	var buf syncBuffer
	p := New(Options{
		Tasks: &staticLister{tasks: []*v1alpha1.Task{
			task("good", PhaseRunning, ok.ip),
			task("idle", "Suspended", ok.ip),
		}},
		Store: usage.NewMemoryStore(), Port: ok.port, Timeout: time.Second,
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	p.Sweep(context.Background())
	if got := buf.String(); !strings.Contains(got, `msg="usage puller sweep" pulled=1 failed=0 applied=1`) {
		t.Errorf("first sweep log = %q", got)
	}

	buf.Reset()
	p2 := New(Options{
		Tasks: &staticLister{tasks: []*v1alpha1.Task{task("bad", PhaseRunning, bad.ip)}},
		Store: usage.NewMemoryStore(), Port: bad.port, Timeout: time.Second,
		Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	p2.Sweep(context.Background())
	got := buf.String()
	for _, want := range []string{`pulled=1 failed=1 applied=0`, `lastErrorTask=default/bad`, `lastErrorWorkerIP=` + bad.ip, `lastError=`} {
		if !strings.Contains(got, want) {
			t.Errorf("failing sweep log %q lacks %q", got, want)
		}
	}

	buf.Reset()
	p3 := New(Options{Tasks: &staticLister{}, Store: usage.NewMemoryStore(), Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	p3.Sweep(context.Background())
	if strings.Contains(buf.String(), "usage puller sweep") {
		t.Errorf("sweep without pulls logged %q", buf.String())
	}
}

// syncBuffer is a goroutine-safe bytes.Buffer for log capture.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.Reset()
}

// With RouterAddr, pulls go to the router with the ate-target-actor header
// (Substrate v0.5.0 does not expose actor ports on the worker pod IP).
func TestPull_ViaRouter(t *testing.T) {
	var (
		mu      sync.Mutex
		targets []string
		paths   []string
	)
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		targets = append(targets, r.Header.Get(TargetActorHeader))
		paths = append(paths, r.Method+" "+r.URL.Path)
		mu.Unlock()
		_, _ = io.WriteString(w, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 7}}))
	}))
	defer router.Close()

	// The worker IP and Port point at a live runner that must never be
	// dialed in router mode.
	worker := newFakeRunner(t)
	worker.set(http.StatusOK, reportJSON(t, "direct", t0, map[string]map[string]any{"m": {"input": 1000}}))
	store := usage.NewMemoryStore()
	tk := task("t1", PhaseRunning, worker.ip)
	tk.Metadata.Atespace = "team-a"
	p := New(Options{
		Tasks:      &staticLister{tasks: []*v1alpha1.Task{tk, task("t2", "Suspended", worker.ip)}},
		Store:      store,
		Port:       worker.port,
		RouterAddr: router.Listener.Addr().String(),
		Timeout:    time.Second,
		Logger:     quietLogger(),
	})
	p.Sweep(context.Background())
	if n := worker.hits.Load(); n != 0 {
		t.Errorf("worker IP dialed %d times in router mode, want 0", n)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(targets) != 1 || targets[0] != "team-a/t1" || paths[0] != "GET /usage" {
		t.Fatalf("router requests = %v %v, want one GET /usage for team-a/t1", paths, targets)
	}
	u, err := store.Get(context.Background(), usage.Key{Atespace: "team-a", Name: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := u.Tokens["m"].Input; got != 7 {
		t.Errorf("input tokens = %d, want 7", got)
	}
	// A task without a valid worker IP is still not pulled.
	if err := p.Flush(context.Background(), task("t3", PhaseRunning, "")); err != nil {
		t.Errorf("Flush without worker IP = %v, want nil", err)
	}
	if len(targets) != 1 {
		t.Errorf("router requests after Flush = %d, want 1", len(targets))
	}
}

// liveTasks is a TaskLister + TaskGetter over a mutable task map; onList runs
// after ListRunningTasks returned its snapshot (e.g. a concurrent Suspend).
type liveTasks struct {
	mu     sync.Mutex
	tasks  map[string]*v1alpha1.Task
	getErr error
	gets   int
	onList func()
}

func newLiveTasks(ts ...*v1alpha1.Task) *liveTasks {
	l := &liveTasks{tasks: map[string]*v1alpha1.Task{}}
	for _, t := range ts {
		l.tasks[t.GetMetadata().GetName()] = t
	}
	return l
}

func (l *liveTasks) ListRunningTasks(context.Context) ([]*v1alpha1.Task, error) {
	l.mu.Lock()
	var out []*v1alpha1.Task
	for _, t := range l.tasks {
		if t.GetStatus().GetPhase() == PhaseRunning {
			out = append(out, task(t.GetMetadata().GetName(), PhaseRunning, t.GetStatus().GetWorkerIp()))
		}
	}
	onList := l.onList
	l.mu.Unlock()
	if onList != nil {
		onList()
	}
	return out, nil
}

func (l *liveTasks) LookupTask(_ context.Context, atespace, name string) (*v1alpha1.Task, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gets++
	if l.getErr != nil {
		return nil, l.getErr
	}
	t, ok := l.tasks[name]
	if !ok || t.GetMetadata().GetAtespace() != atespace {
		return nil, nil
	}
	return task(name, t.GetStatus().GetPhase(), t.GetStatus().GetWorkerIp()), nil
}

func (l *liveTasks) set(name, phase, ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tasks[name] = task(name, phase, ip)
}

func newRouter(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 7}}))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// T12c R1: a Suspend that lands between the sweep's list and the pull sends
// no router request (the router would resume the suspended actor).
func TestSweep_SuspendBetweenListAndPullSkipsRouter(t *testing.T) {
	router, hits := newRouter(t)
	tasks := newLiveTasks(task("t1", PhaseRunning, "10.0.0.9"), task("t2", PhaseRunning, "10.0.0.8"))
	tasks.onList = func() { tasks.set("t1", "Suspended", "") }
	var buf syncBuffer
	p := New(Options{
		Tasks: tasks, Store: usage.NewMemoryStore(), RouterAddr: router.Listener.Addr().String(),
		Timeout: time.Second, Logger: slog.New(slog.NewTextHandler(&buf, nil)),
	})
	p.Sweep(context.Background())
	if n := hits.Load(); n != 1 {
		t.Errorf("router requests = %d, want 1 (t2 only)", n)
	}
	if st := p.Stats(); st.Pulls != 1 || st.Skipped != 1 || st.Failures != 0 {
		t.Errorf("stats = %+v, want 1 pull, 1 skipped", st)
	}
	if got := buf.String(); !strings.Contains(got, `pulled=1 failed=0 applied=1 skipped=1`) {
		t.Errorf("sweep log = %q", got)
	}

	// A sweep whose only task is skipped still logs its summary.
	buf.Reset()
	tasks.onList = func() { tasks.set("t2", "Suspended", "") }
	p.Sweep(context.Background())
	if got := buf.String(); !strings.Contains(got, `pulled=0 failed=0 applied=0 skipped=1`) {
		t.Errorf("all-skipped sweep log = %q", got)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("router requests after second sweep = %d, want 1", n)
	}
}

// T12c R1: the router-mode re-check, for Flush (called by SuspendTask while
// the task is still Running) and the closing-state check on the usage record.
func TestPull_RouterRecheck(t *testing.T) {
	// record stores a usage record whose state events are running, then state.
	record := func(t *testing.T, s usage.Store, name, state string) {
		t.Helper()
		err := s.Update(context.Background(), usage.Key{Atespace: "default", Name: name}, func(u *usage.TaskUsage) error {
			u.ApplyStateChange(usage.StateChange{Time: t0.Add(-time.Minute), State: "running"})
			u.ApplyStateChange(usage.StateChange{Time: t0.Add(-time.Second), State: state})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name      string
		stored    *v1alpha1.Task // nil = deleted
		getErr    error
		lastState string // usage record's last state event ("" = no record)
		wantHits  int64
		wantErr   bool
	}{
		{name: "still running", stored: task("t1", PhaseRunning, "10.0.0.9"), wantHits: 1},
		{name: "running, last state running", stored: task("t1", PhaseRunning, "10.0.0.9"), lastState: "running", wantHits: 1},
		{name: "suspended", stored: task("t1", "Suspended", "")},
		{name: "deleted"},
		{name: "rescheduled", stored: task("t1", PhaseRunning, "10.0.0.7")},
		{name: "re-read fails", stored: task("t1", PhaseRunning, "10.0.0.9"), getErr: errors.New("redis down"), wantErr: true},
		{name: "actor suspending", stored: task("t1", PhaseRunning, "10.0.0.9"), lastState: "suspending"},
		{name: "actor suspended", stored: task("t1", PhaseRunning, "10.0.0.9"), lastState: "suspended"},
		{name: "actor crashed", stored: task("t1", PhaseRunning, "10.0.0.9"), lastState: "crashed"},
		{name: "actor deleted", stored: task("t1", PhaseRunning, "10.0.0.9"), lastState: "deleted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router, hits := newRouter(t)
			tasks := newLiveTasks()
			if tc.stored != nil {
				tasks.tasks["t1"] = tc.stored
			}
			tasks.getErr = tc.getErr
			store := usage.NewMemoryStore()
			if tc.lastState != "" {
				record(t, store, "t1", tc.lastState)
			}
			p := New(Options{Tasks: tasks, Store: store, RouterAddr: router.Listener.Addr().String(), Timeout: time.Second, Logger: quietLogger()})
			err := p.Flush(context.Background(), task("t1", PhaseRunning, "10.0.0.9"))
			if (err != nil) != tc.wantErr {
				t.Errorf("Flush = %v, wantErr %v", err, tc.wantErr)
			}
			if n := hits.Load(); n != tc.wantHits {
				t.Errorf("router requests = %d, want %d", n, tc.wantHits)
			}
			st := p.Stats()
			wantSkipped := int64(0)
			if tc.wantHits == 0 && !tc.wantErr {
				wantSkipped = 1
			}
			if st.Skipped != wantSkipped {
				t.Errorf("stats = %+v, want %d skipped", st, wantSkipped)
			}
			if tc.wantErr && st.Failures != 1 {
				t.Errorf("stats = %+v, want 1 failure", st)
			}
		})
	}
}

// Direct pod-IP pulls never resume an actor: no re-check (the getter is not
// called and a closing state does not block the pull).
func TestPull_DirectModeHasNoRecheck(t *testing.T) {
	f := newFakeRunner(t)
	f.set(http.StatusOK, reportJSON(t, "s1", t0, map[string]map[string]any{"m": {"input": 1}}))
	tasks := newLiveTasks()
	store := usage.NewMemoryStore()
	p := newTestPuller(f, tasks, store, nil, nil)
	if err := p.Flush(context.Background(), task("t1", PhaseRunning, f.ip)); err != nil {
		t.Fatal(err)
	}
	if f.hits.Load() != 1 || tasks.gets != 0 {
		t.Errorf("runner hits = %d, getter calls = %d, want 1, 0", f.hits.Load(), tasks.gets)
	}
}

// R3: the default transport keeps one idle connection per concurrent pull
// (router pulls all go to one host).
func TestNew_TransportIdlePoolMatchesConcurrency(t *testing.T) {
	for _, c := range []int{0, 3, 100} {
		p := New(Options{Concurrency: c})
		tr, ok := p.o.Client.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("transport = %T", p.o.Client.Transport)
		}
		want := c
		if c == 0 {
			want = DefaultConcurrency
		}
		if tr.MaxIdleConnsPerHost != want || tr.MaxIdleConns < want {
			t.Errorf("Concurrency %d: MaxIdleConnsPerHost = %d, MaxIdleConns = %d", c, tr.MaxIdleConnsPerHost, tr.MaxIdleConns)
		}
	}
}
