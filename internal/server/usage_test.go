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

package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/store/redis"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/internal/usage/metrics"
	"github.com/google/ax/internal/usage/puller"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fixturePath is the T04 golden OTLP fixture (Substrate v0.5.0 schema):
// task t1 (atespace default) and t2 (atespace team-a).
const fixturePath = "../usage/ingest/testdata/substrate_events.json"

// t1UID is the actor uid of t1 in the fixture.
const t1UID = "0b6c1f0e-3d1a-4c55-9a7e-2f1d8c4b9a01"

// testRateCard prices $0.001 per CPU-second and nothing for memory, so t1's
// fixture cost is exactly cpu × 0.001 = $0.0622.
const testRateCard = `currency: USD
compute: { default: { vcpuHour: 3.6, gibHour: 0 } }
models:
  gemini-2.5-flash: { input: 1.0, output: 2.0 }
metricLabels: [agent]
`

// Fixture results for t1 and t2 (T04 golden test).
const (
	t1CPU    = 62.2
	t1Active = 289 * time.Second
	t1Cost   = t1CPU * 0.001
	t2CPU    = 4.0
)

// fixtureCreated precedes every fixture event (events older than the task's
// creation are dropped by the ingestor).
var fixtureCreated = time.Unix(1792144000, 0)

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// syncBuffer is a goroutine-safe log sink for slog JSON handlers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the JSON log lines with msg (all lines when msg is "").
func (b *syncBuffer) lines(t *testing.T, msg string) []map[string]any {
	t.Helper()
	b.mu.Lock()
	data := slices.Clone(b.buf.Bytes())
	b.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad log line %q: %v", sc.Text(), err)
		}
		if msg == "" || m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

// summaries returns the usage summary lines with reason for task.
func (b *syncBuffer) summaries(t *testing.T, reason, task string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range b.lines(t, metrics.SummaryMessage) {
		if l["reason"] == reason && l["task"] == task {
			out = append(out, l)
		}
	}
	return out
}

type finopsEnv struct {
	srv    *server.Server
	tasks  *memory.MemoryStore
	usage  usage.Store // the raw usage store (writes here do not trigger budget checks)
	reader *sdkmetric.ManualReader
	logs   *syncBuffer
	locker lock.Locker
}

type envOptions struct {
	reconciler server.Reconciler
	usage      usage.Store
	flusher    server.UsageFlusher
}

func newFinopsEnv(t *testing.T, o envOptions) *finopsEnv {
	t.Helper()
	card, err := cost.Parse([]byte(testRateCard))
	if err != nil {
		t.Fatal(err)
	}
	pricer := cost.NewPricer(card)
	tasks := memory.NewStore()
	us := o.usage
	if us == nil {
		us = usage.NewMemoryStore()
	}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	labels := server.NewLabelCache(tasks, 0)
	rec, err := metrics.New(mp.Meter(metrics.ScopeName), func() []string { return pricer.RateCard().MetricLabels }, labels)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	locker := lock.NewMemoryLocker()
	srv := server.NewServer(tasks, server.Options{
		Locker:     locker,
		Reconciler: o.reconciler,
		Usage: &server.UsageOptions{
			Store:    us,
			Pricer:   pricer,
			RateCard: pricer.RateCard,
			Metrics:  rec,
			Labels:   labels,
			Summary:  metrics.NewSummaryLogger(logger),
			Flusher:  o.flusher,
			Logger:   logger,
		},
	})
	return &finopsEnv{srv: srv, tasks: tasks, usage: us, reader: reader, logs: logs, locker: locker}
}

type taskOpt func(*v1alpha1.Task)

func withLabels(kv ...string) taskOpt {
	return func(t *v1alpha1.Task) {
		t.Metadata.Labels = map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			t.Metadata.Labels[kv[i]] = kv[i+1]
		}
	}
}

func withBudget(b *v1alpha1.BudgetSpec) taskOpt {
	return func(t *v1alpha1.Task) { t.Spec.Budget = b }
}

func withWorker(ip string) taskOpt {
	return func(t *v1alpha1.Task) { t.Status.WorkerIp = ip }
}

func (e *finopsEnv) saveTask(t *testing.T, atespace, name, phase string, opts ...taskOpt) *v1alpha1.Task {
	t.Helper()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Atespace: atespace, Name: name, CreationTimestamp: timestamppb.New(fixtureCreated)},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		Status:   &v1alpha1.TaskStatus{Phase: phase, Actor: name},
	}
	for _, o := range opts {
		o(task)
	}
	if err := e.tasks.SaveTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	return task
}

func (e *finopsEnv) stored(t *testing.T, atespace, name string) *v1alpha1.Task {
	t.Helper()
	task, err := e.tasks.GetTask(context.Background(), atespace, name)
	if err != nil {
		t.Fatalf("stored task %s/%s: %v", atespace, name, err)
	}
	return task
}

func (e *finopsEnv) get(t *testing.T, atespace, name string) *v1alpha1.Task {
	t.Helper()
	task, err := e.srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Atespace: atespace, Name: name})
	if err != nil {
		t.Fatalf("GetTask %s/%s: %v", atespace, name, err)
	}
	return task
}

// postFixture POSTs the golden OTLP JSON fixture to the server's ingest
// handler and requires 200.
func postFixture(t *testing.T, srv *server.Server) {
	t.Helper()
	body, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.IngestHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/logs = %d: %s", rec.Code, rec.Body.String())
	}
}

func condition(task *v1alpha1.Task, typ string) *v1alpha1.Condition {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == typ {
			return c
		}
	}
	return nil
}

func requireCondition(t *testing.T, task *v1alpha1.Task, typ, status, reason string) {
	t.Helper()
	c := condition(task, typ)
	if c == nil {
		t.Fatalf("%s: no %s condition; conditions = %v", task.GetMetadata().GetName(), typ, task.GetStatus().GetConditions())
	}
	if c.GetStatus() != status || (reason != "" && c.GetReason() != reason) {
		t.Fatalf("%s: %s = %s/%s (%q), want %s/%s", task.GetMetadata().GetName(), typ, c.GetStatus(), c.GetReason(), c.GetMessage(), status, reason)
	}
}

// metricValue sums the points of metric name whose attributes include all of
// want.
func metricValue(t *testing.T, reader *sdkmetric.ManualReader, name string, want ...attribute.KeyValue) float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	match := func(set attribute.Set) bool {
		for _, kv := range want {
			if v, ok := set.Value(kv.Key); !ok || v.Emit() != kv.Value.Emit() {
				return false
			}
		}
		return true
	}
	var sum float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[float64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						sum += dp.Value
					}
				}
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					if match(dp.Attributes) {
						sum += float64(dp.Value)
					}
				}
			}
		}
	}
	return sum
}

// statusReconciler is a fake reconciler that, like the real one, keeps
// existing conditions and only moves the phase / worker IP.
type statusReconciler struct {
	mu      sync.Mutex
	calls   int
	deletes int
}

func (r *statusReconciler) Reconcile(_ context.Context, task *v1alpha1.Task, _ ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	if task.Status.Phase == "Suspended" {
		task.Status.WorkerIp = ""
	}
	return task, nil
}

func (r *statusReconciler) ReconcileDelete(context.Context, string, string) error {
	r.mu.Lock()
	r.deletes++
	r.mu.Unlock()
	return nil
}

func (r *statusReconciler) reconciles() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// recordingFlusher records Flush calls, the reconcile count at that time and
// whether the task lock was free (Flush must run without it).
type recordingFlusher struct {
	locker lock.Locker
	rec    *statusReconciler

	mu    sync.Mutex
	calls []flushCall
}

type flushCall struct {
	task          string
	phase         string
	lockFree      bool
	reconcilesNow int
}

func (f *recordingFlusher) Flush(ctx context.Context, task *v1alpha1.Task) error {
	c := flushCall{task: task.GetMetadata().GetName(), phase: task.GetStatus().GetPhase()}
	lctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if unlock, err := f.locker.Lock(lctx, "task", task.GetMetadata().GetAtespace(), task.GetMetadata().GetName()); err == nil {
		c.lockFree = true
		unlock()
	}
	if f.rec != nil {
		c.reconcilesNow = f.rec.reconciles()
	}
	f.mu.Lock()
	f.calls = append(f.calls, c)
	f.mu.Unlock()
	return nil
}

func (f *recordingFlusher) get() []flushCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// Acceptance 1 (T08): synthetic OTLP events posted to the ingest handler show
// up in GetTask/ListTasks/WatchTask (read-time merge), status.actorUid is
// filled from the usage record, and nothing is persisted into the task.
func TestFinOps_IngestThenGetTask(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	e.saveTask(t, "default", "t1", "Suspended", withLabels("agent", "triage"))
	e.saveTask(t, "team-a", "t2", "Suspended")

	postFixture(t, e.srv)

	got := e.get(t, "default", "t1")
	u := got.GetStatus().GetUsage()
	if u == nil {
		t.Fatal("GetTask: no status.usage")
	}
	if !approx(u.GetCpuSeconds(), t1CPU) {
		t.Errorf("usage.cpuSeconds = %v, want %v", u.GetCpuSeconds(), t1CPU)
	}
	if d := u.GetActiveDuration().AsDuration(); d != t1Active {
		t.Errorf("usage.activeDuration = %v, want %v", d, t1Active)
	}
	if c := u.GetCost(); c.GetTotal() <= 0 || !approx(c.GetTotal(), t1Cost) || c.GetCurrency() != "USD" {
		t.Errorf("usage.cost = %v, want total %v USD", c, t1Cost)
	}
	if got.GetStatus().GetActorUid() != t1UID {
		t.Errorf("status.actorUid = %q, want %q (from the usage record)", got.GetStatus().GetActorUid(), t1UID)
	}
	if raw := e.stored(t, "default", "t1"); raw.GetStatus().GetUsage() != nil || raw.GetStatus().GetActorUid() != "" {
		t.Errorf("usage was persisted into the task record: %v", raw.GetStatus())
	}

	list, err := e.srv.ListTasks(context.Background(), &v1alpha1.ListTasksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	cpu := map[string]float64{}
	for _, task := range list.GetTasks() {
		cpu[task.GetMetadata().GetName()] = task.GetStatus().GetUsage().GetCpuSeconds()
	}
	if !approx(cpu["t1"], t1CPU) || !approx(cpu["t2"], t2CPU) {
		t.Errorf("ListTasks cpu = %v, want t1=%v t2=%v", cpu, t1CPU, t2CPU)
	}

	// WatchTask over gRPC: INITIAL and MODIFIED both carry usage.
	client := startGRPC(t, e.srv)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.WatchTask(ctx, &v1alpha1.WatchTaskRequest{Atespace: "default", Name: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if initial.GetAction() != "INITIAL" || !approx(initial.GetTask().GetStatus().GetUsage().GetCpuSeconds(), t1CPU) {
		t.Errorf("watch INITIAL = %s usage %v", initial.GetAction(), initial.GetTask().GetStatus().GetUsage())
	}
	st := e.stored(t, "default", "t1").GetStatus()
	st.Phase = "Running"
	if err := e.tasks.UpdateTaskStatus(ctx, "default", "t1", st); err != nil {
		t.Fatal(err)
	}
	modified, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if modified.GetAction() != "MODIFIED" || modified.GetTask().GetStatus().GetPhase() != "Running" ||
		!approx(modified.GetTask().GetStatus().GetUsage().GetCpuSeconds(), t1CPU) {
		t.Errorf("watch MODIFIED = %s phase %s usage %v", modified.GetAction(), modified.GetTask().GetStatus().GetPhase(),
			modified.GetTask().GetStatus().GetUsage())
	}
}

func startGRPC(t *testing.T, srv *server.Server) v1alpha1.AXClient {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: srv.Handler()}
	hs.Protocols = new(http.Protocols)
	hs.Protocols.SetHTTP1(true)
	hs.Protocols.SetUnencryptedHTTP2(true)
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return v1alpha1.NewAXClient(conn)
}

// countingUsageStore counts reads of the wrapped usage store.
type countingUsageStore struct {
	usage.Store
	gets, lists atomic.Int64
	mu          sync.Mutex
	listArgs    []string
}

func (c *countingUsageStore) Get(ctx context.Context, k usage.Key) (*usage.TaskUsage, error) {
	c.gets.Add(1)
	return c.Store.Get(ctx, k)
}

func (c *countingUsageStore) List(ctx context.Context, atespace string) (map[usage.Key]*usage.TaskUsage, error) {
	c.lists.Add(1)
	c.mu.Lock()
	c.listArgs = append(c.listArgs, atespace)
	c.mu.Unlock()
	return c.Store.List(ctx, atespace)
}

// Acceptance 2 (T08): ListTasks merges usage for many tasks with one usage
// List call and no per-task Get.
func TestFinOps_ListTasksUsesOneUsageList(t *testing.T) {
	cs := &countingUsageStore{Store: usage.NewMemoryStore()}
	e := newFinopsEnv(t, envOptions{usage: cs})
	ctx := context.Background()
	const n = 30
	for i := range n {
		atespace := "a"
		if i%2 == 1 {
			atespace = "b"
		}
		name := fmt.Sprintf("task-%02d", i)
		e.saveTask(t, atespace, name, "Suspended", withLabels("agent", "x"+strconv.Itoa(i%3)))
		cpu := float64(i + 1)
		if err := cs.Update(ctx, usage.Key{Atespace: atespace, Name: name}, func(u *usage.TaskUsage) error {
			u.CPUSeconds = cpu
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		name     string
		req      *v1alpha1.ListTasksRequest
		wantN    int
		wantList string
	}{
		{"all atespaces", &v1alpha1.ListTasksRequest{Limit: 100}, n, ""},
		{"one atespace", &v1alpha1.ListTasksRequest{Atespace: "b", Limit: 100}, n / 2, "b"},
		{"label selector", &v1alpha1.ListTasksRequest{LabelSelector: "agent=x1", Limit: 100}, n / 3, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cs.gets.Store(0)
			cs.lists.Store(0)
			cs.mu.Lock()
			cs.listArgs = nil
			cs.mu.Unlock()
			resp, err := e.srv.ListTasks(ctx, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.GetTasks()) != tc.wantN {
				t.Fatalf("got %d tasks, want %d", len(resp.GetTasks()), tc.wantN)
			}
			for _, task := range resp.GetTasks() {
				var i int
				fmt.Sscanf(task.GetMetadata().GetName(), "task-%d", &i)
				if got := task.GetStatus().GetUsage().GetCpuSeconds(); got != float64(i+1) {
					t.Errorf("%s: cpu = %v, want %v", task.GetMetadata().GetName(), got, i+1)
				}
			}
			if l, g := cs.lists.Load(), cs.gets.Load(); l != 1 || g != 0 {
				t.Errorf("usage store calls: List=%d Get=%d, want List=1 Get=0", l, g)
			}
			cs.mu.Lock()
			args := slices.Clone(cs.listArgs)
			cs.mu.Unlock()
			if len(args) != 1 || args[0] != tc.wantList {
				t.Errorf("List atespace args = %q, want [%q]", args, tc.wantList)
			}
		})
	}
}

// Acceptance 3a (T08): SuspendTask flushes the runner's usage first, while the
// task is still Running, before the reconciler suspends it, and without
// holding the task lock; then it logs a suspend summary.
func TestFinOps_SuspendFlushesFirst(t *testing.T) {
	rec := &statusReconciler{}
	fl := &recordingFlusher{rec: rec}
	e := newFinopsEnv(t, envOptions{reconciler: rec, flusher: fl})
	fl.locker = e.locker
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"))

	resp, err := e.srv.SuspendTask(context.Background(), &v1alpha1.SuspendTaskRequest{Name: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus().GetPhase() != "Suspended" {
		t.Fatalf("phase = %s", resp.GetStatus().GetPhase())
	}
	calls := fl.get()
	if len(calls) != 1 {
		t.Fatalf("Flush calls = %v, want 1", calls)
	}
	if c := calls[0]; c.task != "t1" || c.phase != "Running" || !c.lockFree || c.reconcilesNow != 0 {
		t.Errorf("Flush call = %+v, want task t1 in phase Running, lock free, before any reconcile", c)
	}
	if n := len(e.logs.summaries(t, metrics.ReasonSuspend, "t1")); n != 1 {
		t.Errorf("suspend summary lines = %d, want 1", n)
	}
}

// Suspend with the real puller against a fake runner: the flushed tokens are
// recorded and priced, the budget transition triggered by the flush's hook
// (which takes the task lock) is persisted without deadlocking the suspend,
// and the suspend summary carries the tokens.
func TestFinOps_SuspendFlushWithRealPuller(t *testing.T) {
	var hits atomic.Int64
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != puller.UsagePath {
			http.NotFound(w, r)
			return
		}
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"session":"s1","reportTime":%q,"tokens":{"gemini-2.5-flash":{"input":1000,"output":500}}}`,
			time.Now().UTC().Format(time.RFC3339Nano))
	}))
	defer runner.Close()
	addr := netip.MustParseAddrPort(runner.Listener.Addr().String())

	rec := &statusReconciler{}
	e := newFinopsEnv(t, envOptions{reconciler: rec})
	e.srv.NewUsagePuller(puller.Options{Port: int(addr.Port())})
	e.saveTask(t, "default", "t1", "Running", withWorker(addr.Addr().String()), withBudget(&v1alpha1.BudgetSpec{MaxTokens: 1000}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := e.srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "t1"})
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 1 {
		t.Errorf("runner hits = %d, want 1", hits.Load())
	}
	tok := resp.GetStatus().GetUsage().GetTokens()
	if len(tok) != 1 || tok[0].GetModel() != "gemini-2.5-flash" || tok[0].GetInput() != 1000 || tok[0].GetOutput() != 500 {
		t.Errorf("suspend response tokens = %v", tok)
	}
	if c := resp.GetStatus().GetUsage().GetCost().GetTokens(); !approx(c, 0.002) {
		t.Errorf("token cost = %v, want 0.002", c)
	}
	stored := e.stored(t, "default", "t1")
	requireCondition(t, stored, v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxTokensExceeded")
	if stored.GetStatus().GetPhase() != "Suspended" {
		t.Errorf("stored phase = %s", stored.GetStatus().GetPhase())
	}
	if v := metricValue(t, e.reader, metrics.MetricThresholdCrossed, attribute.String(metrics.AttrBudgetDimension, budget.DimensionMaxTokens),
		attribute.String(metrics.AttrBudgetLevel, string(budget.LevelExceeded))); v != 1 {
		t.Errorf("threshold crossings = %v, want 1", v)
	}
	sums := e.logs.summaries(t, metrics.ReasonSuspend, "t1")
	if len(sums) != 1 || sums[0]["tokens_total"] != float64(1500) {
		t.Errorf("suspend summaries = %v, want one with tokens_total 1500", sums)
	}
}

// orderingUsageStore checks, when the usage record is deleted, that the task
// record is already gone and the delete summary was already written.
type orderingUsageStore struct {
	usage.Store
	tasks store.Store
	logs  *syncBuffer
	t     *testing.T

	deletes            atomic.Int64
	taskGoneAtDelete   atomic.Bool
	summaryBeforeDelte atomic.Bool
}

func (o *orderingUsageStore) Delete(ctx context.Context, k usage.Key) error {
	o.deletes.Add(1)
	_, err := o.tasks.GetTask(ctx, k.Atespace, k.Name)
	o.taskGoneAtDelete.Store(errors.Is(err, store.ErrNotFound))
	o.summaryBeforeDelte.Store(len(o.logs.summaries(o.t, metrics.ReasonDelete, k.Name)) == 1)
	return o.Store.Delete(ctx, k)
}

// Acceptance 3b (T08): DeleteTask flushes, writes the final summary, deletes
// the task record and only then the usage record; late events for the
// deleted task do not recreate it.
func TestFinOps_DeleteRemovesUsageAfterTask(t *testing.T) {
	rec := &statusReconciler{}
	fl := &recordingFlusher{rec: rec}
	os := &orderingUsageStore{Store: usage.NewMemoryStore(), t: t}
	e := newFinopsEnv(t, envOptions{reconciler: rec, flusher: fl, usage: os})
	fl.locker = e.locker
	os.tasks, os.logs = e.tasks, e.logs
	ctx := context.Background()

	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"), withLabels("agent", "triage"))
	key := usage.Key{Atespace: "default", Name: "t1"}
	if err := e.usage.Update(ctx, key, func(u *usage.TaskUsage) error { u.CPUSeconds = 5; return nil }); err != nil {
		t.Fatal(err)
	}

	if _, err := e.srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "t1"}); err != nil {
		t.Fatal(err)
	}
	if calls := fl.get(); len(calls) != 1 || calls[0].phase != "Running" || !calls[0].lockFree {
		t.Errorf("Flush calls = %+v, want one while Running without the lock", calls)
	}
	if os.deletes.Load() != 1 || !os.taskGoneAtDelete.Load() || !os.summaryBeforeDelte.Load() {
		t.Errorf("usage delete: calls=%d taskGone=%v summaryBefore=%v, want 1/true/true",
			os.deletes.Load(), os.taskGoneAtDelete.Load(), os.summaryBeforeDelte.Load())
	}
	if _, err := e.usage.Get(ctx, key); !errors.Is(err, usage.ErrNotFound) {
		t.Errorf("usage after delete: err = %v, want ErrNotFound", err)
	}
	sums := e.logs.summaries(t, metrics.ReasonDelete, "t1")
	if len(sums) != 1 || sums[0]["cpu_seconds"] != float64(5) {
		t.Errorf("delete summaries = %v, want one with cpu_seconds 5", sums)
	}

	// Late Substrate events for the deleted task are dropped.
	postFixture(t, e.srv)
	if _, err := e.usage.Get(ctx, key); !errors.Is(err, usage.ErrNotFound) {
		t.Errorf("late events recreated the usage record: err = %v", err)
	}
}

// T09 acceptance 3: synthetic events push a task over maxCost; GetTask shows
// BudgetExceeded=True, the condition is persisted by the write path, and the
// crossing is reported exactly once (metric, slog line, summary), also after
// a replay of the same events.
func TestFinOps_BudgetExceededFromIngest(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	e.saveTask(t, "default", "t1", "Suspended", withLabels("agent", "triage"),
		withBudget(&v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Currency: "USD", Amount: 0.01}}))

	postFixture(t, e.srv)

	got := e.get(t, "default", "t1")
	requireCondition(t, got, v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxCostExceeded")
	requireCondition(t, e.stored(t, "default", "t1"), v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxCostExceeded")

	check := func() {
		t.Helper()
		exceeded := metricValue(t, e.reader, metrics.MetricThresholdCrossed,
			attribute.String(metrics.AttrAtespace, "default"), attribute.String("ax.label.agent", "triage"),
			attribute.String(metrics.AttrBudgetDimension, budget.DimensionMaxCost), attribute.String(metrics.AttrBudgetLevel, string(budget.LevelExceeded)))
		all := metricValue(t, e.reader, metrics.MetricThresholdCrossed)
		if exceeded != 1 || all != 1 {
			t.Errorf("threshold crossings: maxCost/exceeded=%v total=%v, want 1/1", exceeded, all)
		}
		if n := len(e.logs.lines(t, "budget threshold crossed")); n != 1 {
			t.Errorf("crossing log lines = %d, want 1", n)
		}
		if n := len(e.logs.summaries(t, metrics.ReasonThreshold, "t1")); n != 1 {
			t.Errorf("threshold summaries = %d, want 1", n)
		}
	}
	check()
	postFixture(t, e.srv) // replay: nothing changes
	check()
}

// T10 part B: ingested usage is recorded as OTel metrics with the task's
// configured labels (ManualReader).
func TestFinOps_MetricsRecordedAfterIngest(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	e.saveTask(t, "default", "t1", "Suspended", withLabels("agent", "triage", "pod", "ignored"))
	e.saveTask(t, "team-a", "t2", "Suspended")

	postFixture(t, e.srv)

	t1 := []attribute.KeyValue{attribute.String(metrics.AttrAtespace, "default"), attribute.String("ax.label.agent", "triage")}
	t2 := []attribute.KeyValue{attribute.String(metrics.AttrAtespace, "team-a"), attribute.String("ax.label.agent", metrics.NoneValue)}
	if v := metricValue(t, e.reader, metrics.MetricCPUTime, t1...); !approx(v, t1CPU) {
		t.Errorf("t1 %s = %v, want %v", metrics.MetricCPUTime, v, t1CPU)
	}
	if v := metricValue(t, e.reader, metrics.MetricCPUTime, t2...); !approx(v, t2CPU) {
		t.Errorf("t2 %s = %v, want %v", metrics.MetricCPUTime, v, t2CPU)
	}
	if v := metricValue(t, e.reader, metrics.MetricActiveTime, t1...); !approx(v, t1Active.Seconds()) {
		t.Errorf("t1 %s = %v, want %v", metrics.MetricActiveTime, v, t1Active.Seconds())
	}
	if v := metricValue(t, e.reader, metrics.MetricCost, append(t1, attribute.String(metrics.AttrCostKind, metrics.CostKindCompute))...); !approx(v, t1Cost) {
		t.Errorf("t1 compute cost = %v, want %v", v, t1Cost)
	}
	if v := metricValue(t, e.reader, metrics.MetricCPUTime, attribute.String("ax.label.pod", "ignored")); v != 0 {
		t.Errorf("unconfigured label key appeared on metrics")
	}
}

// R2: a usage write with a zero delta (runner reports commandExited) is
// evaluated and persisted (Idle=True) although usage hooks never see it, and
// phase changes (suspend/resume) re-evaluate Idle.
func TestFinOps_ZeroDeltaAndPhaseChangesEvaluate(t *testing.T) {
	rec := &statusReconciler{}
	e := newFinopsEnv(t, envOptions{reconciler: rec})
	ctx := context.Background()
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"))
	key := usage.Key{Atespace: "default", Name: "t1"}

	now := time.Now()
	if err := e.srv.UsageStore().Update(ctx, key, func(u *usage.TaskUsage) error {
		if d := u.ApplyRunnerReport(usage.RunnerReport{Session: "s1", ReportTime: now, CommandExited: true}, now); !d.IsZero() {
			t.Errorf("runner report delta = %+v, want zero", d)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	requireCondition(t, e.stored(t, "default", "t1"), v1alpha1.ConditionIdle, budget.StatusTrue, budget.ReasonCommandExited)

	if _, err := e.srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "t1"}); err != nil {
		t.Fatal(err)
	}
	requireCondition(t, e.stored(t, "default", "t1"), v1alpha1.ConditionIdle, budget.StatusFalse, budget.ReasonNotRunning)

	if _, err := e.srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "t1"}); err != nil {
		t.Fatal(err)
	}
	requireCondition(t, e.stored(t, "default", "t1"), v1alpha1.ConditionIdle, budget.StatusTrue, budget.ReasonCommandExited)
}

// R1: a hook carrying a stale snapshot (lower cost than stored) does not
// regress a persisted BudgetExceeded nor re-emit the crossing: the decision
// is made on a fresh read under the task lock.
func TestFinOps_StaleHookSnapshotDoesNotRegress(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	ctx := context.Background()
	e.saveTask(t, "default", "t1", "Running", withBudget(&v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Currency: "USD", Amount: 0.01}}))
	key := usage.Key{Atespace: "default", Name: "t1"}
	if err := e.usage.Update(ctx, key, func(u *usage.TaskUsage) error {
		u.Cost = usage.Cost{Currency: "USD", Total: 0.02, Compute: 0.02}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	fresh, err := e.usage.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	hooks := e.srv.UsageHooks()
	hooks.OnDelta(ctx, key, usage.Delta{}, usage.Cost{}, fresh)
	requireCondition(t, e.stored(t, "default", "t1"), v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxCostExceeded")

	stale := usage.NewTaskUsage()
	stale.Cost = usage.Cost{Currency: "USD", Total: 0.009, Compute: 0.009} // 90%: Warning in the snapshot
	hooks.OnDelta(ctx, key, usage.Delta{}, usage.Cost{}, stale)

	stored := e.stored(t, "default", "t1")
	requireCondition(t, stored, v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxCostExceeded")
	if c := condition(stored, v1alpha1.ConditionBudgetWarning); c != nil && c.GetStatus() == budget.StatusTrue {
		t.Errorf("stale snapshot persisted BudgetWarning=True: %v", c)
	}
	if v := metricValue(t, e.reader, metrics.MetricThresholdCrossed); v != 1 {
		t.Errorf("threshold crossings = %v, want 1", v)
	}
}

// The periodic sweep logs a summary per Running task and re-evaluates
// budgets (safety net for skipped checks).
func TestFinOps_UsageSweep(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	ctx := context.Background()
	e.saveTask(t, "default", "run", "Running", withBudget(&v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Amount: 0.01}}))
	e.saveTask(t, "default", "sus", "Suspended")
	if err := e.usage.Update(ctx, usage.Key{Atespace: "default", Name: "run"}, func(u *usage.TaskUsage) error {
		u.CPUSeconds = 7
		u.Cost = usage.Cost{Currency: "USD", Total: 0.5, Compute: 0.5}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.UsageSweep(ctx); err != nil {
		t.Fatal(err)
	}
	if s := e.logs.summaries(t, metrics.ReasonPeriodic, "run"); len(s) != 1 || s[0]["cpu_seconds"] != float64(7) {
		t.Errorf("periodic summaries for run = %v", s)
	}
	if s := e.logs.summaries(t, metrics.ReasonPeriodic, "sus"); len(s) != 0 {
		t.Errorf("suspended task summarized periodically: %v", s)
	}
	requireCondition(t, e.stored(t, "default", "run"), v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxCostExceeded")
}

// ListRunningTasks (puller.TaskLister) pages through all atespaces of both
// task stores.
func TestListRunningTasks(t *testing.T) {
	defer server.SetRunningPageSize(2)()
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	for name, st := range map[string]store.Store{
		"memory": memory.NewStore(),
		"redis":  redis.NewStore(rc, redis.Options{}),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			want := []string{}
			i := 0
			for _, atespace := range []string{"default", "team-a", "team-b"} {
				for _, phase := range []string{"Running", "Suspended", "Running", "Pending"} {
					name := fmt.Sprintf("t%d", i)
					i++
					task := &v1alpha1.Task{
						Metadata: &v1alpha1.ObjectMeta{Atespace: atespace, Name: name, CreationTimestamp: timestamppb.New(fixtureCreated.Add(time.Duration(i) * time.Second))},
						Spec:     &v1alpha1.TaskSpec{},
						Status:   &v1alpha1.TaskStatus{Phase: phase},
					}
					if err := st.SaveTask(ctx, task); err != nil {
						t.Fatal(err)
					}
					if phase == "Running" {
						want = append(want, atespace+"/"+name)
					}
				}
			}
			srv := server.NewServer(st, server.Options{Usage: &server.UsageOptions{Store: usage.NewMemoryStore()}})
			tasks, err := srv.ListRunningTasks(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, task := range tasks {
				got = append(got, task.GetMetadata().GetAtespace()+"/"+task.GetMetadata().GetName())
			}
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("running tasks = %v, want %v", got, want)
			}
		})
	}
}

type countingGetter struct {
	tasks map[string]*v1alpha1.Task
	calls atomic.Int64
}

func (c *countingGetter) GetTask(_ context.Context, atespace, name string) (*v1alpha1.Task, error) {
	c.calls.Add(1)
	if t, ok := c.tasks[atespace+"/"+name]; ok {
		return t, nil
	}
	return nil, store.ErrNotFound
}

func TestLabelCache(t *testing.T) {
	g := &countingGetter{tasks: map[string]*v1alpha1.Task{
		"default/t1": {Metadata: &v1alpha1.ObjectMeta{Labels: map[string]string{"agent": "a"}}},
	}}
	now := time.Unix(0, 0)
	c := server.NewLabelCache(g, time.Minute)
	c.SetClock(func() time.Time { return now })
	ctx := context.Background()
	k := usage.Key{Name: "t1"} // empty atespace = default
	for range 3 {
		if l := c.Labels(ctx, k); l["agent"] != "a" {
			t.Fatalf("labels = %v", l)
		}
	}
	if n := g.calls.Load(); n != 1 {
		t.Errorf("lookups = %d, want 1 (cached)", n)
	}
	c.Invalidate(usage.Key{Atespace: "default", Name: "t1"})
	c.Labels(ctx, k)
	if n := g.calls.Load(); n != 2 {
		t.Errorf("lookups after Invalidate = %d, want 2", n)
	}
	now = now.Add(2 * time.Minute)
	c.Labels(ctx, k)
	if n := g.calls.Load(); n != 3 {
		t.Errorf("lookups after TTL = %d, want 3", n)
	}
	missing := usage.Key{Atespace: "default", Name: "nope"}
	if l := c.Labels(ctx, missing); l != nil {
		t.Errorf("missing task labels = %v, want nil", l)
	}
	c.Labels(ctx, missing)
	if n := g.calls.Load(); n != 5 {
		t.Errorf("missing tasks must not be cached: lookups = %d, want 5", n)
	}
}

// Without UsageOptions the server behaves as before: no usage in responses
// and no OTLP endpoint.
func TestFinOps_Disabled(t *testing.T) {
	st := memory.NewStore()
	srv := server.NewServer(st)
	if srv.UsageEnabled() || srv.UsageStore() != nil || srv.UsageHooks() != nil || srv.NewUsagePuller(puller.Options{}) != nil {
		t.Fatal("usage should be disabled")
	}
	if err := st.SaveTask(context.Background(), &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "t1"}, Status: &v1alpha1.TaskStatus{Phase: "Running"}}); err != nil {
		t.Fatal(err)
	}
	got, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "t1"})
	if err != nil || got.GetStatus().GetUsage() != nil {
		t.Fatalf("GetTask = %v, %v", got, err)
	}
	rec := httptest.NewRecorder()
	srv.IngestHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/logs", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("POST /v1/logs without usage = %d, want 404", rec.Code)
	}
}

// Concurrent ingest, hooks, suspend/resume and sweeps on one task finish
// (no lock-order deadlock) and leave consistent persisted conditions.
func TestFinOps_ConcurrentWritersNoDeadlock(t *testing.T) {
	rec := &statusReconciler{}
	fl := &recordingFlusher{rec: rec}
	e := newFinopsEnv(t, envOptions{reconciler: rec, flusher: fl})
	fl.locker = e.locker
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"),
		withBudget(&v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Currency: "USD", Amount: 0.01}}))
	e.saveTask(t, "team-a", "t2", "Running")
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				for range 5 {
					postFixture(t, e.srv)
				}
			})
		}
		wg.Go(func() {
			for range 5 {
				if _, err := e.srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "t1"}); err != nil {
					t.Error(err)
				}
				if _, err := e.srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "t1"}); err != nil {
					t.Error(err)
				}
			}
		})
		wg.Go(func() {
			for range 5 {
				if err := e.srv.UsageSweep(ctx); err != nil {
					t.Error(err)
				}
				e.get(t, "default", "t1")
			}
		})
		wg.Wait()
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent FinOps operations did not finish (deadlock?)")
	}
	requireCondition(t, e.stored(t, "default", "t1"), v1alpha1.ConditionBudgetExceeded, budget.StatusTrue, "MaxCostExceeded")
	if v := metricValue(t, e.reader, metrics.MetricThresholdCrossed); v != 1 {
		t.Errorf("threshold crossings = %v, want 1", v)
	}
}

// N1: creating a task whose name has a leftover usage record (e.g. late
// events that raced the previous task's delete) starts from zero usage; an
// existing task's usage is never dropped by a create call.
func TestFinOps_CreateTaskDropsOrphanUsage(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	ctx := context.Background()
	key := usage.Key{Atespace: "default", Name: "t1"}
	if err := e.usage.Update(ctx, key, func(u *usage.TaskUsage) error {
		u.CPUSeconds = 99
		u.Cost = usage.Cost{Currency: "USD", Total: 0.5, Compute: 0.5}
		u.State.ActorUID = "old-actor"
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := e.srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t1"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine", Budget: &v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Currency: "USD", Amount: 0.01}}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.usage.Get(ctx, key); !errors.Is(err, usage.ErrNotFound) {
		t.Errorf("orphan usage survived CreateTask: err = %v", err)
	}
	got := e.get(t, "default", "t1")
	if got.GetStatus().GetUsage() != nil || got.GetStatus().GetActorUid() != "" {
		t.Errorf("new task shows stale usage: %v", got.GetStatus())
	}
	if c := condition(got, v1alpha1.ConditionBudgetExceeded); c != nil {
		t.Errorf("new task shows stale budget condition: %v", c)
	}

	// The task now exists: a (rejected) create must not touch its usage.
	if err := e.usage.Update(ctx, key, func(u *usage.TaskUsage) error { u.CPUSeconds = 3; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := e.srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t1"},
		Spec:     &v1alpha1.TaskSpec{Image: "other"},
	}}); err == nil {
		t.Fatal("create of an existing task with another spec succeeded")
	}
	if u, err := e.usage.Get(ctx, key); err != nil || u.CPUSeconds != 3 {
		t.Errorf("existing task usage = %v, %v; want cpu 3", u, err)
	}
}

// N5: client-supplied BudgetWarning/BudgetExceeded/Idle conditions are not
// stored on a new task (they are server-computed); other conditions stay.
func TestFinOps_CreateTaskClearsClientBudgetConditions(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	ctx := context.Background()
	if _, err := e.srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t1"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
		Status: &v1alpha1.TaskStatus{
			ActorUid: "forged",
			Conditions: []*v1alpha1.Condition{
				{Type: v1alpha1.ConditionBudgetExceeded, Status: "True", Reason: "MaxCostExceeded"},
				{Type: v1alpha1.ConditionBudgetWarning, Status: "True", Reason: "MaxCostWarning"},
				{Type: v1alpha1.ConditionIdle, Status: "True", Reason: "LowCPU"},
				{Type: "Custom", Status: "True", Reason: "Kept"},
			},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	stored := e.stored(t, "default", "t1")
	for _, typ := range []string{v1alpha1.ConditionBudgetExceeded, v1alpha1.ConditionBudgetWarning, v1alpha1.ConditionIdle} {
		if c := condition(stored, typ); c != nil {
			t.Errorf("client-supplied %s condition stored: %v", typ, c)
		}
	}
	if condition(stored, "Custom") == nil {
		t.Error("unrelated condition was dropped")
	}
	if stored.GetStatus().GetActorUid() != "" {
		t.Errorf("client-supplied actorUid stored: %q", stored.GetStatus().GetActorUid())
	}
}

// countingTaskStore counts GetTask calls.
type countingTaskStore struct {
	*memory.MemoryStore
	gets atomic.Int64
}

func (c *countingTaskStore) GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	c.gets.Add(1)
	return c.MemoryStore.GetTask(ctx, atespace, name)
}

// N2: the hook following a write through UsageStore() skips its budget
// pre-check (the commit observer already ran it); a hook for another
// snapshot still checks.
func TestFinOps_HookSkipsCheckAlreadyDoneByObserver(t *testing.T) {
	ts := &countingTaskStore{MemoryStore: memory.NewStore()}
	srv := server.NewServer(ts, server.Options{Usage: &server.UsageOptions{Store: usage.NewMemoryStore(), Labels: server.NewLabelCache(ts, 0)}})
	ctx := context.Background()
	if err := ts.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "t1"}, Spec: &v1alpha1.TaskSpec{}, Status: &v1alpha1.TaskStatus{Phase: "Running"}}); err != nil {
		t.Fatal(err)
	}
	key := usage.Key{Atespace: "default", Name: "t1"}
	var snap *usage.TaskUsage
	if err := srv.UsageStore().Update(ctx, key, func(u *usage.TaskUsage) error {
		u.CPUSeconds++
		snap = u
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	afterObserver := ts.gets.Load()
	if afterObserver == 0 {
		t.Fatal("commit observer did not check the budget")
	}
	srv.UsageHooks().OnDelta(ctx, key, usage.Delta{}, usage.Cost{}, snap)
	if n := ts.gets.Load(); n != afterObserver {
		t.Errorf("hook re-checked an observed write: GetTask %d -> %d", afterObserver, n)
	}
	srv.UsageHooks().OnDelta(ctx, key, usage.Delta{}, usage.Cost{}, usage.NewTaskUsage())
	if n := ts.gets.Load(); n == afterObserver {
		t.Error("hook for an unobserved snapshot skipped its check")
	}
}
