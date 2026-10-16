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

package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fakeTasks is a TaskLookup over a fixed set of tasks.
type fakeTasks struct {
	tasks map[[2]string]*v1alpha1.Task
	err   error
}

func newFakeTasks(tasks ...*v1alpha1.Task) *fakeTasks {
	f := &fakeTasks{tasks: map[[2]string]*v1alpha1.Task{}}
	for _, t := range tasks {
		f.tasks[[2]string{t.GetMetadata().GetAtespace(), t.GetMetadata().GetName()}] = t
	}
	return f
}

func (f *fakeTasks) GetTask(_ context.Context, atespace, name string) (*v1alpha1.Task, error) {
	if f.err != nil {
		return nil, f.err
	}
	t, ok := f.tasks[[2]string{atespace, name}]
	if !ok {
		return nil, store.ErrNotFound
	}
	return t, nil
}

func task(atespace, name, actorUID string) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Atespace: atespace, Name: name},
		Status:   &v1alpha1.TaskStatus{ActorUid: actorUID},
	}
}

// linearPricer charges 0.001/cpu-second and 0.0001/GiB-second.
type linearPricer struct{}

func (linearPricer) Price(d usage.Delta, _ string) usage.Cost {
	s := d.Sum()
	compute := s.CPUSeconds*0.001 + s.MemoryGiBSeconds*0.0001
	idle := s.IdleCPUSeconds*0.001 + s.IdleMemoryGiBSeconds*0.0001
	return usage.Cost{Currency: "USD", Total: compute, Compute: compute, Idle: idle}
}

type hookCall struct {
	key  usage.Key
	d    usage.Delta
	c    usage.Cost
	cpu  float64
	cost float64
}

type recordingHooks struct {
	mu    sync.Mutex
	calls []hookCall
}

func (h *recordingHooks) OnDelta(_ context.Context, key usage.Key, d usage.Delta, c usage.Cost, u *usage.TaskUsage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, hookCall{key: key, d: d, c: c, cpu: u.CPUSeconds, cost: u.Cost.Total})
}

func (h *recordingHooks) reset() []hookCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.calls
	h.calls = nil
	return c
}

const (
	uidT1 = "0b6c1f0e-3d1a-4c55-9a7e-2f1d8c4b9a01"
	uidT2 = "7d2e9a44-5b0c-4f1e-8e3a-6c9b1d0f2e77"
)

func fixtureJSON(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/substrate_events.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fixtureProto(t *testing.T) *collogspb.ExportLogsServiceRequest {
	t.Helper()
	req := &collogspb.ExportLogsServiceRequest{}
	if err := protojson.Unmarshal(fixtureJSON(t), req); err != nil {
		t.Fatalf("fixture is not valid OTLP JSON: %v", err)
	}
	return req
}

func post(t *testing.T, h http.Handler, contentType, encoding string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	if encoding != "" {
		r.Header.Set("Content-Encoding", encoding)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func approx(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

type stores map[string]func(t *testing.T) usage.Store

var allStores = stores{
	"memory": func(*testing.T) usage.Store { return usage.NewMemoryStore() },
	"redis": func(t *testing.T) usage.Store {
		mr := miniredis.RunT(t)
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { c.Close() })
		return usage.NewRedisStore(c, "")
	},
}

type wantUsage struct {
	key                          usage.Key
	active, cpu, mem, idle, cost float64
	uid, pool, state             string
}

// Expected totals for testdata/substrate_events.json (see the comments in
// the test for the derivation).
var fixtureWant = []wantUsage{
	{
		// t1: epoch E1 = 09:59:58 (2s before RUNNING commits) → final sample
		// 10:03:05 (187s) and epoch E2 = 10:09:58 → crashed 10:11:40 (102s,
		// body-only records). Active time is epoch-based: the activation
		// starts at ate.actor.epoch (real data: ~0.3s before the running
		// event) and ends at the final sample (written at teardown; real
		// data: 1.4ms after suspending), or for a crash (no final sample) at
		// the crash event; so 289 rather than the 290 the RUNNING/SUSPENDING
		// events alone give (this synthetic final precedes SUSPENDING by 5s).
		// CPU = max(epoch1)=61.0 + max(epoch2)=1.2.
		// Memory = 1 GiB × (2s after the pending sample + 3×60s) + 0.5 GiB × 60s.
		// The uid-mismatch sample (cpu 999) is ignored.
		key: usage.Key{Atespace: "default", Name: "t1"}, active: 289, cpu: 62.2, mem: 212, idle: 0,
		cost: 62.2*0.001 + 212*0.0001, uid: uidT1, pool: "ax-pool", state: "crashed",
	},
	{
		// t2: RUNNING at 10:00:00, samples at +10s (cpu 1) and +70s (cpu 4),
		// 2 GiB working set; the open interval is accrued to the last sample.
		key: usage.Key{Atespace: "team-a", Name: "t2"}, active: 70, cpu: 4, mem: 120, idle: 0,
		cost: 4*0.001 + 120*0.0001, uid: uidT2, pool: "big-pool", state: "running",
	},
}

func checkUsage(t *testing.T, s usage.Store, want []wantUsage) {
	t.Helper()
	for _, w := range want {
		u, err := s.Get(context.Background(), w.key)
		if err != nil {
			t.Fatalf("Get(%v): %v", w.key, err)
		}
		if !approx(u.ActiveSeconds, w.active) || !approx(u.CPUSeconds, w.cpu) ||
			!approx(u.MemoryGiBSeconds, w.mem) || !approx(u.IdleSeconds, w.idle) {
			t.Errorf("%v: active=%v cpu=%v mem=%v idle=%v, want %v %v %v %v", w.key,
				u.ActiveSeconds, u.CPUSeconds, u.MemoryGiBSeconds, u.IdleSeconds, w.active, w.cpu, w.mem, w.idle)
		}
		if !approx(u.Cost.Total, w.cost) || u.Cost.Currency != "USD" {
			t.Errorf("%v: cost=%+v, want total %v USD", w.key, u.Cost, w.cost)
		}
		if u.State.ActorUID != w.uid || u.State.Pool != w.pool || u.State.State != w.state {
			t.Errorf("%v: uid=%q pool=%q state=%q, want %q %q %q", w.key,
				u.State.ActorUID, u.State.Pool, u.State.State, w.uid, w.pool, w.state)
		}
		day := u.Days["2026-10-16"]
		if day == nil || !approx(day.CPUSeconds, w.cpu) || !approx(day.Cost.Total, w.cost) {
			t.Errorf("%v: day bucket = %+v, want cpu %v cost %v", w.key, day, w.cpu, w.cost)
		}
	}
}

func newTestHandler(s usage.Store, tasks TaskLookup, hooks usage.Hooks) *Handler {
	return NewHandler(Options{Store: s, Tasks: tasks, Pricer: linearPricer{}, Hooks: hooks})
}

// Acceptance 1 (+4): the golden fixture yields the expected TaskUsage totals;
// replaying it changes nothing and calls no hooks.
func TestGoldenFixture(t *testing.T) {
	for name, mk := range allStores {
		t.Run(name, func(t *testing.T) {
			s := mk(t)
			hooks := &recordingHooks{}
			h := newTestHandler(s, newFakeTasks(task("default", "t1", ""), task("team-a", "t2", "")), hooks)

			w := post(t, h, "application/json", "", fixtureJSON(t))
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}
			if got := strings.TrimSpace(w.Body.String()); got != "{}" {
				t.Errorf("body = %q, want {}", got)
			}
			checkUsage(t, s, fixtureWant)

			if _, err := s.Get(context.Background(), usage.Key{Atespace: "default", Name: "not-a-task"}); !errors.Is(err, usage.ErrNotFound) {
				t.Errorf("not-a-task usage: err = %v, want ErrNotFound", err)
			}

			calls := hooks.reset()
			if len(calls) != 2 {
				t.Fatalf("hook calls = %d, want 2 (one per task)", len(calls))
			}
			for _, c := range calls {
				if c.d.IsZero() {
					t.Errorf("hook called with empty delta for %v", c.key)
				}
				var want wantUsage
				for _, fw := range fixtureWant {
					if fw.key == c.key {
						want = fw
					}
				}
				if sum := c.d.Sum(); !approx(sum.CPUSeconds, want.cpu) || !approx(c.c.Total, want.cost) ||
					!approx(c.cpu, want.cpu) || !approx(c.cost, want.cost) {
					t.Errorf("hook %v: delta cpu %v cost %v usage cpu %v cost %v, want %v/%v",
						c.key, sum.CPUSeconds, c.c.Total, c.cpu, c.cost, want.cpu, want.cost)
				}
			}

			// Replay (collector retry): no change, no hooks.
			if w := post(t, h, "application/json", "", fixtureJSON(t)); w.Code != http.StatusOK {
				t.Fatalf("replay status %d", w.Code)
			}
			checkUsage(t, s, fixtureWant)
			if calls := hooks.reset(); len(calls) != 0 {
				t.Errorf("replay hook calls = %d, want 0", len(calls))
			}
		})
	}
}

// Records split across requests (one per record, in fixture order: all
// ateapi state events first, then the ateom samples with t1's final@185
// before periodic@65/@125) give the same totals as one batch, except for one
// bounded loss: a sample older than the newest one of its epoch adds its cpu
// but no memory (the interval was already integrated from the previous newer
// sample), so t1 memory is 152 instead of 212 GiB·s (lost: 60 GiB·s, at most
// one gap cap per reordered sample). CPU is order-independent. Active time
// differs by a bounded amount: the state events arrive before any sample, so
// [running@0, suspending@190) is counted from the state events alone, and the
// later final sample (synthetic: 5s before suspending) cannot take the 5s
// back; t1 active is 294 instead of 289 (the batch sees the samples first).
func TestFixtureRecordByRecord(t *testing.T) {
	s := usage.NewMemoryStore()
	h := newTestHandler(s, newFakeTasks(task("default", "t1", ""), task("team-a", "t2", "")), nil)
	for _, rl := range fixtureProto(t).GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				one := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
					Resource:  rl.GetResource(),
					ScopeLogs: []*logspb.ScopeLogs{{Scope: sl.GetScope(), LogRecords: []*logspb.LogRecord{rec}}},
				}}}
				body, _ := proto.Marshal(one)
				if w := post(t, h, "application/x-protobuf", "", body); w.Code != http.StatusOK {
					t.Fatalf("status %d", w.Code)
				}
			}
		}
	}
	t1 := fixtureWant[0]
	t1.mem = 152
	t1.active = 294
	t1.cost = t1.cpu*0.001 + t1.mem*0.0001
	checkUsage(t, s, []wantUsage{t1, fixtureWant[1]})
}

// Acceptance 2: protobuf and JSON (and gzip) accepted; wrong method → 405;
// oversized → 413.
func TestEncodingsAndErrors(t *testing.T) {
	pb, err := proto.Marshal(fixtureProto(t))
	if err != nil {
		t.Fatal(err)
	}
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(pb)
	zw.Close()
	var gzJSON bytes.Buffer
	zw = gzip.NewWriter(&gzJSON)
	zw.Write(fixtureJSON(t))
	zw.Close()

	for _, tc := range []struct {
		name, ct, enc string
		body          []byte
		wantCT        string
	}{
		{"protobuf", "application/x-protobuf", "", pb, "application/x-protobuf"},
		{"protobuf no content type", "", "", pb, "application/x-protobuf"},
		{"json", "application/json; charset=utf-8", "", fixtureJSON(t), "application/json"},
		{"gzip protobuf", "application/x-protobuf", "gzip", gz.Bytes(), "application/x-protobuf"},
		{"gzip json", "application/json", "gzip", gzJSON.Bytes(), "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := usage.NewMemoryStore()
			h := newTestHandler(s, newFakeTasks(task("default", "t1", ""), task("team-a", "t2", "")), nil)
			w := post(t, h, tc.ct, tc.enc, tc.body)
			if w.Code != http.StatusOK {
				t.Fatalf("status %d: %s", w.Code, w.Body)
			}
			if got := w.Header().Get("Content-Type"); got != tc.wantCT {
				t.Errorf("Content-Type = %q, want %q", got, tc.wantCT)
			}
			resp := &collogspb.ExportLogsServiceResponse{}
			if tc.wantCT == "application/json" {
				err = protojson.Unmarshal(w.Body.Bytes(), resp)
			} else {
				err = proto.Unmarshal(w.Body.Bytes(), resp)
			}
			if err != nil {
				t.Errorf("response not an ExportLogsServiceResponse: %v", err)
			}
			checkUsage(t, s, fixtureWant)
		})
	}

	h := newTestHandler(usage.NewMemoryStore(), newFakeTasks(), nil)
	big := bytes.Repeat([]byte{'x'}, MaxBodyBytes+1)
	var bigGz bytes.Buffer
	zw = gzip.NewWriter(&bigGz)
	zw.Write(big)
	zw.Close()
	// A gzip body that is itself over the limit (stored, incompressible).
	var hugeGz bytes.Buffer
	zw, _ = gzip.NewWriterLevel(&hugeGz, gzip.NoCompression)
	zw.Write(big)
	zw.Close()
	for _, tc := range []struct {
		name, method, ct, enc string
		body                  []byte
		chunked               bool
		want                  int
	}{
		{"GET", http.MethodGet, "application/x-protobuf", "", nil, false, http.StatusMethodNotAllowed},
		{"PUT", http.MethodPut, "application/x-protobuf", "", pb, false, http.StatusMethodNotAllowed},
		{"oversized", http.MethodPost, "application/x-protobuf", "", big, false, http.StatusRequestEntityTooLarge},
		{"oversized chunked", http.MethodPost, "application/x-protobuf", "", big, true, http.StatusRequestEntityTooLarge},
		{"oversized after gunzip", http.MethodPost, "application/x-protobuf", "gzip", bigGz.Bytes(), false, http.StatusRequestEntityTooLarge},
		{"oversized compressed chunked", http.MethodPost, "application/x-protobuf", "gzip", hugeGz.Bytes(), true, http.StatusRequestEntityTooLarge},
		{"unsupported content type", http.MethodPost, "text/plain", "", pb, false, http.StatusUnsupportedMediaType},
		{"unsupported encoding", http.MethodPost, "application/x-protobuf", "br", pb, false, http.StatusUnsupportedMediaType},
		{"bad gzip", http.MethodPost, "application/x-protobuf", "gzip", pb, false, http.StatusBadRequest},
		{"bad protobuf", http.MethodPost, "application/x-protobuf", "", []byte{0xff, 0xff, 0xff}, false, http.StatusBadRequest},
		{"bad json", http.MethodPost, "application/json", "", []byte("{"), false, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "/v1/logs", bytes.NewReader(tc.body))
			if tc.chunked {
				r.ContentLength = -1
			}
			r.Header.Set("Content-Type", tc.ct)
			if tc.enc != "" {
				r.Header.Set("Content-Encoding", tc.enc)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Errorf("status %d, want %d (%s)", w.Code, tc.want, w.Body)
			}
			if tc.want == http.StatusMethodNotAllowed && w.Header().Get("Allow") != http.MethodPost {
				t.Errorf("Allow = %q", w.Header().Get("Allow"))
			}
		})
	}
}

// --- record builders for focused tests ---

var t0 = time.Date(2026, 10, 16, 10, 0, 0, 0, time.UTC)

func sv(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}
func iv(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}
func dv(k string, v float64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v}}}
}

func sampleRec(name, uid string, off time.Duration, epoch int64, cpu float64, ws int64) *logspb.LogRecord {
	return &logspb.LogRecord{
		TimeUnixNano: uint64(t0.Add(off).UnixNano()),
		EventName:    EventUsageSampled,
		Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Actor usage sampled"}},
		Attributes: []*commonpb.KeyValue{
			sv(AttrAtespace, "default"), sv(AttrActorName, name), sv(AttrActorUID, uid),
			sv(AttrWorkerPoolName, "ax-pool"), sv(AttrStatsKind, "periodic"), iv(AttrEpoch, epoch),
			dv(AttrCPUTime, cpu), iv(AttrMemWorkingSet, ws),
		},
	}
}

func stateRec(name, uid string, off time.Duration, state string) *logspb.LogRecord {
	return &logspb.LogRecord{
		TimeUnixNano: uint64(t0.Add(off).UnixNano()),
		EventName:    EventStateChanged,
		Attributes: []*commonpb.KeyValue{
			sv(AttrAtespace, "default"), sv(AttrActorName, name), sv(AttrActorUID, uid),
			sv(AttrActorOperation, "resume"), sv(AttrActorState, state),
		},
	}
}

func request(recs ...*logspb.LogRecord) *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: recs}},
	}}}
}

// Acceptance 3: events for actors without an AX task are ignored; a uid
// mismatch (against the task's status.actorUid or the recorded uid) is ignored.
func TestNoTaskAndUIDMismatch(t *testing.T) {
	const epoch = int64(1)
	s := usage.NewMemoryStore()
	hooks := &recordingHooks{}
	h := newTestHandler(s, newFakeTasks(task("default", "pinned", "uid-a"), task("default", "free", "")), hooks)
	ctx := context.Background()

	st, err := h.Ingest(ctx, request(
		sampleRec("ghost", "uid-g", 0, epoch, 1, 1<<30),
		sampleRec("pinned", "uid-b", 0, epoch, 1, 1<<30), // ≠ status.actorUid
		sampleRec("pinned", "uid-b", time.Minute, epoch, 5, 1<<30),
		sampleRec("pinned", "uid-a", 0, epoch, 2, 1<<30),
		sampleRec("free", "uid-x", 0, epoch, 3, 1<<30),
		sampleRec("free", "uid-y", time.Minute, epoch, 30, 1<<30), // ≠ recorded uid
	))
	if err != nil {
		t.Fatal(err)
	}
	if st.Records != 6 || st.Events != 6 || st.NoTask != 1 || st.UIDSkip != 2 || st.Tasks != 2 || st.Deltas != 2 {
		t.Errorf("stats = %+v", st)
	}
	if _, err := s.Get(ctx, usage.Key{Atespace: "default", Name: "ghost"}); !errors.Is(err, usage.ErrNotFound) {
		t.Errorf("ghost: err = %v, want ErrNotFound", err)
	}
	pinned, err := s.Get(ctx, usage.Key{Atespace: "default", Name: "pinned"})
	if err != nil {
		t.Fatal(err)
	}
	if pinned.CPUSeconds != 2 || pinned.State.ActorUID != "uid-a" {
		t.Errorf("pinned: cpu=%v uid=%q, want 2 uid-a", pinned.CPUSeconds, pinned.State.ActorUID)
	}
	free, err := s.Get(ctx, usage.Key{Atespace: "default", Name: "free"})
	if err != nil {
		t.Fatal(err)
	}
	if free.CPUSeconds != 3 || free.State.ActorUID != "uid-x" {
		t.Errorf("free: cpu=%v uid=%q, want 3 uid-x (first uid recorded)", free.CPUSeconds, free.State.ActorUID)
	}
}

// Acceptance 4: hooks are called with non-empty deltas only, and only after
// the store committed.
func TestHooksNonEmptyOnly(t *testing.T) {
	s := usage.NewMemoryStore()
	hooks := &recordingHooks{}
	h := newTestHandler(s, newFakeTasks(task("default", "t", "")), hooks)
	ctx := context.Background()
	key := usage.Key{Atespace: "default", Name: "t"}

	pending := sampleRec("t", "u", 0, 1, 0, 0)
	pending.Attributes = pending.Attributes[:6] // measurements absent
	steps := []struct {
		name      string
		req       *collogspb.ExportLogsServiceRequest
		wantHooks int
		wantStore bool
	}{
		// These records write state (open interval, uid, epoch, last
		// sample) but their deltas are empty, so no hook.
		{"running", request(stateRec("t", "u", 0, "running")), 0, true},
		{"pending sample", request(pending), 0, true},
		{"replayed pending sample", request(pending), 0, true},
		{"measured sample", request(sampleRec("t", "u", 60*time.Second, 1, 1.5, 1<<30)), 1, true},
		{"replayed measured sample", request(sampleRec("t", "u", 60*time.Second, 1, 1.5, 1<<30)), 0, true},
		{"stale state event", request(stateRec("t", "u", 0, "suspended")), 0, true},
		{"unknown event", request(&logspb.LogRecord{EventName: "ate.actor.something", Attributes: []*commonpb.KeyValue{
			sv(AttrAtespace, "default"), sv(AttrActorName, "t")}}), 0, true},
	}
	for _, step := range steps {
		w := post(t, h, "application/x-protobuf", "", mustMarshal(t, step.req))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: status %d", step.name, w.Code)
		}
		calls := hooks.reset()
		if len(calls) != step.wantHooks {
			t.Errorf("%s: hook calls = %d, want %d", step.name, len(calls), step.wantHooks)
		}
		for _, c := range calls {
			if c.d.IsZero() {
				t.Errorf("%s: hook with empty delta", step.name)
			}
		}
		if _, err := s.Get(ctx, key); (err == nil) != step.wantStore {
			t.Errorf("%s: Get err = %v", step.name, err)
		}
	}
	u, _ := s.Get(ctx, key)
	if !approx(u.CPUSeconds, 1.5) || !approx(u.ActiveSeconds, 60) {
		t.Errorf("cpu=%v active=%v, want 1.5 60", u.CPUSeconds, u.ActiveSeconds)
	}
}

// Task-lookup and store failures are reported as 503 (retryable); the
// accounting makes the retry safe.
func TestLookupErrorIsRetryable(t *testing.T) {
	tasks := newFakeTasks(task("default", "t1", ""), task("team-a", "t2", ""))
	tasks.err = errors.New("redis down")
	s := usage.NewMemoryStore()
	h := newTestHandler(s, tasks, nil)
	if w := post(t, h, "application/json", "", fixtureJSON(t)); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	tasks.err = nil
	if w := post(t, h, "application/json", "", fixtureJSON(t)); w.Code != http.StatusOK {
		t.Fatalf("retry status %d", w.Code)
	}
	checkUsage(t, s, fixtureWant)
}

// The decoder accepts the event name from the LogRecord field, the legacy
// event.name attribute, or the body; and numeric attributes in any OTLP type.
func TestDecodeLenient(t *testing.T) {
	h := NewHandler(Options{Clock: func() time.Time { return t0 }})
	for _, tc := range []struct {
		name     string
		rec      *logspb.LogRecord
		res      []*commonpb.KeyValue
		wantOK   bool
		wantName string
		check    func(t *testing.T, ev event)
	}{
		{
			name: "string-typed numbers, legacy event.name attr, observed time",
			rec: &logspb.LogRecord{
				ObservedTimeUnixNano: uint64(t0.Add(time.Second).UnixNano()),
				Attributes: []*commonpb.KeyValue{
					sv("event.name", EventUsageSampled), sv(AttrAtespace, "a"), sv(AttrActorName, "n"),
					sv(AttrEpoch, "1788903185000000000"), sv(AttrCPUTime, "0.25"), sv(AttrMemWorkingSet, "1024"),
				},
			},
			wantOK: true, wantName: EventUsageSampled,
			check: func(t *testing.T, ev event) {
				s := ev.sample
				if s.Epoch != 1788903185000000000 || s.CPUSeconds == nil || *s.CPUSeconds != 0.25 ||
					s.WorkingSetBytes == nil || *s.WorkingSetBytes != 1024 || !s.Time.Equal(t0.Add(time.Second)) {
					t.Errorf("sample = %+v", s)
				}
			},
		},
		{
			name: "int cpu, double working set, identity on the resource",
			rec: &logspb.LogRecord{
				Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Actor usage sampled"}},
				Attributes: []*commonpb.KeyValue{iv(AttrEpoch, 7), iv(AttrCPUTime, 3), dv(AttrMemWorkingSet, 2048)},
			},
			res:    []*commonpb.KeyValue{sv(AttrAtespace, "a"), sv(AttrActorName, "n")},
			wantOK: true, wantName: EventUsageSampled,
			check: func(t *testing.T, ev event) {
				s := ev.sample
				if s.Epoch != 7 || *s.CPUSeconds != 3 || *s.WorkingSetBytes != 2048 || !s.Time.Equal(t0) {
					t.Errorf("sample = %+v", s)
				}
				if ev.atespace != "a" || ev.actor != "n" {
					t.Errorf("ref = %s/%s", ev.atespace, ev.actor)
				}
			},
		},
		{
			name: "pending sample: measurements nil",
			rec: &logspb.LogRecord{EventName: EventUsageSampled, Attributes: []*commonpb.KeyValue{
				sv(AttrActorName, "n"), iv(AttrEpoch, 7), sv("ate.stats.source", "unspecified")}},
			wantOK: true, wantName: EventUsageSampled,
			check: func(t *testing.T, ev event) {
				if ev.sample.CPUSeconds != nil || ev.sample.WorkingSetBytes != nil || ev.atespace != "default" {
					t.Errorf("event = %+v", ev)
				}
			},
		},
		{
			name: "crashed body-only without state attribute",
			rec: &logspb.LogRecord{
				Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Actor crashed"}},
				Attributes: []*commonpb.KeyValue{sv(AttrAtespace, "a"), sv(AttrActorName, "n"), sv(AttrActorUID, "u")},
			},
			wantOK: true, wantName: EventCrashed,
			check: func(t *testing.T, ev event) {
				if ev.change.State != usage.StateCrashed || ev.change.ActorUID != "u" {
					t.Errorf("change = %+v", ev.change)
				}
			},
		},
		{
			name: "event_name wins over body",
			rec: &logspb.LogRecord{EventName: EventStateChanged,
				Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Actor usage sampled"}},
				Attributes: []*commonpb.KeyValue{sv(AttrActorName, "n"), sv(AttrActorState, "suspending"), sv(AttrActorOperation, "suspend")}},
			wantOK: true, wantName: EventStateChanged,
			check: func(t *testing.T, ev event) {
				if ev.change.State != "suspending" || ev.change.Operation != "suspend" {
					t.Errorf("change = %+v", ev.change)
				}
			},
		},
		{name: "state change without state", rec: &logspb.LogRecord{EventName: EventStateChanged,
			Attributes: []*commonpb.KeyValue{sv(AttrActorName, "n")}}},
		{name: "no actor name", rec: &logspb.LogRecord{EventName: EventUsageSampled}},
		{name: "other event", rec: &logspb.LogRecord{EventName: "ate.worker.something",
			Attributes: []*commonpb.KeyValue{sv(AttrActorName, "n")}}},
		{name: "plain log line", rec: &logspb.LogRecord{
			Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
			Attributes: []*commonpb.KeyValue{sv(AttrActorName, "n")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, ok := h.decode(tc.rec, tc.res)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if ev.name != tc.wantName {
				t.Errorf("name = %q, want %q", ev.name, tc.wantName)
			}
			if tc.check != nil {
				tc.check(t, ev)
			}
		})
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Regression (verifier D1): a task deleted and re-created with the same name
// must not stay pinned to the previous actor's uid. Verifier sequence: OLD
// "deleted"@10s arrives, then NEW create/running@20s and a sample.
func TestTaskRecreatedWithSameName(t *testing.T) {
	const oldUID, newUID = "uid-old", "uid-new"
	newEpoch := t0.Add(19 * time.Second).UnixNano()
	seq := func() *collogspb.ExportLogsServiceRequest {
		created := stateRec("t", newUID, 12*time.Second, "suspended")
		created.Attributes[3] = sv(AttrActorOperation, "create")
		return request(
			stateRec("t", oldUID, 10*time.Second, "deleted"),
			created,
			stateRec("t", newUID, 20*time.Second, "running"),
			sampleRec("t", newUID, 80*time.Second, newEpoch, 3, 1<<30),
		)
	}
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(t0.Add(d)) }
	for _, tc := range []struct {
		name               string
		created            *timestamppb.Timestamp
		statusUID          string
		separate           bool // old event in its own earlier request
		wantStale, wantRst int
	}{
		{name: "creation timestamp drops the old event", created: ts(13 * time.Second), wantStale: 1},
		{name: "old event within skew, new actor's create replaces it", created: ts(11500 * time.Millisecond), wantRst: 1},
		{name: "no creation timestamp, new actor's create replaces it", wantRst: 1},
		{name: "no creation timestamp, separate requests", separate: true, wantRst: 1},
		{name: "status.actorUid resets a record pinned to the old actor", statusUID: newUID, separate: true, wantRst: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := usage.NewMemoryStore()
			tk := task("default", "t", tc.statusUID)
			tk.Metadata.CreationTimestamp = tc.created
			h := newTestHandler(s, newFakeTasks(tk), nil)
			ctx := context.Background()
			req := seq()
			var stats Stats
			if tc.separate {
				recs := req.ResourceLogs[0].ScopeLogs[0].LogRecords
				// The old actor's record lands first, pinning the uid (with
				// status.actorUid set, a record written before T08 set it).
				pin := NewHandler(Options{Store: s, Tasks: newFakeTasks(task("default", "t", "")), Pricer: linearPricer{}})
				if _, err := pin.Ingest(ctx, request(recs[0])); err != nil {
					t.Fatal(err)
				}
				req = request(recs[1:]...)
			}
			stats, err := h.Ingest(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if stats.Stale != tc.wantStale || stats.Resets != tc.wantRst {
				t.Errorf("stats = %+v, want stale %d resets %d", stats, tc.wantStale, tc.wantRst)
			}
			u, err := s.Get(ctx, usage.Key{Atespace: "default", Name: "t"})
			if err != nil {
				t.Fatal(err)
			}
			// Epoch 19s → last sample 80s: 61s active; running@20 open → 80.
			if u.State.ActorUID != newUID || u.CPUSeconds != 3 || !approx(u.ActiveSeconds, 61) {
				t.Errorf("uid=%q cpu=%v active=%v, want %q 3 61", u.State.ActorUID, u.CPUSeconds, u.ActiveSeconds, newUID)
			}
		})
	}

	// A late final sample of the old actor (older than the new actor's
	// events) stays rejected, and a stray foreign sample cannot flip a live
	// actor's record.
	t.Run("late old sample and stray foreign sample are ignored", func(t *testing.T) {
		s := usage.NewMemoryStore()
		h := newTestHandler(s, newFakeTasks(task("default", "t", "")), nil)
		ctx := context.Background()
		if _, err := h.Ingest(ctx, seq()); err != nil {
			t.Fatal(err)
		}
		st, err := h.Ingest(ctx, request(
			sampleRec("t", oldUID, 9*time.Second, t0.UnixNano(), 61, 1<<30),
			sampleRec("t", "uid-stray", 200*time.Second, t0.UnixNano(), 99, 1<<30),
		))
		if err != nil {
			t.Fatal(err)
		}
		u, _ := s.Get(ctx, usage.Key{Atespace: "default", Name: "t"})
		if st.Resets != 0 || u.State.ActorUID != newUID || u.CPUSeconds != 3 {
			t.Errorf("resets=%d uid=%q cpu=%v, want 0 %q 3", st.Resets, u.State.ActorUID, u.CPUSeconds, newUID)
		}
	})
}
