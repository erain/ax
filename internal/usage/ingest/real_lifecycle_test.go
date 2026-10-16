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
	"context"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/usage"
)

// testdata/real_lifecycle.json holds real Agent Substrate v0.5.0 ate.actor.*
// events captured on the ax-finops-poc cluster (T13, 2026-10-16) from Cloud
// Logging and converted back to the OTLP/JSON the sidecar collector forwards
// to ax-server (tracker validate/v13-cl2otlp.py --scrub: Cloud Logging
// stringifies attributes, so the OTLP types are restored from the Substrate
// source; actor uids are replaced by stable fake uuids; pod-identifying
// resource attributes are dropped; timestamps are the original nanoseconds;
// ate.stats.cpu.time keeps the 6 significant digits Cloud Logging shows).
//
// Two AX tasks, each create → resume → running → suspend → resume → delete:
//   - t13-sleep-095341: `sleep 3600`, suspended 60s after Running, resumed,
//     deleted 20s later;
//   - t13-burn-095341: `timeout 120 <busy loop>; sleep 600`, suspended 60s
//     after Running (mid-burn), resumed (Substrate restores the filesystem
//     and restarts the command, so it burns 120s again), deleted after the
//     burn.
//
// Plus the Substrate golden-snapshot actors of both templates (atespace
// ate-golden, uuid names), which have no AX task.
//
// Expected totals (computed independently from the records):
//   - active = Σ_epoch (final sample − ate.actor.epoch); the final sample comes
//     ~1ms after suspending/deleting, the epoch ~0.1s before running.
//     sleep: 62.830781470 + 22.055053950; burn: 62.301892241 + 132.866516415.
//   - cpu = Σ_epoch max(cpu.time): sleep 0.128511 + 0.133376; burn 63.026 +
//     121.003 (the burner: ~1.01 cpu-s per wall-second while burning).
//   - memory = Σ mean(working set) × Δt over consecutive samples per epoch.
//   - idle = 0 (no ≥5 min low-cpu streak).
const (
	realSleepUID = "0c5d2a76-1875-5025-ba01-6803dd3b89bc"
	realBurnUID  = "d8b0b808-ee89-53ac-aa83-0d728e0b8e14"
)

var realWant = []wantUsage{
	{
		key:    usage.Key{Atespace: "default", Name: "t13-sleep-095341"},
		active: 84.885835420, cpu: 0.261887, mem: 1.754753201265,
		uid: realSleepUID, pool: "ax-pool", state: "deleted",
	},
	{
		key:    usage.Key{Atespace: "default", Name: "t13-burn-095341"},
		active: 195.168408656, cpu: 184.029, mem: 4.348231925999,
		uid: realBurnUID, pool: "ax-pool", state: "deleted",
	},
}

func init() {
	for i := range realWant {
		realWant[i].cost = realWant[i].cpu*0.001 + realWant[i].mem*0.0001
	}
}

// Records in the fixture: 31 events of the two tasks, 18 of the golden actors.
const (
	realTaskEvents   = 31
	realGoldenEvents = 18
)

func realFixtureJSON(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/real_lifecycle.json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func realFixtureProto(t *testing.T) *collogspb.ExportLogsServiceRequest {
	t.Helper()
	req := &collogspb.ExportLogsServiceRequest{}
	if err := protojson.Unmarshal(realFixtureJSON(t), req); err != nil {
		t.Fatalf("real fixture is not valid OTLP JSON: %v", err)
	}
	return req
}

// realTasks: the sleep task has status.actorUid (set by the reconciler), the
// burner does not (the usage record pins the uid). Creation timestamps are
// when `ax apply` ran (the actors are created ~1-4s later).
func realTasks() *fakeTasks {
	created := timestamppb.New(time.Date(2026, 10, 16, 13, 53, 44, 0, time.UTC))
	sleep := task("default", "t13-sleep-095341", realSleepUID)
	sleep.Metadata.CreationTimestamp = created
	burn := task("default", "t13-burn-095341", "")
	burn.Metadata.CreationTimestamp = created
	return newFakeTasks(sleep, burn)
}

func checkReal(t *testing.T, s usage.Store, want []wantUsage, tol float64) {
	t.Helper()
	for _, w := range want {
		u, err := s.Get(context.Background(), w.key)
		if err != nil {
			t.Fatalf("Get(%v): %v", w.key, err)
		}
		near := func(a, b float64) bool { return math.Abs(a-b) <= tol }
		if !near(u.ActiveSeconds, w.active) || !near(u.CPUSeconds, w.cpu) ||
			!near(u.MemoryGiBSeconds, w.mem) || u.IdleSeconds != 0 {
			t.Errorf("%v: active=%.9f cpu=%.6f mem=%.12f idle=%v, want %.9f %.6f %.12f 0", w.key,
				u.ActiveSeconds, u.CPUSeconds, u.MemoryGiBSeconds, u.IdleSeconds, w.active, w.cpu, w.mem)
		}
		if !near(u.Cost.Total, w.cost) || u.Cost.Currency != "USD" {
			t.Errorf("%v: cost=%+v, want total %v USD", w.key, u.Cost, w.cost)
		}
		if u.State.ActorUID != w.uid || u.State.Pool != w.pool || u.State.State != w.state {
			t.Errorf("%v: uid=%q pool=%q state=%q, want %q %q %q", w.key,
				u.State.ActorUID, u.State.Pool, u.State.State, w.uid, w.pool, w.state)
		}
		if day := u.Days["2026-10-16"]; day == nil || len(u.Days) != 1 || !near(day.CPUSeconds, w.cpu) ||
			!near(day.ActiveSeconds, w.active) {
			t.Errorf("%v: days = %v, want one 2026-10-16 bucket with cpu %v active %v", w.key, u.Days, w.cpu, w.active)
		}
	}
}

// The fixture carries the exact attribute names and OTLP value types that
// Substrate v0.5.0 emits; this pins them so a regenerated fixture cannot
// silently drift (e.g. a stringly-typed epoch).
func TestRealFixtureSchema(t *testing.T) {
	type kind int
	const (
		str kind = iota
		i64
		f64
	)
	common := map[string]kind{
		AttrAtespace: str, AttrActorName: str, AttrActorUID: str,
		"ate.template.atespace": str, "ate.template.name": str,
	}
	perEvent := map[string]map[string]kind{
		EventStateChanged: {AttrActorOperation: str, AttrActorState: str},
		EventUsageSampled: {
			AttrWorkerPoolName: str, "ate.workerpool.namespace": str, "ate.sandbox.class": str,
			"ate.stats.source": str, AttrStatsKind: str, AttrEpoch: i64, AttrCPUTime: f64,
			AttrMemWorkingSet: i64, "ate.stats.memory.usage": i64, "ate.stats.memory.peak": i64,
		},
	}
	bodies := map[string]string{EventStateChanged: "Actor state changed", EventUsageSampled: "Actor usage sampled"}
	services := map[string]string{EventStateChanged: "ateapi", EventUsageSampled: "ateom-gvisor"}
	n := 0
	for _, rl := range realFixtureProto(t).GetResourceLogs() {
		svc := ""
		for _, kv := range rl.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				svc = kv.GetValue().GetStringValue()
			}
		}
		for _, sl := range rl.GetScopeLogs() {
			if sl.GetScope().GetName() != "github.com/agent-substrate/substrate/internal/actorevent" {
				t.Errorf("scope = %q", sl.GetScope().GetName())
			}
			for _, rec := range sl.GetLogRecords() {
				n++
				name := rec.GetEventName()
				want, ok := perEvent[name]
				if !ok {
					t.Fatalf("record %d: event_name %q", n, name)
				}
				if rec.GetBody().GetStringValue() != bodies[name] || rec.GetSeverityNumber() != logspb.SeverityNumber_SEVERITY_NUMBER_INFO ||
					rec.GetTimeUnixNano() == 0 || svc != services[name] {
					t.Errorf("record %d (%s): body %q severity %v time %d service %q", n, name,
						rec.GetBody().GetStringValue(), rec.GetSeverityNumber(), rec.GetTimeUnixNano(), svc)
				}
				got := map[string]kind{}
				for _, kv := range rec.GetAttributes() {
					switch kv.GetValue().GetValue().(type) {
					case *commonpb.AnyValue_StringValue:
						got[kv.GetKey()] = str
					case *commonpb.AnyValue_IntValue:
						got[kv.GetKey()] = i64
					case *commonpb.AnyValue_DoubleValue:
						got[kv.GetKey()] = f64
					default:
						t.Errorf("record %d: %s has type %T", n, kv.GetKey(), kv.GetValue().GetValue())
					}
				}
				if len(got) != len(common)+len(want) {
					t.Errorf("record %d (%s): %d attributes %v, want %d", n, name, len(got), got, len(common)+len(want))
				}
				for _, m := range []map[string]kind{common, want} {
					for k, kd := range m {
						if g, ok := got[k]; !ok || g != kd {
							t.Errorf("record %d (%s): attribute %s kind %v (present %v), want %v", n, name, k, g, ok, kd)
						}
					}
				}
			}
		}
	}
	if n != realTaskEvents+realGoldenEvents {
		t.Errorf("records = %d, want %d", n, realTaskEvents+realGoldenEvents)
	}
}

// The captured lifecycle yields the hand-derived totals; replaying it is a
// no-op.
func TestRealLifecycleGolden(t *testing.T) {
	for name, mk := range allStores {
		t.Run(name, func(t *testing.T) {
			s := mk(t)
			hooks := &recordingHooks{}
			h := newTestHandler(s, realTasks(), hooks)
			st, err := h.Ingest(context.Background(), realFixtureProto(t))
			if err != nil {
				t.Fatal(err)
			}
			want := Stats{Records: 49, Events: 49, NoTask: realGoldenEvents, Tasks: 2, Deltas: 2}
			if st != want {
				t.Errorf("stats = %+v, want %+v", st, want)
			}
			checkReal(t, s, realWant, 1e-6)
			if calls := hooks.reset(); len(calls) != 2 {
				t.Errorf("hook calls = %d, want 2", len(calls))
			}

			// Replay over HTTP (JSON, as a collector retry would): no change.
			if w := post(t, h, "application/json", "", realFixtureJSON(t)); w.Code != http.StatusOK {
				t.Fatalf("replay status %d: %s", w.Code, w.Body)
			}
			checkReal(t, s, realWant, 1e-6)
			if calls := hooks.reset(); len(calls) != 0 {
				t.Errorf("replay hook calls = %d, want 0", len(calls))
			}
		})
	}
}

// realRecords flattens the fixture into single-record requests (keeping each
// record's resource and scope).
func realRecords(t *testing.T) []*collogspb.ExportLogsServiceRequest {
	t.Helper()
	var out []*collogspb.ExportLogsServiceRequest
	for _, rl := range realFixtureProto(t).GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				out = append(out, &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
					Resource:  rl.GetResource(),
					ScopeLogs: []*logspb.ScopeLogs{{Scope: sl.GetScope(), LogRecords: []*logspb.LogRecord{rec}}},
				}}})
			}
		}
	}
	return out
}

func replayOneByOne(t *testing.T, h *Handler, reqs []*collogspb.ExportLogsServiceRequest) {
	t.Helper()
	for _, r := range reqs {
		body, err := proto.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		if w := post(t, h, "application/x-protobuf", "", body); w.Code != http.StatusOK {
			t.Fatalf("status %d: %s", w.Code, w.Body)
		}
	}
}

// interleave merges the per-source record streams in a random order that
// keeps each source's own order (ateapi state events and ateom samples reach
// the collector through different exporters, each in order).
func interleave(rng *rand.Rand, reqs []*collogspb.ExportLogsServiceRequest) []*collogspb.ExportLogsServiceRequest {
	bySource := map[string][]*collogspb.ExportLogsServiceRequest{}
	var sources []string
	for _, r := range reqs {
		svc := ""
		for _, kv := range r.GetResourceLogs()[0].GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				svc = kv.GetValue().GetStringValue()
			}
		}
		if _, ok := bySource[svc]; !ok {
			sources = append(sources, svc)
		}
		bySource[svc] = append(bySource[svc], r)
	}
	var out []*collogspb.ExportLogsServiceRequest
	for len(out) < len(reqs) {
		var live []string
		for _, src := range sources {
			if len(bySource[src]) > 0 {
				live = append(live, src)
			}
		}
		src := live[rng.IntN(len(live))]
		out = append(out, bySource[src][0])
		bySource[src] = bySource[src][1:]
	}
	return out
}

// Order robustness, one record per request (the ingestor sorts within a
// request, so a shuffled single batch is trivially identical).
//
//   - Per-source order kept, sources interleaved at random (what the collector
//     can deliver): CPU and memory are exact; active time is within 1s (a
//     RUNNING event seen before its epoch's samples opens [running, ...)
//     from the state event alone, up to ~0.5s more than the epoch interval).
//   - Fully shuffled (stress, beyond what the pipeline produces): CPU is exact;
//     active time never under-counts and over-counts by at most one sample
//     gap cap (120s) per epoch (documented bound: an epoch whose samples
//     arrive before any of its closing events is extended to its last sample
//   - gap cap, and coverage never shrinks); memory never over-counts beyond
//     trapezoid rounding, and a sample older than the newest one of its epoch
//     adds no memory (bounded loss, at most the whole fixture's memory here).
func TestRealLifecycleOrderRobust(t *testing.T) {
	recs := realRecords(t)
	gapCap := usage.DefaultSampleGapCap
	const epochs = 2
	for seed := uint64(1); seed <= 100; seed++ {
		rng := rand.New(rand.NewPCG(seed, 13))

		s := usage.NewMemoryStore()
		replayOneByOne(t, newTestHandler(s, realTasks(), nil), interleave(rng, recs))
		for _, w := range realWant {
			u, _ := s.Get(context.Background(), w.key)
			if math.Abs(u.CPUSeconds-w.cpu) > 1e-9 || math.Abs(u.MemoryGiBSeconds-w.mem) > 1e-6 ||
				u.ActiveSeconds < w.active-1e-6 || u.ActiveSeconds > w.active+1 || u.IdleSeconds != 0 {
				t.Errorf("interleaved seed %d %s: active %+.3f cpu %+.6f mem %+.6f idle %v vs golden", seed, w.key.Name,
					u.ActiveSeconds-w.active, u.CPUSeconds-w.cpu, u.MemoryGiBSeconds-w.mem, u.IdleSeconds)
			}
		}

		shuffled := append([]*collogspb.ExportLogsServiceRequest(nil), recs...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		s = usage.NewMemoryStore()
		h := newTestHandler(s, realTasks(), nil)
		replayOneByOne(t, h, shuffled)
		for _, w := range realWant {
			u, _ := s.Get(context.Background(), w.key)
			if math.Abs(u.CPUSeconds-w.cpu) > 1e-9 || u.ActiveSeconds < w.active-1e-6 ||
				u.ActiveSeconds > w.active+epochs*gapCap.Seconds() ||
				u.MemoryGiBSeconds > w.mem+0.01 || u.MemoryGiBSeconds < 0 || u.State.ActorUID != w.uid {
				t.Errorf("shuffled seed %d %s: active %+.3f cpu %+.6f mem %+.6f uid %q vs golden", seed, w.key.Name,
					u.ActiveSeconds-w.active, u.CPUSeconds-w.cpu, u.MemoryGiBSeconds-w.mem, u.State.ActorUID)
			}
		}
		// A batch replay of everything afterwards changes no CPU.
		if _, err := h.Ingest(context.Background(), realFixtureProto(t)); err != nil {
			t.Fatal(err)
		}
		for _, w := range realWant {
			if u, _ := s.Get(context.Background(), w.key); math.Abs(u.CPUSeconds-w.cpu) > 1e-9 {
				t.Errorf("shuffled seed %d %s: cpu after batch replay %+.6f", seed, w.key.Name, u.CPUSeconds-w.cpu)
			}
		}
	}
}
