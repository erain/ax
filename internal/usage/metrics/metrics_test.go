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

package metrics

import (
	"context"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/google/ax/internal/usage"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type fakeLabels map[usage.Key]map[string]string

func (f fakeLabels) Labels(_ context.Context, k usage.Key) map[string]string { return f[k] }

func newTestRecorder(t *testing.T, keys func() []string, labels LabelLookup) (*Recorder, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	r, err := New(mp.Meter(ScopeName), keys, labels)
	if err != nil {
		t.Fatal(err)
	}
	return r, reader
}

func staticKeys(keys ...string) func() []string { return func() []string { return keys } }

// series is one collected metric: unit plus value per encoded attribute set
// ("k=v,k=v", keys sorted).
type series struct {
	unit      string
	monotonic bool
	temporal  metricdata.Temporality
	points    map[string]float64
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]series {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]series{}
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != ScopeName {
			t.Errorf("scope = %q, want %q", sm.Scope.Name, ScopeName)
		}
		for _, m := range sm.Metrics {
			s := series{unit: m.Unit, points: map[string]float64{}}
			switch d := m.Data.(type) {
			case metricdata.Sum[float64]:
				s.monotonic, s.temporal = d.IsMonotonic, d.Temporality
				for _, dp := range d.DataPoints {
					s.points[dp.Attributes.Encoded(attribute.DefaultEncoder())] = dp.Value
				}
			case metricdata.Sum[int64]:
				s.monotonic, s.temporal = d.IsMonotonic, d.Temporality
				for _, dp := range d.DataPoints {
					s.points[dp.Attributes.Encoded(attribute.DefaultEncoder())] = float64(dp.Value)
				}
			default:
				t.Errorf("metric %s: unexpected data %T", m.Name, m.Data)
			}
			out[m.Name] = s
		}
	}
	return out
}

// enc returns the encoded attribute set of comma-separated "k=v" lists
// (keys are sorted, as in collected data).
func enc(lists ...string) string {
	var kvs []attribute.KeyValue
	for _, l := range lists {
		for _, kv := range strings.Split(l, ",") {
			k, v, _ := strings.Cut(kv, "=")
			kvs = append(kvs, attribute.String(k, v))
		}
	}
	s := attribute.NewSet(kvs...)
	return s.Encoded(attribute.DefaultEncoder())
}

func approxEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func checkPoints(t *testing.T, got map[string]series, name, unit string, want map[string]float64) {
	t.Helper()
	s, ok := got[name]
	if !ok {
		if len(want) > 0 {
			t.Errorf("metric %s missing", name)
		}
		return
	}
	if s.unit != unit {
		t.Errorf("%s unit = %q, want %q", name, s.unit, unit)
	}
	if !s.monotonic || s.temporal != metricdata.CumulativeTemporality {
		t.Errorf("%s: monotonic=%v temporality=%v, want monotonic cumulative", name, s.monotonic, s.temporal)
	}
	if len(s.points) != len(want) {
		t.Errorf("%s: %d series, want %d:\n got %v\nwant %v", name, len(s.points), len(want), s.points, want)
	}
	for attrs, v := range want {
		if g, ok := s.points[attrs]; !ok || !approxEqual(g, v) {
			t.Errorf("%s{%s} = %v (present %v), want %v", name, attrs, g, ok, v)
		}
	}
}

func testDelta() usage.Delta {
	return usage.Delta{Days: map[string]*usage.DayDelta{
		"2026-10-15": {Amounts: usage.Amounts{
			CPUSeconds: 10, ActiveSeconds: 60, IdleSeconds: 30, MemoryGiBSeconds: 99,
			Tokens: map[string]usage.TokenCounts{
				"gemini-2.5-flash": {Input: 1000, Output: 200, CacheRead: 400, Reasoning: 50},
			},
		}},
		"2026-10-16": {Amounts: usage.Amounts{
			CPUSeconds: 5, ActiveSeconds: 30,
			Tokens: map[string]usage.TokenCounts{
				"gemini-2.5-flash": {Input: 100},
				"gemini-2.5-pro":   {Input: 10, Output: 5, CacheWrite: 3},
			},
		}},
	}}
}

func testCost() usage.Cost {
	return usage.Cost{Currency: "USD", Compute: 0.5, Idle: 0.125, Tokens: 0.25, Storage: 0.0625, Total: 0.8125}
}

// Acceptance 1: after deltas, the collected metrics have the expected names,
// units, attribute sets and values.
func TestRecorder_OnDeltaMetrics(t *testing.T) {
	labels := fakeLabels{
		{Atespace: "default", Name: "t1"}: {"agent": "triage", "customer": "acme"},
		{Atespace: "default", Name: "t2"}: {"agent": "triage", "customer": "acme"},
		{Atespace: "team-b", Name: "t3"}:  {"agent": "coder"},
	}
	r, reader := newTestRecorder(t, staticKeys("agent", "customer"), labels)
	ctx := context.Background()
	r.OnDelta(ctx, usage.Key{Atespace: "default", Name: "t1"}, testDelta(), testCost(), nil)
	r.OnDelta(ctx, usage.Key{Atespace: "default", Name: "t2"}, testDelta(), testCost(), nil)
	// t3: only CPU and storage, in another atespace, without a customer label.
	r.OnDelta(ctx, usage.Key{Atespace: "team-b", Name: "t3"},
		usage.Delta{Days: map[string]*usage.DayDelta{"2026-10-16": {Amounts: usage.Amounts{CPUSeconds: 2, StorageGiBSeconds: 7}}}},
		usage.Cost{Currency: "USD", Compute: 0.25, Storage: 0.5, Total: 0.75}, nil)
	// Zero delta: records nothing.
	r.OnDelta(ctx, usage.Key{Atespace: "team-b", Name: "t3"}, usage.Delta{}, usage.Cost{}, nil)

	got := collect(t, reader)
	names := make([]string, 0, len(got))
	for n := range got {
		names = append(names, n)
	}
	sort.Strings(names)
	if want := "ax.task.active.time,ax.task.cost,ax.task.cpu.time,ax.task.idle.time,ax.task.tokens"; strings.Join(names, ",") != want {
		t.Errorf("metric names = %v, want %s", names, want)
	}

	acme := "ax.atespace=default,ax.label.agent=triage,ax.label.customer=acme"
	coder := "ax.atespace=team-b,ax.label.agent=coder,ax.label.customer=none"
	checkPoints(t, got, MetricCost, "USD", map[string]float64{
		enc(acme, "ax.cost.kind=compute"):  2 * 0.375,
		enc(acme, "ax.cost.kind=idle"):     2 * 0.125,
		enc(acme, "ax.cost.kind=tokens"):   2 * 0.25,
		enc(acme, "ax.cost.kind=storage"):  2 * 0.0625,
		enc(coder, "ax.cost.kind=compute"): 0.25,
		enc(coder, "ax.cost.kind=storage"): 0.5,
	})
	// Sum over kinds = total cost.
	var totalAcme float64
	for _, kind := range []string{"compute", "idle", "tokens", "storage"} {
		totalAcme += got[MetricCost].points[enc(acme, "ax.cost.kind="+kind)]
	}
	if !approxEqual(totalAcme, 2*0.8125) {
		t.Errorf("sum of ax.task.cost kinds = %v, want %v", totalAcme, 2*0.8125)
	}
	checkPoints(t, got, MetricCPUTime, "s", map[string]float64{acme: 30, coder: 2})
	checkPoints(t, got, MetricActiveTime, "s", map[string]float64{acme: 180})
	checkPoints(t, got, MetricIdleTime, "s", map[string]float64{acme: 60})
	flash := acme + ",gen_ai.request.model=gemini-2.5-flash,gen_ai.token.type="
	pro := acme + ",gen_ai.request.model=gemini-2.5-pro,gen_ai.token.type="
	checkPoints(t, got, MetricTokens, "{token}", map[string]float64{
		flash + "input":      2 * 1100,
		flash + "output":     2 * 200,
		flash + "cache_read": 2 * 400,
		flash + "reasoning":  2 * 50,
		pro + "input":        2 * 10,
		pro + "output":       2 * 5,
		pro + "cache_write":  2 * 3,
	})
}

// Acceptance 2: only the configured label keys appear as attributes; missing
// or empty labels are "none"; the key set follows labelKeys() (rate card
// reload); no per-task attribute ever appears.
func TestRecorder_LabelCardinality(t *testing.T) {
	keys := []string{"agent"}
	labels := fakeLabels{
		{Atespace: "default", Name: "t1"}: {"agent": "triage", "customer": "acme", "run-id": "r-123", "pod": "x"},
		{Atespace: "default", Name: "t2"}: {"agent": "", "run-id": "r-456"},
	}
	r, reader := newTestRecorder(t, func() []string { return keys }, labels)
	ctx := context.Background()
	cpu := func(v float64) usage.Delta {
		return usage.Delta{Days: map[string]*usage.DayDelta{"2026-10-16": {Amounts: usage.Amounts{CPUSeconds: v}}}}
	}
	r.OnDelta(ctx, usage.Key{Atespace: "default", Name: "t1"}, cpu(1), usage.Cost{}, nil)
	r.OnDelta(ctx, usage.Key{Atespace: "default", Name: "t2"}, cpu(2), usage.Cost{}, nil)
	r.OnDelta(ctx, usage.Key{Atespace: "", Name: "unknown-task"}, cpu(4), usage.Cost{}, nil) // no labels; "" -> default
	r.OnThreshold(ctx, usage.Key{Atespace: "default", Name: "t1"}, DimensionMaxCost, LevelWarning)

	keys = []string{"customer", "customer", ""} // reloaded card (duplicates/empties ignored)
	r.OnDelta(ctx, usage.Key{Atespace: "default", Name: "t1"}, cpu(8), usage.Cost{}, nil)

	got := collect(t, reader)
	checkPoints(t, got, MetricCPUTime, "s", map[string]float64{
		"ax.atespace=default,ax.label.agent=triage":  1,
		"ax.atespace=default,ax.label.agent=none":    6,
		"ax.atespace=default,ax.label.customer=acme": 8,
	})
	allowed := map[string]bool{
		AttrAtespace: true, AttrLabelPrefix + "agent": true, AttrLabelPrefix + "customer": true,
		AttrCostKind: true, AttrModel: true, AttrTokenType: true, AttrBudgetDimension: true, AttrBudgetLevel: true,
	}
	for name, s := range got {
		for attrs := range s.points {
			for _, kv := range strings.Split(attrs, ",") {
				k, _, _ := strings.Cut(kv, "=")
				if !allowed[k] {
					t.Errorf("%s has unexpected attribute %q", name, k)
				}
				if strings.Contains(kv, "t1") || strings.Contains(kv, "r-123") {
					t.Errorf("%s has a per-task attribute value %q", name, kv)
				}
			}
		}
	}

	// No label keys configured, no lookup: only ax.atespace.
	r2, reader2 := newTestRecorder(t, nil, nil)
	r2.OnDelta(ctx, usage.Key{Atespace: "a", Name: "t"}, cpu(3), usage.Cost{}, nil)
	checkPoints(t, collect(t, reader2), MetricCPUTime, "s", map[string]float64{"ax.atespace=a": 3})

	// Keys configured but no lookup: every label is "none".
	r3, reader3 := newTestRecorder(t, staticKeys("agent"), nil)
	r3.OnDelta(ctx, usage.Key{Atespace: "a", Name: "t"}, cpu(3), usage.Cost{}, nil)
	checkPoints(t, collect(t, reader3), MetricCPUTime, "s", map[string]float64{"ax.atespace=a,ax.label.agent=none": 3})
}

func TestRecorder_OnThreshold(t *testing.T) {
	labels := LabelLookupFunc(func(_ context.Context, k usage.Key) map[string]string {
		return map[string]string{"agent": "a-" + k.Name[:1]}
	})
	r, reader := newTestRecorder(t, staticKeys("agent"), labels)
	ctx := context.Background()
	k := usage.Key{Atespace: "default", Name: "x1"}
	r.OnThreshold(ctx, k, DimensionMaxCost, LevelWarning)
	r.OnThreshold(ctx, usage.Key{Atespace: "default", Name: "x2"}, DimensionMaxCost, LevelWarning)
	r.OnThreshold(ctx, k, DimensionMaxCost, LevelExceeded)
	r.OnThreshold(ctx, k, DimensionMaxTokens, LevelWarning)
	r.OnThreshold(ctx, k, DimensionActiveDeadline, LevelExceeded)
	r.OnThreshold(ctx, k, "", "")

	got := collect(t, reader)
	base := "ax.atespace=default,ax.budget.dimension="
	checkPoints(t, got, MetricThresholdCrossed, "{event}", map[string]float64{
		base + "maxCost,ax.budget.level=warning,ax.label.agent=a-x":         2,
		base + "maxCost,ax.budget.level=exceeded,ax.label.agent=a-x":        1,
		base + "maxTokens,ax.budget.level=warning,ax.label.agent=a-x":       1,
		base + "activeDeadline,ax.budget.level=exceeded,ax.label.agent=a-x": 1,
		base + "unknown,ax.budget.level=unknown,ax.label.agent=a-x":         1,
	})
	if _, ok := got[MetricCost]; ok {
		t.Error("OnThreshold must not record cost")
	}
}

func TestRecorder_NegativeAndNaNIgnored(t *testing.T) {
	r, reader := newTestRecorder(t, nil, nil)
	r.OnDelta(context.Background(), usage.Key{Atespace: "a", Name: "t"},
		usage.Delta{Days: map[string]*usage.DayDelta{"2026-10-16": {Amounts: usage.Amounts{CPUSeconds: math.NaN(), ActiveSeconds: -1}}}},
		// Idle slightly above compute (float noise): compute kind clamps to 0.
		usage.Cost{Compute: 0.1, Idle: 0.1000000001, Tokens: -1}, nil)
	got := collect(t, reader)
	checkPoints(t, got, MetricCost, "USD", map[string]float64{"ax.atespace=a,ax.cost.kind=idle": 0.1000000001})
	for _, n := range []string{MetricCPUTime, MetricActiveTime, MetricTokens} {
		if s, ok := got[n]; ok && len(s.points) > 0 {
			t.Errorf("%s recorded %v", n, s.points)
		}
	}
}
