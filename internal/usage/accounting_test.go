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

package usage

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func f64(v float64) *float64 { return &v }
func i64(v int64) *int64     { return &v }

func at(sec float64) time.Time { return t0.Add(time.Duration(sec * float64(time.Second))) }

func approx(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

const oneGiB = int64(1 << 30)

// Acceptance 1.
func TestAccounting_CPUSumOfEpochMaxima(t *testing.T) {
	e1, e2 := t0.UnixNano(), t0.Add(time.Hour).UnixNano()
	samples := []UsageSample{
		{Time: at(0), Epoch: e1, Kind: "initial", CPUSeconds: f64(1.0)},
		{Time: at(120), Epoch: e1, Kind: "periodic", CPUSeconds: f64(3.0)},
		{Time: at(60), Epoch: e1, Kind: "periodic", CPUSeconds: f64(2.5)}, // out of order
		{Time: at(180), Epoch: e1, Kind: "final", CPUSeconds: f64(4.0)},
		{Time: at(3600), Epoch: e2, Kind: "initial", CPUSeconds: f64(0.5)},
		{Time: at(3660), Epoch: e2, Kind: "periodic", CPUSeconds: f64(1.5)},
	}
	u := NewTaskUsage()
	var sum Delta
	for _, s := range samples {
		sum.Add(u.ApplyUsageSample(s, AccountingConfig{}))
	}
	approx(t, "CPUSeconds", u.CPUSeconds, 5.5)
	approx(t, "delta CPUSeconds", sum.Sum().CPUSeconds, 5.5)
	approx(t, "day CPUSeconds", u.Day(DateOf(t0)).CPUSeconds, 5.5)

	before, _ := json.Marshal(u)
	for _, s := range samples {
		if d := u.ApplyUsageSample(s, AccountingConfig{}); !d.IsZero() {
			t.Errorf("replay of %+v produced delta %+v", s, d.Sum())
		}
	}
	approx(t, "CPUSeconds after replay", u.CPUSeconds, 5.5)
	after, _ := json.Marshal(u)
	if string(before) != string(after) {
		t.Errorf("replay changed usage:\n%s\n%s", before, after)
	}
}

// Acceptance 2.
func TestAccounting_MemoryGiBSeconds(t *testing.T) {
	u := NewTaskUsage()
	e := t0.UnixNano()
	for i := range 3 {
		u.ApplyUsageSample(UsageSample{Time: at(float64(60 * i)), Epoch: e, WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
	}
	approx(t, "MemoryGiBSeconds", u.MemoryGiBSeconds, 120)

	// A 10-minute gap is capped at 120s.
	d := u.ApplyUsageSample(UsageSample{Time: at(120 + 600), Epoch: e, WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
	approx(t, "gap delta", d.Sum().MemoryGiBSeconds, 120)
	approx(t, "MemoryGiBSeconds", u.MemoryGiBSeconds, 240)

	// Intervals spanning midnight are split per day.
	u = NewTaskUsage()
	mid := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	u.ApplyUsageSample(UsageSample{Time: mid.Add(-30 * time.Second), Epoch: e, WorkingSetBytes: i64(2 * oneGiB)}, AccountingConfig{})
	d = u.ApplyUsageSample(UsageSample{Time: mid.Add(30 * time.Second), Epoch: e, WorkingSetBytes: i64(2 * oneGiB)}, AccountingConfig{})
	if got := d.Dates(); !reflect.DeepEqual(got, []string{"2026-10-16", "2026-10-17"}) {
		t.Fatalf("dates = %v", got)
	}
	approx(t, "day1", u.Day("2026-10-16").MemoryGiBSeconds, 60)
	approx(t, "day2", u.Day("2026-10-17").MemoryGiBSeconds, 60)
}

// Acceptance 3.
func TestAccounting_ActiveTime(t *testing.T) {
	t.Run("running then suspending", func(t *testing.T) {
		u := NewTaskUsage()
		u.ApplyStateChange(StateChange{Time: at(0), State: "RUNNING"})
		d := u.ApplyStateChange(StateChange{Time: at(300), State: "SUSPENDING"})
		approx(t, "delta", d.Sum().ActiveSeconds, 300)
		approx(t, "ActiveSeconds", u.ActiveSeconds, 300)
		// Replays are ignored.
		if d := u.ApplyStateChange(StateChange{Time: at(0), State: "RUNNING"}); !d.IsZero() {
			t.Errorf("replay produced %+v", d)
		}
		if d := u.ApplyStateChange(StateChange{Time: at(300), State: "SUSPENDING"}); !d.IsZero() {
			t.Errorf("replay produced %+v", d)
		}
		approx(t, "ActiveSeconds", u.ActiveSeconds, 300)
	})
	t.Run("across midnight", func(t *testing.T) {
		u := NewTaskUsage()
		u.ApplyStateChange(StateChange{Time: time.Date(2026, 10, 16, 23, 59, 0, 0, time.UTC), State: "running"})
		d := u.ApplyStateChange(StateChange{Time: time.Date(2026, 10, 17, 0, 1, 0, 0, time.UTC), State: "suspended"})
		approx(t, "delta day1", d.Days["2026-10-16"].ActiveSeconds, 60)
		approx(t, "delta day2", d.Days["2026-10-17"].ActiveSeconds, 60)
		approx(t, "day1", u.Day("2026-10-16").ActiveSeconds, 60)
		approx(t, "day2", u.Day("2026-10-17").ActiveSeconds, 60)
		approx(t, "lifetime", u.ActiveSeconds, 120)
	})
	t.Run("samples accrue open interval", func(t *testing.T) {
		u := NewTaskUsage()
		u.ApplyStateChange(StateChange{Time: at(0), State: "ACTOR_STATE_RUNNING"})
		u.ApplyUsageSample(UsageSample{Time: at(120), Epoch: 1, CPUSeconds: f64(1)}, AccountingConfig{})
		approx(t, "ActiveSeconds after sample", u.ActiveSeconds, 120)
		u.ApplyStateChange(StateChange{Time: at(300), State: "crashed"})
		approx(t, "ActiveSeconds", u.ActiveSeconds, 300)
		// A late sample after close adds no active time.
		u.ApplyUsageSample(UsageSample{Time: at(310), Epoch: 1, CPUSeconds: f64(1)}, AccountingConfig{})
		approx(t, "ActiveSeconds", u.ActiveSeconds, 300)
	})
	t.Run("sample opens interval when state unknown", func(t *testing.T) {
		u := NewTaskUsage()
		u.ApplyUsageSample(UsageSample{Time: at(60), Epoch: 1, CPUSeconds: f64(1)}, AccountingConfig{})
		u.ApplyUsageSample(UsageSample{Time: at(120), Epoch: 1, CPUSeconds: f64(2)}, AccountingConfig{})
		approx(t, "ActiveSeconds", u.ActiveSeconds, 60)
		// The late RUNNING event extends the interval back to its start.
		u.ApplyStateChange(StateChange{Time: at(0), State: "running"})
		approx(t, "ActiveSeconds", u.ActiveSeconds, 120)
		u.ApplyStateChange(StateChange{Time: at(180), State: "suspending"})
		approx(t, "ActiveSeconds", u.ActiveSeconds, 180)
	})
}

// lowCPUSamples returns samples every 60s from start for n intervals with
// cpu growing by perInterval each time.
func applySamples(u *TaskUsage, cfg AccountingConfig, epoch int64, startSec float64, n int, cpu0, perInterval float64) float64 {
	cpu := cpu0
	for i := 0; i <= n; i++ {
		u.ApplyUsageSample(UsageSample{Time: at(startSec + float64(60*i)), Epoch: epoch, CPUSeconds: f64(cpu), WorkingSetBytes: i64(oneGiB)}, cfg)
		if i < n {
			cpu += perInterval
		}
	}
	return cpu
}

// Acceptance 4.
func TestAccounting_Idle(t *testing.T) {
	t.Run("low cpu streak", func(t *testing.T) {
		u := NewTaskUsage()
		// 0.06 cpu-s per 60s = 0.001 vCPU, below 0.02, for 10 minutes.
		cpu := applySamples(u, AccountingConfig{}, 1, 0, 10, 0, 0.06)
		// Idle only after the streak reached 5 min: [300s, 600s].
		approx(t, "IdleSeconds", u.IdleSeconds, 300)
		approx(t, "IdleMemoryGiBSeconds", u.IdleMemoryGiBSeconds, 300)
		approx(t, "IdleCPUSeconds", u.IdleCPUSeconds, 5*0.06)
		// A busy interval breaks the streak.
		cpu += 6 // 0.1 vCPU
		d := u.ApplyUsageSample(UsageSample{Time: at(660), Epoch: 1, CPUSeconds: f64(cpu), WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
		approx(t, "busy idle delta", d.Sum().IdleSeconds, 0)
		// Low again from 660s: idle starts at 960s.
		applySamples(u, AccountingConfig{}, 1, 660, 6, cpu, 0)
		approx(t, "IdleSeconds", u.IdleSeconds, 300+60)
	})
	t.Run("configurable", func(t *testing.T) {
		u := NewTaskUsage()
		cfg := AccountingConfig{IdleCPUThreshold: 0.5, IdleAfter: 2 * time.Minute}
		// 6 cpu-s per 60s = 0.1 vCPU: busy by default, low with threshold 0.5.
		applySamples(u, cfg, 1, 0, 5, 0, 6)
		approx(t, "IdleSeconds", u.IdleSeconds, 180)
		u2 := NewTaskUsage()
		applySamples(u2, AccountingConfig{}, 1, 0, 5, 0, 6)
		approx(t, "IdleSeconds default", u2.IdleSeconds, 0)
	})
	t.Run("command exited", func(t *testing.T) {
		u := NewTaskUsage()
		cpu := applySamples(u, AccountingConfig{}, 1, 0, 1, 0, 30) // busy
		u.ApplyRunnerReport(RunnerReport{Session: "s1", ReportTime: at(90), CommandExited: true, ExitCode: 3}, at(90))
		if !u.State.CommandExited || u.State.ExitCode != 3 {
			t.Fatalf("state = %+v", u.State)
		}
		cpu += 30
		d := u.ApplyUsageSample(UsageSample{Time: at(120), Epoch: 1, CPUSeconds: f64(cpu), WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
		approx(t, "IdleSeconds", d.Sum().IdleSeconds, 60)
		approx(t, "IdleCPUSeconds", d.Sum().IdleCPUSeconds, 30)
		approx(t, "IdleMemoryGiBSeconds", d.Sum().IdleMemoryGiBSeconds, 60)
		// Same session keeps the flag.
		u.ApplyRunnerReport(RunnerReport{Session: "s1", ReportTime: at(150)}, at(150))
		cpu += 30
		d = u.ApplyUsageSample(UsageSample{Time: at(180), Epoch: 1, CPUSeconds: f64(cpu), WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
		approx(t, "IdleSeconds same session", d.Sum().IdleSeconds, 60)
		// A new session clears it.
		u.ApplyRunnerReport(RunnerReport{Session: "s2", ReportTime: at(200)}, at(200))
		cpu += 30
		d = u.ApplyUsageSample(UsageSample{Time: at(240), Epoch: 1, CPUSeconds: f64(cpu), WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
		approx(t, "IdleSeconds new session", d.Sum().IdleSeconds, 0)
		approx(t, "IdleSeconds total", u.IdleSeconds, 120)
	})
}

// Acceptance 5.
func TestAccounting_RunnerTokens(t *testing.T) {
	const m = "gemini-2.5-flash"
	u := NewTaskUsage()
	rep := func(session string, sec float64, toks map[string]RunnerTokens) Delta {
		return u.ApplyRunnerReport(RunnerReport{Session: session, ReportTime: at(sec), Tokens: toks}, at(sec+1))
	}
	tok := func(d Delta, model string) TokenCounts { return d.Sum().Tokens[model] }

	d := rep("A", 0, map[string]RunnerTokens{m: {Input: 100, Output: 10, CacheRead: 5, Reasoning: 2, CostAmount: 0.01, CostCurrency: "USD"}})
	if got, want := tok(d, m), (TokenCounts{Input: 100, Output: 10, CacheRead: 5, Reasoning: 2}); got != want {
		t.Errorf("first report = %+v, want %+v", got, want)
	}
	if got := d.Sum().ProviderCost[m]; got.Currency != "USD" || math.Abs(got.Amount-0.01) > 1e-12 {
		t.Errorf("provider cost = %+v", got)
	}
	d = rep("A", 60, map[string]RunnerTokens{m: {Input: 150, Output: 20, CacheRead: 5, Reasoning: 2, CostAmount: 0.03, CostCurrency: "USD"}})
	if got, want := tok(d, m), (TokenCounts{Input: 50, Output: 10}); got != want {
		t.Errorf("second report = %+v, want %+v", got, want)
	}
	approx(t, "provider cost diff", d.Sum().ProviderCost[m].Amount, 0.02)
	// Replay: nothing.
	if d := rep("A", 60, map[string]RunnerTokens{m: {Input: 150, Output: 20, CacheRead: 5, Reasoning: 2, CostAmount: 0.03, CostCurrency: "USD"}}); !d.IsZero() {
		t.Errorf("replay produced %+v", d.Sum())
	}
	// New session with values >= previous for every model: same counters file.
	d = rep("B", 120, map[string]RunnerTokens{m: {Input: 160, Output: 25, CacheRead: 5, Reasoning: 2, CostAmount: 0.03}, "gemini-2.5-pro": {Input: 7}})
	if got, want := tok(d, m), (TokenCounts{Input: 10, Output: 5}); got != want {
		t.Errorf("continuing session = %+v, want %+v", got, want)
	}
	if got, want := tok(d, "gemini-2.5-pro"), (TokenCounts{Input: 7}); got != want {
		t.Errorf("new model = %+v, want %+v", got, want)
	}
	// New session with lower values: counters file reset, diff against zero.
	d = rep("C", 180, map[string]RunnerTokens{m: {Input: 30, Output: 5}})
	if got, want := tok(d, m), (TokenCounts{Input: 30, Output: 5}); got != want {
		t.Errorf("reset session = %+v, want %+v", got, want)
	}
	if got := tok(d, "gemini-2.5-pro"); !got.IsZero() {
		t.Errorf("reset session pro = %+v", got)
	}
	want := map[string]TokenCounts{
		m:                {Input: 190, Output: 30, CacheRead: 5, Reasoning: 2},
		"gemini-2.5-pro": {Input: 7},
	}
	if !reflect.DeepEqual(u.Tokens, want) {
		t.Errorf("lifetime tokens = %+v, want %+v", u.Tokens, want)
	}
	if !reflect.DeepEqual(u.Day(DateOf(t0)).Tokens, want) {
		t.Errorf("day tokens = %+v, want %+v", u.Day(DateOf(t0)).Tokens, want)
	}
	// A stale (older) report is ignored.
	if d := rep("B", 150, map[string]RunnerTokens{m: {Input: 999}}); !d.IsZero() {
		t.Errorf("stale report produced %+v", d.Sum())
	}
	if !u.State.LastPullTime.Equal(at(181)) {
		t.Errorf("LastPullTime = %v", u.State.LastPullTime)
	}
}

// Acceptance 6.
func TestAccounting_StorageGiBSeconds(t *testing.T) {
	u := NewTaskUsage()
	u.ApplyRunnerReport(RunnerReport{Session: "s", ReportTime: at(0), WorkspaceBytes: 2 * oneGiB}, at(0))
	d := u.ApplyRunnerReport(RunnerReport{Session: "s", ReportTime: at(3600), WorkspaceBytes: 2 * oneGiB}, at(3600))
	approx(t, "delta", d.Sum().StorageGiBSeconds, 7200)
	approx(t, "StorageGiBSeconds", u.StorageGiBSeconds, 7200)
	if u.WorkspaceBytes != 2*oneGiB {
		t.Errorf("WorkspaceBytes = %d", u.WorkspaceBytes)
	}
	// State transitions also accrue storage (e.g. while suspended).
	u.ApplyStateChange(StateChange{Time: at(7200), State: "suspended"})
	approx(t, "StorageGiBSeconds", u.StorageGiBSeconds, 14400)
}

// Acceptance 7.
func TestAccounting_ActorUIDGuard(t *testing.T) {
	u := NewTaskUsage()
	u.ApplyStateChange(StateChange{Time: at(0), ActorUID: "uid-a", State: "running"})
	if u.State.ActorUID != "uid-a" {
		t.Fatalf("ActorUID = %q", u.State.ActorUID)
	}
	before, _ := json.Marshal(u)
	if d := u.ApplyUsageSample(UsageSample{Time: at(60), ActorUID: "uid-b", Epoch: 1, CPUSeconds: f64(9)}, AccountingConfig{}); !d.IsZero() {
		t.Errorf("foreign sample produced %+v", d.Sum())
	}
	if d := u.ApplyStateChange(StateChange{Time: at(120), ActorUID: "uid-b", State: "suspended"}); !d.IsZero() {
		t.Errorf("foreign state change produced %+v", d.Sum())
	}
	after, _ := json.Marshal(u)
	if string(before) != string(after) {
		t.Errorf("foreign events changed usage:\n%s\n%s", before, after)
	}
	d := u.ApplyUsageSample(UsageSample{Time: at(60), ActorUID: "uid-a", Epoch: 1, CPUSeconds: f64(2)}, AccountingConfig{})
	approx(t, "own sample cpu", d.Sum().CPUSeconds, 2)
}

// Acceptance 9.
func TestToStatus(t *testing.T) {
	if ToStatus(nil) != nil {
		t.Error("ToStatus(nil) != nil")
	}
	u := NewTaskUsage()
	u.ActiveSeconds = 90.5
	u.IdleSeconds = 30
	u.CPUSeconds = 12.25
	u.MemoryGiBSeconds = 64
	u.WorkspaceBytes = 2048
	u.Tokens = map[string]TokenCounts{
		"gemini-2.5-pro":   {Input: 1, Output: 2},
		"gemini-2.5-flash": {Input: 10, Output: 20, CacheRead: 3, CacheWrite: 4, Reasoning: 5},
		"empty":            {},
	}
	u.Cost = Cost{Currency: "USD", Total: 1.5, Compute: 1, Tokens: 0.4, Storage: 0.1, Idle: 0.2, TokensPartiallyUnpriced: true}
	u.UpdateTime = t0
	s := ToStatus(u)
	if got := s.GetActiveDuration().AsDuration(); got != 90500*time.Millisecond {
		t.Errorf("ActiveDuration = %v", got)
	}
	if got := s.GetIdleDuration().AsDuration(); got != 30*time.Second {
		t.Errorf("IdleDuration = %v", got)
	}
	if s.GetCpuSeconds() != 12.25 || s.GetMemoryGibSeconds() != 64 || s.GetWorkspaceBytes() != 2048 {
		t.Errorf("scalars = %v %v %v", s.GetCpuSeconds(), s.GetMemoryGibSeconds(), s.GetWorkspaceBytes())
	}
	if len(s.GetTokens()) != 2 {
		t.Fatalf("tokens = %v", s.GetTokens())
	}
	f, p := s.GetTokens()[0], s.GetTokens()[1]
	if f.GetModel() != "gemini-2.5-flash" || f.GetInput() != 10 || f.GetOutput() != 20 || f.GetCacheRead() != 3 || f.GetCacheWrite() != 4 || f.GetReasoning() != 5 {
		t.Errorf("tokens[0] = %v", f)
	}
	if p.GetModel() != "gemini-2.5-pro" || p.GetInput() != 1 || p.GetOutput() != 2 {
		t.Errorf("tokens[1] = %v", p)
	}
	c := s.GetCost()
	if c.GetCurrency() != "USD" || c.GetTotal() != 1.5 || c.GetCompute() != 1 || c.GetTokens() != 0.4 ||
		c.GetStorage() != 0.1 || c.GetIdle() != 0.2 || !c.GetTokensPartiallyUnpriced() {
		t.Errorf("cost = %v", c)
	}
	if !s.GetUpdateTime().AsTime().Equal(t0) {
		t.Errorf("UpdateTime = %v", s.GetUpdateTime())
	}
}

type fakePricer struct{ calls []Delta }

// Price charges $1 per active hour and $0.5 per CPU-second, plus provider cost.
func (p *fakePricer) Price(d Delta, pool string) Cost {
	p.calls = append(p.calls, d)
	s := d.Sum()
	c := Cost{Currency: "USD", Compute: s.ActiveSeconds/3600 + 0.5*s.CPUSeconds}
	for _, m := range s.ProviderCost {
		c.Tokens += m.Amount
	}
	c.Total = c.Compute + c.Tokens
	return c
}

func TestChargeDelta_PricesPerDay(t *testing.T) {
	u := NewTaskUsage()
	u.ApplyStateChange(StateChange{Time: time.Date(2026, 10, 16, 23, 0, 0, 0, time.UTC), State: "running"})
	d := u.ApplyStateChange(StateChange{Time: time.Date(2026, 10, 17, 1, 0, 0, 0, time.UTC), State: "suspended"})
	p := &fakePricer{}
	c := u.ChargeDelta(d, p, "pool-a")
	if len(p.calls) != 2 {
		t.Fatalf("Price called %d times, want once per day", len(p.calls))
	}
	for _, call := range p.calls {
		if len(call.Days) != 1 {
			t.Errorf("Price got %d days, want 1", len(call.Days))
		}
	}
	approx(t, "total", c.Total, 2)
	approx(t, "lifetime", u.Cost.Total, 2)
	approx(t, "day1", u.Day("2026-10-16").Cost.Total, 1)
	approx(t, "day2", u.Day("2026-10-17").Cost.Compute, 1)
	if u.Cost.Currency != "USD" {
		t.Errorf("currency = %q", u.Cost.Currency)
	}
	if c := u.ChargeDelta(d, nil, ""); c != (Cost{}) {
		t.Errorf("nil pricer cost = %+v", c)
	}
}

func TestDelta_IsZeroAndSum(t *testing.T) {
	var d Delta
	if !d.IsZero() {
		t.Error("empty delta not zero")
	}
	d.day("2026-10-16")
	if !d.IsZero() {
		t.Error("delta with empty day not zero")
	}
	d.day("2026-10-16").Tokens = map[string]TokenCounts{"m": {}}
	if !d.IsZero() {
		t.Error("delta with zero tokens not zero")
	}
	d.day("2026-10-17").ProviderCost = map[string]Money{"m": {Currency: "USD", Amount: 0.1}}
	if d.IsZero() {
		t.Error("delta with provider cost is zero")
	}
	d.day("2026-10-16").CPUSeconds = 2
	d.day("2026-10-17").CPUSeconds = 3
	s := d.Sum()
	approx(t, "sum cpu", s.CPUSeconds, 5)
	approx(t, "sum provider", s.ProviderCost["m"].Amount, 0.1)
	if got := d.Dates(); !reflect.DeepEqual(got, []string{"2026-10-16", "2026-10-17"}) {
		t.Errorf("Dates = %v", got)
	}
}

// The store serializes the record between every event; accounting must give
// the same result with a JSON round trip after each step.
func TestAccounting_JSONRoundTripEquivalence(t *testing.T) {
	type step func(u *TaskUsage) Delta
	e := t0.UnixNano()
	steps := []step{
		func(u *TaskUsage) Delta {
			return u.ApplyStateChange(StateChange{Time: at(0), ActorUID: "a", State: "running"})
		},
		func(u *TaskUsage) Delta {
			return u.ApplyRunnerReport(RunnerReport{Session: "s", ReportTime: at(10), WorkspaceBytes: oneGiB, Tokens: map[string]RunnerTokens{"m": {Input: 5}}}, at(10))
		},
	}
	for i := range 12 {
		cpu := 0.01 * float64(i)
		steps = append(steps, func(u *TaskUsage) Delta {
			return u.ApplyUsageSample(UsageSample{Time: at(float64(60 * i)), ActorUID: "a", Epoch: e, CPUSeconds: f64(cpu), WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
		})
	}
	steps = append(steps,
		func(u *TaskUsage) Delta {
			return u.ApplyRunnerReport(RunnerReport{Session: "s", ReportTime: at(700), CommandExited: true, WorkspaceBytes: oneGiB, Tokens: map[string]RunnerTokens{"m": {Input: 9}}}, at(700))
		},
		func(u *TaskUsage) Delta {
			return u.ApplyUsageSample(UsageSample{Time: at(720), ActorUID: "a", Epoch: e, CPUSeconds: f64(5), WorkingSetBytes: i64(oneGiB)}, AccountingConfig{})
		},
		func(u *TaskUsage) Delta {
			return u.ApplyStateChange(StateChange{Time: at(800), ActorUID: "a", State: "suspended"})
		},
	)
	direct := NewTaskUsage()
	rt := NewTaskUsage()
	for i, s := range steps {
		d1 := s(direct)
		d2 := s(rt)
		if !reflect.DeepEqual(d1, d2) {
			t.Fatalf("step %d: delta %+v != %+v", i, d1, d2)
		}
		b, err := marshalUsage(rt)
		if err != nil {
			t.Fatal(err)
		}
		if rt, err = unmarshalUsage(b); err != nil {
			t.Fatal(err)
		}
	}
	b1, _ := json.Marshal(direct)
	b2, _ := json.Marshal(rt)
	if string(b1) != string(b2) {
		t.Errorf("round-trip result differs:\n%s\n%s", b1, b2)
	}
	approx(t, "ActiveSeconds", direct.ActiveSeconds, 800)
	approx(t, "CPUSeconds", direct.CPUSeconds, 5)
	if direct.IdleSeconds <= 0 {
		t.Errorf("IdleSeconds = %v, want > 0", direct.IdleSeconds)
	}
}

func TestAccounting_EpochPruning(t *testing.T) {
	u := NewTaskUsage()
	for e := int64(1); e <= maxEpochs+3; e++ {
		u.ApplyUsageSample(UsageSample{Time: at(float64(100 * e)), Epoch: e, CPUSeconds: f64(1)}, AccountingConfig{})
	}
	approx(t, "CPUSeconds", u.CPUSeconds, maxEpochs+3)
	if len(u.State.EpochMaxCPU) > maxEpochs {
		t.Errorf("kept %d epochs", len(u.State.EpochMaxCPU))
	}
	// A replay of a pruned epoch must not count again.
	if d := u.ApplyUsageSample(UsageSample{Time: at(100), Epoch: 1, CPUSeconds: f64(1)}, AccountingConfig{}); !d.IsZero() {
		t.Errorf("pruned epoch replay produced %+v", d.Sum())
	}
}

// Regression (verifier D1): samples arriving before the late state events
// must not double count active time.
//
// Arrival order: samples at 10s, 70s, 130s (state unknown, so the first
// sample opens the interval at 10s and the next two accrue [10s, 130s]); then
// the late events resuming@0s (closes; nothing to add) and running@8s; then
// sample@190s and suspending@200s. The actor was really RUNNING from 8s to
// 200s, so the exact answer is 200 - 8 = 192s: [10,130] from the samples,
// [8,10] added when the late RUNNING extends the covered range back,
// [130,190] from the sample and [190,200] at close. Before the fix this gave
// 312s, because reopening at 8s reset the accrual mark and recounted [8,130].
func TestAccounting_ActiveLateStateEventsNoDoubleCount(t *testing.T) {
	u := NewTaskUsage()
	cfg := AccountingConfig{}
	for _, sec := range []float64{10, 70, 130} {
		u.ApplyUsageSample(UsageSample{Time: at(sec), Epoch: 1, CPUSeconds: f64(sec / 10)}, cfg)
	}
	u.ApplyStateChange(StateChange{Time: at(0), State: "resuming"})
	u.ApplyStateChange(StateChange{Time: at(8), State: "running"})
	u.ApplyUsageSample(UsageSample{Time: at(190), Epoch: 1, CPUSeconds: f64(19)}, cfg)
	u.ApplyStateChange(StateChange{Time: at(200), State: "suspending"})
	approx(t, "ActiveSeconds", u.ActiveSeconds, 192)
	approx(t, "day ActiveSeconds", u.Day(DateOf(t0)).ActiveSeconds, 192)

	// A later activation (past the high-water mark) starts a fresh range.
	u.ApplyStateChange(StateChange{Time: at(1000), State: "running"})
	u.ApplyStateChange(StateChange{Time: at(1100), State: "suspended"})
	approx(t, "ActiveSeconds after second activation", u.ActiveSeconds, 292)
}

// Regression (verifier D2): a late sample of an older epoch must not start
// the low-cpu streak of the current epoch.
func TestAccounting_IdleStreakIgnoresOlderEpochs(t *testing.T) {
	u := NewTaskUsage()
	cfg := AccountingConfig{}
	e1, e2 := int64(1000), int64(2000)
	u.ApplyUsageSample(UsageSample{Time: at(0), Epoch: e1, CPUSeconds: f64(0), WorkingSetBytes: i64(oneGiB)}, cfg)
	// E2 starts at 710s.
	u.ApplyUsageSample(UsageSample{Time: at(710), Epoch: e2, CPUSeconds: f64(0), WorkingSetBytes: i64(oneGiB)}, cfg)
	// E1's final sample (700s, low cpu over [0,700]) arrives late.
	d := u.ApplyUsageSample(UsageSample{Time: at(700), Epoch: e1, Kind: "final", CPUSeconds: f64(0.1), WorkingSetBytes: i64(oneGiB)}, cfg)
	approx(t, "late E1 cpu", d.Sum().CPUSeconds, 0.1)
	approx(t, "late E1 memory (capped 120s)", d.Sum().MemoryGiBSeconds, 120)
	approx(t, "late E1 idle", d.Sum().IdleSeconds, 0)
	if !u.State.IdleStreakStart.IsZero() {
		t.Errorf("IdleStreakStart = %v, want zero", u.State.IdleStreakStart)
	}
	// E2's first low interval [710,770] is only the start of a streak.
	d = u.ApplyUsageSample(UsageSample{Time: at(770), Epoch: e2, CPUSeconds: f64(0), WorkingSetBytes: i64(oneGiB)}, cfg)
	approx(t, "E2 first low interval idle", d.Sum().IdleSeconds, 0)
	if !u.State.IdleStreakStart.Equal(at(710)) {
		t.Errorf("IdleStreakStart = %v, want %v", u.State.IdleStreakStart, at(710))
	}
}

// Apply* may advance state with a zero Delta; callers must still persist.
func TestAccounting_ZeroDeltaStillAdvancesState(t *testing.T) {
	u := NewTaskUsage()
	if d := u.ApplyStateChange(StateChange{Time: at(0), ActorUID: "a", State: "running"}); !d.IsZero() {
		t.Fatalf("first RUNNING delta = %+v", d.Sum())
	}
	if u.State.ActorUID != "a" || u.State.ActiveSince.IsZero() {
		t.Errorf("state not advanced: %+v", u.State)
	}
	if d := u.ApplyRunnerReport(RunnerReport{Session: "s", ReportTime: at(5)}, at(6)); !d.IsZero() {
		t.Fatalf("empty report delta = %+v", d.Sum())
	}
	if u.State.RunnerSession != "s" || !u.State.LastPullTime.Equal(at(6)) {
		t.Errorf("runner state not advanced: %+v", u.State)
	}
}
