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

package budget

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

var t0 = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func newTask(b *v1alpha1.BudgetSpec, phase string) *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t1", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Budget: b},
		Status:   &v1alpha1.TaskStatus{Phase: phase},
	}
}

// sim drives Evaluate + MergeConditions the way ax-server does on the delta
// path: persist (count an update) only when MergeConditions reports a change,
// and emit crossings only then.
type sim struct {
	t         *testing.T
	task      *v1alpha1.Task
	cfg       Config
	updates   int
	crossings []Crossing
}

func (s *sim) step(u *usage.TaskUsage, now time.Time) Result {
	s.t.Helper()
	res := Evaluate(s.task, u, s.cfg, now)
	merged, changed := MergeConditions(s.task.GetStatus().GetConditions(), res.Conditions())
	if len(res.Crossings) > 0 && !changed {
		s.t.Fatalf("crossings %v without a condition change", res.Crossings)
	}
	if changed {
		s.updates++
		s.task.Status.Conditions = merged
		s.crossings = append(s.crossings, res.Crossings...)
	}
	return res
}

func cond(t *testing.T, conds []*v1alpha1.Condition, typ string) *v1alpha1.Condition {
	t.Helper()
	for _, c := range conds {
		if c.GetType() == typ {
			return c
		}
	}
	return nil
}

func status(c *v1alpha1.Condition) string {
	if c == nil {
		return "absent"
	}
	return c.GetStatus()
}

func usageWith(dim string, frac float64) *usage.TaskUsage {
	u := usage.NewTaskUsage()
	switch dim {
	case DimensionActiveDeadline:
		u.ActiveSeconds = 1800 * frac
	case DimensionMaxTokens:
		// 100k limit, split across two models and token kinds.
		n := int64(100000 * frac)
		u.Tokens = map[string]usage.TokenCounts{
			"gemini-2.5-flash": {Input: n / 2, Output: n / 4, CacheRead: n / 8, Reasoning: n / 16},
			"gemini-2.5-pro":   {Input: n - n/2 - n/4, Output: 0},
		}
	case DimensionMaxCost:
		u.Cost = usage.Cost{Currency: "USD", Total: 1.0 * frac}
	}
	return u
}

func budgetFor(dim string) *v1alpha1.BudgetSpec {
	switch dim {
	case DimensionActiveDeadline:
		return &v1alpha1.BudgetSpec{ActiveDeadline: durationpb.New(30 * time.Minute)}
	case DimensionMaxTokens:
		return &v1alpha1.BudgetSpec{MaxTokens: 100000}
	default:
		return &v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Currency: "USD", Amount: 1.0}}
	}
}

// Acceptance 1: crossing 80% then 100% for each dimension, each transition
// recorded once (no duplicate updates on repeated deltas).
func TestBudget_CrossWarnThenExceededPerDimension(t *testing.T) {
	steps := []struct {
		frac               float64
		warning, exceeded  string
		updates, crossings int
	}{
		{0.10, "absent", "absent", 0, 0},
		{0.50, "absent", "absent", 0, 0},
		{0.79, "absent", "absent", 0, 0},
		{0.80, "True", "absent", 1, 1},
		{0.83, "True", "absent", 1, 1},
		{0.83, "True", "absent", 1, 1}, // repeated delta
		{0.99, "True", "absent", 1, 1},
		{1.00, "False", "True", 2, 2},
		{1.30, "False", "True", 2, 2},
		{1.30, "False", "True", 2, 2},
	}
	for _, dim := range []string{DimensionActiveDeadline, DimensionMaxTokens, DimensionMaxCost} {
		t.Run(dim, func(t *testing.T) {
			s := &sim{t: t, task: newTask(budgetFor(dim), "Running")}
			for i, st := range steps {
				now := t0.Add(time.Duration(i) * time.Minute)
				s.step(usageWith(dim, st.frac), now)
				conds := s.task.Status.Conditions
				if got := status(cond(t, conds, v1alpha1.ConditionBudgetWarning)); got != st.warning {
					t.Errorf("step %d (%.0f%%): BudgetWarning = %s, want %s", i, st.frac*100, got, st.warning)
				}
				if got := status(cond(t, conds, v1alpha1.ConditionBudgetExceeded)); got != st.exceeded {
					t.Errorf("step %d (%.0f%%): BudgetExceeded = %s, want %s", i, st.frac*100, got, st.exceeded)
				}
				if s.updates != st.updates || len(s.crossings) != st.crossings {
					t.Errorf("step %d (%.0f%%): updates=%d crossings=%d, want %d/%d", i, st.frac*100, s.updates, len(s.crossings), st.updates, st.crossings)
				}
			}
			if len(s.crossings) != 2 {
				t.Fatalf("crossings = %v", s.crossings)
			}
			if c := s.crossings[0]; c.Dimension != dim || c.Level != LevelWarning {
				t.Errorf("crossing[0] = %+v, want %s warning", c, dim)
			}
			if c := s.crossings[1]; c.Dimension != dim || c.Level != LevelExceeded || c.Percent < 100 {
				t.Errorf("crossing[1] = %+v, want %s exceeded", c, dim)
			}
			conds := s.task.Status.Conditions
			ex := cond(t, conds, v1alpha1.ConditionBudgetExceeded)
			wantReason := reasonPrefix(dim) + "Exceeded"
			if ex.GetReason() != wantReason {
				t.Errorf("BudgetExceeded reason = %q, want %q", ex.GetReason(), wantReason)
			}
			// Exceeded was set at step 7 and kept its transition time.
			if got := ex.GetLastTransitionTime().AsTime(); !got.Equal(t0.Add(7 * time.Minute)) {
				t.Errorf("BudgetExceeded lastTransitionTime = %v, want %v", got, t0.Add(7*time.Minute))
			}
			if w := cond(t, conds, v1alpha1.ConditionBudgetWarning); w.GetReason() != ReasonBudgetExceeded {
				t.Errorf("BudgetWarning reason = %q, want %q", w.GetReason(), ReasonBudgetExceeded)
			}
		})
	}
}

// Acceptance 1: no budget → no budget conditions.
func TestBudget_NoBudgetNoConditions(t *testing.T) {
	big := usage.NewTaskUsage()
	big.ActiveSeconds = 1e6
	big.Tokens = map[string]usage.TokenCounts{"m": {Input: 1e9}}
	big.Cost = usage.Cost{Currency: "USD", Total: 1e4}
	for name, b := range map[string]*v1alpha1.BudgetSpec{
		"nil":            nil,
		"empty":          {},
		"warnAtOnly":     {WarnAtPercent: 50},
		"zeroLimits":     {ActiveDeadline: durationpb.New(0), MaxCost: &v1alpha1.Money{Currency: "USD"}},
		"zeroCostAmount": {MaxCost: &v1alpha1.Money{}},
	} {
		t.Run(name, func(t *testing.T) {
			task := newTask(b, "Running")
			res := Evaluate(task, big, Config{}, t0)
			if res.BudgetWarning != nil || res.BudgetExceeded != nil || len(res.Dimensions) != 0 || len(res.Crossings) != 0 {
				t.Fatalf("Evaluate = %+v, want no budget result", res)
			}
			merged, changed := MergeConditions(nil, res.Conditions())
			if changed || len(merged) != 0 {
				t.Errorf("merged = %v changed=%v, want none", merged, changed)
			}
		})
	}
	// Nil usage with a budget: everything at 0%, no conditions added.
	res := Evaluate(newTask(budgetFor(DimensionMaxCost), "Running"), nil, Config{}, t0)
	if merged, changed := MergeConditions(nil, res.Conditions()); changed || len(merged) != 0 {
		t.Errorf("nil usage: merged = %v changed=%v", merged, changed)
	}
}

func TestBudget_Messages(t *testing.T) {
	b := &v1alpha1.BudgetSpec{
		ActiveDeadline: durationpb.New(30 * time.Minute),
		MaxTokens:      100000,
		MaxCost:        &v1alpha1.Money{Currency: "USD", Amount: 1},
	}
	u := usage.NewTaskUsage()
	u.ActiveSeconds = 1500
	u.Tokens = map[string]usage.TokenCounts{"a": {Input: 60000, Output: 25000, CacheRead: 50000, Reasoning: 10000}}
	u.Cost = usage.Cost{Currency: "USD", Total: 0.83}
	res := Evaluate(newTask(b, "Running"), u, Config{}, t0)
	want := []string{"active 25m0s of 30m0s (83%)", "tokens 85,000 of 100,000 (85%)", "cost $0.83 of $1.00 (83%)"}
	for i, d := range res.Dimensions {
		if d.Message != want[i] || d.Level != LevelWarning {
			t.Errorf("dimension %d = %q (%s), want %q warning", i, d.Message, d.Level, want[i])
		}
	}
	w := res.BudgetWarning
	if w.GetStatus() != StatusTrue || w.GetReason() != "ActiveDeadlineWarning" || w.GetMessage() != strings.Join(want, "; ") {
		t.Errorf("BudgetWarning = %v", w)
	}
	if res.BudgetExceeded.GetStatus() != StatusFalse || res.BudgetExceeded.GetReason() != ReasonWithinBudget {
		t.Errorf("BudgetExceeded = %v", res.BudgetExceeded)
	}
	if len(res.Crossings) != 3 {
		t.Errorf("crossings = %v, want 3", res.Crossings)
	}

	// Small amounts get 4 decimals; just under the limit never says 100%.
	u.Cost.Total = 0.0041
	res = Evaluate(newTask(&v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Amount: 0.05}}, "Running"), u, Config{}, t0)
	if got := res.Dimensions[0].Message; got != "cost $0.0041 of $0.05 (8%)" {
		t.Errorf("small cost message = %q", got)
	}
	u.Cost.Total = 0.99999999
	res = Evaluate(newTask(&v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Amount: 1}}, "Running"), u, Config{}, t0)
	if got := res.Dimensions[0]; got.Level != LevelWarning || !strings.HasSuffix(got.Message, "(99%)") {
		t.Errorf("just-under message = %+v", got)
	}
}

func TestBudget_WarnAtPercent(t *testing.T) {
	u := usageWith(DimensionMaxCost, 0.6)
	cases := []struct {
		name     string
		specWarn int32
		cfg      Config
		want     Level
	}{
		{"default 80", 0, Config{}, LevelOK},
		{"card default 50", 0, Config{DefaultWarnAtPercent: 50}, LevelWarning},
		{"spec 60 overrides card 90", 60, Config{DefaultWarnAtPercent: 90}, LevelWarning},
		{"spec 70", 70, Config{DefaultWarnAtPercent: 50}, LevelOK},
	}
	for _, tc := range cases {
		b := budgetFor(DimensionMaxCost)
		b.WarnAtPercent = tc.specWarn
		res := Evaluate(newTask(b, "Running"), u, tc.cfg, t0)
		if got := res.Dimensions[0].Level; got != tc.want {
			t.Errorf("%s: level = %s, want %s", tc.name, got, tc.want)
		}
	}
	card, err := cost.Parse([]byte("budgets: { defaultWarnAtPercent: 55 }\nidle: { cpuThreshold: 0.05, after: 2m }\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg := ConfigFromRateCard(card); cfg.DefaultWarnAtPercent != 55 || cfg.IdleAfter != 2*time.Minute || cfg.IdleCPUThreshold != 0.05 || cfg.Currency != "USD" {
		t.Errorf("ConfigFromRateCard = %+v", cfg)
	}
}

// A second dimension joining an already-True level is persisted once and
// crosses once; jumping straight past the limit yields only an exceeded crossing.
func TestBudget_MultiDimension(t *testing.T) {
	b := &v1alpha1.BudgetSpec{MaxTokens: 100000, MaxCost: &v1alpha1.Money{Amount: 1}}
	s := &sim{t: t, task: newTask(b, "Running")}
	u := usage.NewTaskUsage()
	u.Tokens = map[string]usage.TokenCounts{"m": {Input: 90000}}
	u.Cost.Total = 0.1
	s.step(u, t0)
	if s.updates != 1 || len(s.crossings) != 1 || s.crossings[0].Dimension != DimensionMaxTokens {
		t.Fatalf("after tokens warning: updates=%d crossings=%v", s.updates, s.crossings)
	}
	u.Cost.Total = 0.85
	s.step(u, t0.Add(time.Minute))
	s.step(u, t0.Add(2*time.Minute))
	u.Cost.Total = 0.9
	s.step(u, t0.Add(3*time.Minute))
	if s.updates != 2 || len(s.crossings) != 2 || s.crossings[1].Dimension != DimensionMaxCost || s.crossings[1].Level != LevelWarning {
		t.Fatalf("after cost warning: updates=%d crossings=%v", s.updates, s.crossings)
	}
	w := cond(t, s.task.Status.Conditions, v1alpha1.ConditionBudgetWarning)
	// The stored message is the one persisted at the transition (85%); later
	// messages only appear in read-time merges.
	if w.GetReason() != "MaxTokensWarning" || !strings.Contains(w.GetMessage(), "cost $0.85 of $1.00 (85%)") {
		t.Errorf("BudgetWarning = %v", w)
	}
	if !w.GetLastTransitionTime().AsTime().Equal(t0) {
		t.Errorf("BudgetWarning lastTransitionTime = %v, want %v", w.GetLastTransitionTime().AsTime(), t0)
	}
	// Tokens jump past the limit: warning stays True (cost), exceeded added.
	u.Tokens = map[string]usage.TokenCounts{"m": {Input: 150000}}
	s.step(u, t0.Add(4*time.Minute))
	if s.updates != 3 || len(s.crossings) != 3 || s.crossings[2].Dimension != DimensionMaxTokens || s.crossings[2].Level != LevelExceeded {
		t.Fatalf("after tokens exceeded: updates=%d crossings=%v", s.updates, s.crossings)
	}
	w = cond(t, s.task.Status.Conditions, v1alpha1.ConditionBudgetWarning)
	if w.GetStatus() != StatusTrue || w.GetReason() != "MaxCostWarning" {
		t.Errorf("BudgetWarning = %v, want True MaxCostWarning", w)
	}

	// Straight from ok to exceeded: one exceeded crossing, no warning crossing.
	s2 := &sim{t: t, task: newTask(budgetFor(DimensionMaxCost), "Running")}
	s2.step(usageWith(DimensionMaxCost, 0.1), t0)
	s2.step(usageWith(DimensionMaxCost, 2), t0.Add(time.Minute))
	if s2.updates != 1 || len(s2.crossings) != 1 || s2.crossings[0].Level != LevelExceeded {
		t.Errorf("jump: updates=%d crossings=%v", s2.updates, s2.crossings)
	}
	if got := status(cond(t, s2.task.Status.Conditions, v1alpha1.ConditionBudgetWarning)); got != "absent" {
		t.Errorf("jump: BudgetWarning = %s, want absent", got)
	}
}

// idleUsage drives the real accounting: one sample per minute with cpu
// advancing by cpuPerMin seconds.
func idleUsage(t *testing.T, u *usage.TaskUsage, from, n int, cpuPerMin float64) {
	t.Helper()
	ws := int64(1 << 30)
	for i := from; i < from+n; i++ {
		var cpu float64
		if m, ok := u.State.EpochMaxCPU[1]; ok {
			cpu = m + cpuPerMin
		}
		u.ApplyUsageSample(usage.UsageSample{
			Time: t0.Add(time.Duration(i) * time.Minute), ActorUID: "uid", Epoch: 1, CPUSeconds: &cpu, WorkingSetBytes: &ws,
		}, usage.AccountingConfig{})
	}
}

// Acceptance 2: Idle set and cleared appropriately.
func TestIdle_SetAndCleared(t *testing.T) {
	s := &sim{t: t, task: newTask(nil, "Running")}
	u := usage.NewTaskUsage()

	// Busy for 10 minutes: not idle, no condition.
	idleUsage(t, u, 0, 11, 30)
	s.step(u, t0.Add(10*time.Minute))
	if c := cond(t, s.task.Status.Conditions, v1alpha1.ConditionIdle); c != nil || s.updates != 0 {
		t.Fatalf("busy: Idle = %v updates=%d", c, s.updates)
	}

	// Low cpu for 4 minutes: streak below 5m, still not idle.
	idleUsage(t, u, 11, 4, 0.1)
	if got := IdleStreak(u); got != 4*time.Minute {
		t.Fatalf("IdleStreak = %v, want 4m", got)
	}
	s.step(u, t0.Add(14*time.Minute))
	if c := cond(t, s.task.Status.Conditions, v1alpha1.ConditionIdle); c != nil {
		t.Fatalf("short streak: Idle = %v", c)
	}

	// Two more low minutes: streak 6m → Idle True (LowCPU).
	idleUsage(t, u, 15, 2, 0.1)
	u.Cost.Idle = 0.0012
	s.step(u, t0.Add(16*time.Minute))
	c := cond(t, s.task.Status.Conditions, v1alpha1.ConditionIdle)
	if c.GetStatus() != StatusTrue || c.GetReason() != ReasonLowCPU || s.updates != 1 {
		t.Fatalf("idle: Idle = %v updates=%d", c, s.updates)
	}
	if want := "cpu below 0.02 vCPU for 6m0s; idle 1m0s total, idle cost $0.0012"; c.GetMessage() != want {
		t.Errorf("idle message = %q, want %q", c.GetMessage(), want)
	}
	// Still idle a minute later: message refreshes, no new update, time kept.
	idleUsage(t, u, 17, 1, 0.1)
	res := s.step(u, t0.Add(17*time.Minute))
	if s.updates != 1 || !strings.Contains(res.Idle.GetMessage(), "for 7m0s; idle 2m0s total") {
		t.Errorf("still idle: updates=%d msg=%q", s.updates, res.Idle.GetMessage())
	}
	merged, _ := MergeConditions(s.task.Status.Conditions, res.Conditions())
	if got := cond(t, merged, v1alpha1.ConditionIdle).GetLastTransitionTime().AsTime(); !got.Equal(t0.Add(16 * time.Minute)) {
		t.Errorf("Idle lastTransitionTime = %v, want %v", got, t0.Add(16*time.Minute))
	}

	// Busy again: Idle False (Active).
	idleUsage(t, u, 18, 1, 30)
	s.step(u, t0.Add(18*time.Minute))
	c = cond(t, s.task.Status.Conditions, v1alpha1.ConditionIdle)
	if c.GetStatus() != StatusFalse || c.GetReason() != ReasonActive || s.updates != 2 {
		t.Fatalf("busy again: Idle = %v updates=%d", c, s.updates)
	}

	// Command exited: Idle True immediately (CommandExited).
	u.ApplyRunnerReport(usage.RunnerReport{Session: "s1", ReportTime: t0.Add(19 * time.Minute), CommandExited: true, ExitCode: 3}, t0.Add(19*time.Minute))
	s.step(u, t0.Add(19*time.Minute))
	c = cond(t, s.task.Status.Conditions, v1alpha1.ConditionIdle)
	if c.GetStatus() != StatusTrue || c.GetReason() != ReasonCommandExited || !strings.HasPrefix(c.GetMessage(), "command exited (code 3); idle ") || s.updates != 3 {
		t.Fatalf("exited: Idle = %v updates=%d", c, s.updates)
	}

	// Task suspended: Idle False (NotRunning) even though the command exited.
	s.task.Status.Phase = "Suspended"
	s.step(u, t0.Add(20*time.Minute))
	c = cond(t, s.task.Status.Conditions, v1alpha1.ConditionIdle)
	if c.GetStatus() != StatusFalse || c.GetReason() != ReasonNotRunning || s.updates != 4 {
		t.Fatalf("suspended: Idle = %v updates=%d", c, s.updates)
	}
	if !c.GetLastTransitionTime().AsTime().Equal(t0.Add(20 * time.Minute)) {
		t.Errorf("suspended: lastTransitionTime = %v", c.GetLastTransitionTime().AsTime())
	}
	// A non-running task that never idled gets no Idle condition.
	if merged, changed := MergeConditions(nil, Evaluate(newTask(nil, "Suspended"), u, Config{}, t0).Conditions()); changed || len(merged) != 0 {
		t.Errorf("never idle: merged = %v changed=%v", merged, changed)
	}
	// Custom IdleAfter from the card.
	short := usage.NewTaskUsage()
	idleUsage(t, short, 0, 3, 0)
	if res := Evaluate(newTask(nil, "Running"), short, Config{IdleAfter: 2 * time.Minute}, t0); res.Idle.GetStatus() != StatusTrue {
		t.Errorf("IdleAfter=2m: Idle = %v", res.Idle)
	}
}

func TestMergeConditions(t *testing.T) {
	old := timestamppb.New(t0)
	existing := []*v1alpha1.Condition{
		{Type: "Ready", Status: "True", Reason: "TaskRunning", LastTransitionTime: old},
		{Type: v1alpha1.ConditionBudgetWarning, Status: "True", Reason: "MaxCostWarning", Message: "cost $0.80 of $1.00 (80%)", LastTransitionTime: old},
	}
	snapshot := make([]*v1alpha1.Condition, len(existing))
	for i, c := range existing {
		snapshot[i] = proto.Clone(c).(*v1alpha1.Condition)
	}
	now := timestamppb.New(t0.Add(time.Hour))

	// Message-only change: merged carries it, not a transition, time kept.
	merged, changed := MergeConditions(existing, []*v1alpha1.Condition{
		{Type: v1alpha1.ConditionBudgetWarning, Status: "True", Reason: "MaxCostWarning", Message: "cost $0.85 of $1.00 (85%)", LastTransitionTime: now},
		{Type: v1alpha1.ConditionIdle, Status: "False", Reason: ReasonActive, LastTransitionTime: now},
	})
	if changed {
		t.Error("message-only change reported as changed")
	}
	if len(merged) != 2 || merged[0].GetType() != "Ready" || merged[1].GetMessage() != "cost $0.85 of $1.00 (85%)" || !merged[1].GetLastTransitionTime().AsTime().Equal(t0) {
		t.Errorf("merged = %v", merged)
	}

	// Status change: new time; True condition appended.
	merged, changed = MergeConditions(existing, []*v1alpha1.Condition{
		{Type: v1alpha1.ConditionBudgetWarning, Status: "False", Reason: ReasonBudgetExceeded, LastTransitionTime: now},
		{Type: v1alpha1.ConditionBudgetExceeded, Status: "True", Reason: "MaxCostExceeded", LastTransitionTime: now},
	})
	if !changed || len(merged) != 3 || !merged[1].GetLastTransitionTime().AsTime().Equal(now.AsTime()) || merged[2].GetType() != v1alpha1.ConditionBudgetExceeded {
		t.Errorf("status change: changed=%v merged=%v", changed, merged)
	}

	// Reason change alone is a transition.
	if _, changed = MergeConditions(existing, []*v1alpha1.Condition{
		{Type: v1alpha1.ConditionBudgetWarning, Status: "True", Reason: "MaxTokensWarning", Message: "tokens 1 of 1 (80%)", LastTransitionTime: now},
	}); !changed {
		t.Error("reason change not reported")
	}

	for i := range existing {
		if !proto.Equal(existing[i], snapshot[i]) {
			t.Errorf("existing[%d] mutated: %v", i, existing[i])
		}
	}
}
