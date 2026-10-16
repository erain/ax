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

// Package budget evaluates a task's soft budgets (spec.budget) and idleness
// against its usage record and turns the result into Task status conditions
// (BudgetWarning, BudgetExceeded, Idle).
//
// The package is pure: no I/O and no clock. A caller (ax-server) evaluates on
// every usage delta and at read time:
//
//	res := budget.Evaluate(task, u, cfg, now)
//	merged, changed := budget.MergeConditions(task.GetStatus().GetConditions(), res.Conditions())
//	if changed { /* persist merged via UpdateTaskStatus; emit res.Crossings */ }
//
// Crossings are non-empty only when MergeConditions reports changed. At read
// time the caller uses merged (fresh messages) and ignores changed/Crossings.
//
// Budgets are WARN only: nothing is enforced here.
package budget

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// Budget dimensions (also the `ax.budget.dimension` metric attribute).
const (
	DimensionActiveDeadline = "activeDeadline"
	DimensionMaxTokens      = "maxTokens"
	DimensionMaxCost        = "maxCost"
)

// Level is how far a dimension is into its limit.
type Level string

// Levels (also the `ax.budget.level` metric attribute).
const (
	LevelOK       Level = "ok"
	LevelWarning  Level = "warning"
	LevelExceeded Level = "exceeded"
)

// Condition statuses.
const (
	StatusTrue  = "True"
	StatusFalse = "False"
)

// Reasons for False conditions. True conditions use `<Dimension>Warning`,
// `<Dimension>Exceeded` (e.g. MaxCostWarning) and the idle reasons below.
const (
	ReasonWithinBudget = "WithinBudget"
	// ReasonBudgetExceeded is the BudgetWarning=False reason when no dimension
	// is in the warning band but some dimension has exceeded its limit.
	ReasonBudgetExceeded = "BudgetExceeded"
	ReasonCommandExited  = "CommandExited"
	ReasonLowCPU         = "LowCPU"
	ReasonActive         = "Active"
	ReasonNotRunning     = "NotRunning"
)

// phaseRunning is the task phase in which a task can be idle.
const phaseRunning = "Running"

// Config holds the server-wide settings budget evaluation needs (from the
// rate card).
type Config struct {
	// DefaultWarnAtPercent applies when spec.budget.warnAtPercent is 0.
	// Zero means cost.DefaultWarnAtPercent (80).
	DefaultWarnAtPercent int32
	// IdleAfter is how long a low-cpu streak must last before the task is
	// Idle. Zero means usage.DefaultIdleAfter (5m).
	IdleAfter time.Duration
	// IdleCPUThreshold is only used in the Idle message. Zero means
	// usage.DefaultIdleCPUThreshold.
	IdleCPUThreshold float64
	// Currency of costs (only used for formatting). Empty means USD.
	Currency string
}

// ConfigFromRateCard returns the budget settings of a rate card (nil → defaults).
func ConfigFromRateCard(c *cost.RateCard) Config {
	if c == nil {
		return Config{}
	}
	return Config{
		DefaultWarnAtPercent: c.Budgets.DefaultWarnAtPercent,
		IdleAfter:            c.Idle.After,
		IdleCPUThreshold:     c.Idle.CPUThreshold,
		Currency:             c.Currency,
	}
}

func (c Config) withDefaults() Config {
	if c.DefaultWarnAtPercent <= 0 {
		c.DefaultWarnAtPercent = cost.DefaultWarnAtPercent
	}
	if c.IdleAfter <= 0 {
		c.IdleAfter = usage.DefaultIdleAfter
	}
	if c.IdleCPUThreshold <= 0 {
		c.IdleCPUThreshold = usage.DefaultIdleCPUThreshold
	}
	if c.Currency == "" {
		c.Currency = cost.DefaultCurrency
	}
	return c
}

// DimensionStatus is the evaluation of one configured budget dimension.
type DimensionStatus struct {
	Dimension string
	Level     Level
	// Value and Limit are in the dimension's unit: seconds (activeDeadline),
	// tokens (maxTokens) or currency units (maxCost).
	Value, Limit float64
	// Percent is Value/Limit*100.
	Percent float64
	// Message is human-readable, e.g. "cost $0.83 of $1.00 (83%)".
	Message string
}

// Crossing is a dimension that reached a level it was not recorded at in the
// task's current conditions. Each crossing should be emitted once (slog line +
// OnThreshold / ax.budget.threshold_crossed).
type Crossing struct {
	Dimension    string
	Level        Level // LevelWarning or LevelExceeded
	Value, Limit float64
	Percent      float64
	Message      string
}

// Result is the outcome of Evaluate.
type Result struct {
	// WarnAtPercent is the effective warning threshold.
	WarnAtPercent int32
	// Dimensions has one entry per configured limit, in the order
	// activeDeadline, maxTokens, maxCost. Empty without a budget.
	Dimensions []DimensionStatus
	// BudgetWarning and BudgetExceeded are the desired conditions; both nil
	// when the task has no budget limits.
	BudgetWarning, BudgetExceeded *v1alpha1.Condition
	// Idle is the desired Idle condition (never nil).
	Idle *v1alpha1.Condition
	// Crossings lists threshold crossings relative to the task's current
	// conditions.
	Crossings []Crossing
}

// Conditions returns the desired conditions (non-nil ones) for MergeConditions.
func (r Result) Conditions() []*v1alpha1.Condition {
	var out []*v1alpha1.Condition
	for _, c := range []*v1alpha1.Condition{r.BudgetWarning, r.BudgetExceeded, r.Idle} {
		if c != nil {
			out = append(out, c)
		}
	}
	return out
}

// Evaluate computes the desired budget and idle conditions of task for usage
// record u (nil = no usage yet). Desired conditions carry
// LastTransitionTime = now; MergeConditions keeps the existing time when the
// status did not change.
//
// Dimensions: activeDeadline vs u.ActiveSeconds, maxTokens vs the total
// tokens (input + output over all models; per semconv input already includes
// cache read/write and output includes reasoning), maxCost vs u.Cost.Total.
// A dimension is "warning" when value ≥ warn% of the limit and "exceeded"
// when value ≥ limit.
//
// BudgetWarning is True when some dimension is in the warning band (and not
// exceeded); BudgetExceeded is True when some dimension is exceeded. Reasons
// name the first such dimension (in Dimensions order); the message lists all.
//
// Idle is True when the task phase is Running and either the runner reported
// the command exited or the current low-cpu streak (from the accounting
// state) lasts at least cfg.IdleAfter.
func Evaluate(task *v1alpha1.Task, u *usage.TaskUsage, cfg Config, now time.Time) Result {
	cfg = cfg.withDefaults()
	if u == nil {
		u = usage.NewTaskUsage()
	}
	ts := timestamppb.New(now)
	b := task.GetSpec().GetBudget()

	res := Result{WarnAtPercent: cfg.DefaultWarnAtPercent}
	if w := b.GetWarnAtPercent(); w > 0 {
		res.WarnAtPercent = w
	}

	if d := b.GetActiveDeadline(); d != nil && d.AsDuration() > 0 {
		limit := d.AsDuration().Seconds()
		res.Dimensions = append(res.Dimensions, dimension(DimensionActiveDeadline, u.ActiveSeconds, limit, res.WarnAtPercent,
			func(pct string) string {
				return fmt.Sprintf("active %s of %s (%s)", fmtSeconds(u.ActiveSeconds), fmtSeconds(limit), pct)
			}))
	}
	if limit := b.GetMaxTokens(); limit > 0 {
		total := TotalTokens(u.Tokens)
		res.Dimensions = append(res.Dimensions, dimension(DimensionMaxTokens, float64(total), float64(limit), res.WarnAtPercent,
			func(pct string) string {
				return fmt.Sprintf("tokens %s of %s (%s)", fmtInt(total), fmtInt(limit), pct)
			}))
	}
	if limit := b.GetMaxCost().GetAmount(); limit > 0 {
		cur := b.GetMaxCost().GetCurrency()
		if cur == "" {
			cur = cfg.Currency
		}
		value := u.Cost.Total
		res.Dimensions = append(res.Dimensions, dimension(DimensionMaxCost, value, limit, res.WarnAtPercent,
			func(pct string) string {
				return fmt.Sprintf("cost %s of %s (%s)", fmtMoney(value, cur), fmtMoney(limit, cur), pct)
			}))
	}

	if len(res.Dimensions) > 0 {
		res.BudgetWarning = levelCondition(v1alpha1.ConditionBudgetWarning, LevelWarning, res.Dimensions, ts)
		res.BudgetExceeded = levelCondition(v1alpha1.ConditionBudgetExceeded, LevelExceeded, res.Dimensions, ts)
		res.Crossings = crossings(task.GetStatus().GetConditions(), res.Dimensions)
	}
	res.Idle = idleCondition(task, u, cfg, ts)
	return res
}

// TotalTokens is the maxTokens value of a usage record: Σ input + output over
// all models (semconv: input includes cache read/write, output includes
// reasoning, so nothing is counted twice).
func TotalTokens(m map[string]usage.TokenCounts) int64 {
	var n int64
	for _, t := range m {
		n += t.Input + t.Output
	}
	return n
}

func dimension(name string, value, limit float64, warnAt int32, msg func(pct string) string) DimensionStatus {
	pct := value / limit * 100
	lvl := LevelOK
	switch {
	case value >= limit:
		lvl = LevelExceeded
	case pct >= float64(warnAt)-1e-9:
		lvl = LevelWarning
	}
	return DimensionStatus{
		Dimension: name,
		Level:     lvl,
		Value:     value,
		Limit:     limit,
		Percent:   pct,
		Message:   msg(fmtPercent(pct, value >= limit)),
	}
}

// levelCondition builds the condition for one level: True when any dimension
// is at that level.
func levelCondition(condType string, lvl Level, dims []DimensionStatus, ts *timestamppb.Timestamp) *v1alpha1.Condition {
	var reason string
	var msgs []string
	exceeded := false
	for _, d := range dims {
		if d.Level == lvl {
			if reason == "" {
				reason = reasonPrefix(d.Dimension) + levelSuffix(lvl)
			}
			msgs = append(msgs, d.Message)
		}
		exceeded = exceeded || d.Level == LevelExceeded
	}
	c := &v1alpha1.Condition{Type: condType, LastTransitionTime: ts}
	if reason != "" {
		c.Status, c.Reason, c.Message = StatusTrue, reason, strings.Join(msgs, "; ")
		return c
	}
	c.Status, c.Reason = StatusFalse, ReasonWithinBudget
	if lvl == LevelWarning && exceeded {
		c.Reason = ReasonBudgetExceeded
	}
	all := make([]string, len(dims))
	for i, d := range dims {
		all[i] = d.Message
	}
	c.Message = strings.Join(all, "; ")
	return c
}

func reasonPrefix(dim string) string {
	switch dim {
	case DimensionActiveDeadline:
		return "ActiveDeadline"
	case DimensionMaxTokens:
		return "MaxTokens"
	case DimensionMaxCost:
		return "MaxCost"
	}
	return dim
}

func levelSuffix(l Level) string {
	if l == LevelExceeded {
		return "Exceeded"
	}
	return "Warning"
}

// messageLabel is the first word of a dimension's message segment; it is how
// crossings recognise which dimensions an existing condition already covers.
func messageLabel(dim string) string {
	switch dim {
	case DimensionActiveDeadline:
		return "active"
	case DimensionMaxTokens:
		return "tokens"
	case DimensionMaxCost:
		return "cost"
	}
	return dim
}

// crossings returns dimensions at warning/exceeded that the existing condition
// of that level does not already record (status True and a message segment
// for the dimension). A dimension jumping straight from ok to exceeded yields
// only the exceeded crossing.
func crossings(existing []*v1alpha1.Condition, dims []DimensionStatus) []Crossing {
	recorded := map[Level]map[string]bool{
		LevelWarning:  coveredLabels(find(existing, v1alpha1.ConditionBudgetWarning)),
		LevelExceeded: coveredLabels(find(existing, v1alpha1.ConditionBudgetExceeded)),
	}
	var out []Crossing
	for _, d := range dims {
		if d.Level == LevelOK || recorded[d.Level][messageLabel(d.Dimension)] {
			continue
		}
		out = append(out, Crossing{
			Dimension: d.Dimension, Level: d.Level, Value: d.Value, Limit: d.Limit, Percent: d.Percent, Message: d.Message,
		})
	}
	return out
}

func coveredLabels(c *v1alpha1.Condition) map[string]bool {
	if c == nil || c.GetStatus() != StatusTrue {
		return nil
	}
	m := map[string]bool{}
	for _, seg := range strings.Split(c.GetMessage(), ";") {
		if f := strings.Fields(seg); len(f) > 0 {
			m[f[0]] = true
		}
	}
	return m
}

// coverageChanged reports whether two True budget conditions list different
// dimensions.
func coverageChanged(old, next *v1alpha1.Condition) bool {
	switch next.GetType() {
	case v1alpha1.ConditionBudgetWarning, v1alpha1.ConditionBudgetExceeded:
	default:
		return false
	}
	if next.GetStatus() != StatusTrue {
		return false
	}
	a, b := coveredLabels(old), coveredLabels(next)
	if len(a) != len(b) {
		return true
	}
	for k := range a {
		if !b[k] {
			return true
		}
	}
	return false
}

func find(conds []*v1alpha1.Condition, condType string) *v1alpha1.Condition {
	for _, c := range conds {
		if c.GetType() == condType {
			return c
		}
	}
	return nil
}

func idleCondition(task *v1alpha1.Task, u *usage.TaskUsage, cfg Config, ts *timestamppb.Timestamp) *v1alpha1.Condition {
	c := &v1alpha1.Condition{Type: v1alpha1.ConditionIdle, Status: StatusFalse, LastTransitionTime: ts}
	totals := fmt.Sprintf("idle %s total, idle cost %s", fmtSeconds(u.IdleSeconds), fmtMoney(u.Cost.Idle, currencyOf(u, cfg)))
	if task.GetStatus().GetPhase() != phaseRunning {
		c.Reason, c.Message = ReasonNotRunning, "task is not running; "+totals
		return c
	}
	st := u.State
	if st.CommandExited {
		c.Status, c.Reason = StatusTrue, ReasonCommandExited
		c.Message = fmt.Sprintf("command exited (code %d); %s", st.ExitCode, totals)
		return c
	}
	if streak := IdleStreak(u); streak >= cfg.IdleAfter {
		c.Status, c.Reason = StatusTrue, ReasonLowCPU
		c.Message = fmt.Sprintf("cpu below %s vCPU for %s; %s",
			strconv.FormatFloat(cfg.IdleCPUThreshold, 'g', -1, 64), fmtSeconds(streak.Seconds()), totals)
		return c
	}
	c.Reason, c.Message = ReasonActive, "task is active; "+totals
	return c
}

// IdleStreak is the length of the current low-cpu streak: from the streak
// start to the latest sample of the current epoch (0 when there is no streak).
func IdleStreak(u *usage.TaskUsage) time.Duration {
	if u == nil || u.State.IdleStreakStart.IsZero() {
		return 0
	}
	last, ok := u.State.LastSample[u.State.CurrentEpoch]
	if !ok || !last.Time.After(u.State.IdleStreakStart) {
		return 0
	}
	return last.Time.Sub(u.State.IdleStreakStart)
}

func currencyOf(u *usage.TaskUsage, cfg Config) string {
	if u.Cost.Currency != "" {
		return u.Cost.Currency
	}
	return cfg.Currency
}

// MergeConditions merges desired conditions (from Result.Conditions) into
// existing ones and returns a new slice; existing is not modified. Conditions
// of other types keep their position. For each desired condition:
//   - same type present: replaced, keeping the existing LastTransitionTime when
//     the status is unchanged;
//   - absent and desired status True: appended;
//   - absent and desired status False: not added (tasks that never warned or
//     idled carry no such condition).
//
// changed reports a transition worth persisting: a condition added, its
// status or reason changed, or (for a True BudgetWarning/BudgetExceeded) the
// set of dimensions it lists changed, so a dimension joining an already-True
// level is persisted and its crossing is not reported again. Message-only updates (e.g. a growing cost) are
// reflected in merged but do not set changed, so a caller persisting only when
// changed writes once per transition, and read-time merges show fresh messages.
func MergeConditions(existing, desired []*v1alpha1.Condition) (merged []*v1alpha1.Condition, changed bool) {
	merged = make([]*v1alpha1.Condition, len(existing), len(existing)+len(desired))
	copy(merged, existing)
	for _, want := range desired {
		if want == nil {
			continue
		}
		idx := -1
		for i, c := range merged {
			if c.GetType() == want.GetType() {
				idx = i
				break
			}
		}
		if idx < 0 {
			if want.GetStatus() == StatusTrue {
				merged = append(merged, proto.Clone(want).(*v1alpha1.Condition))
				changed = true
			}
			continue
		}
		old := merged[idx]
		next := proto.Clone(want).(*v1alpha1.Condition)
		if old.GetStatus() == next.GetStatus() && old.GetLastTransitionTime() != nil {
			next.LastTransitionTime = proto.Clone(old.GetLastTransitionTime()).(*timestamppb.Timestamp)
		}
		if old.GetStatus() != next.GetStatus() || old.GetReason() != next.GetReason() || coverageChanged(old, next) {
			changed = true
		}
		merged[idx] = next
	}
	return merged, changed
}

// fmtPercent renders a percentage, floored so a value just under the limit
// never reads "100%".
func fmtPercent(pct float64, exceeded bool) string {
	p := math.Floor(pct + 1e-9)
	if !exceeded && p >= 100 {
		p = 99
	}
	return fmt.Sprintf("%d%%", int64(p))
}

func fmtSeconds(s float64) string {
	return (time.Duration(math.Round(s)) * time.Second).String()
}

func fmtMoney(v float64, currency string) string {
	format := "%.2f"
	if v != 0 && math.Abs(v) < 0.01 {
		format = "%.4f"
	}
	s := fmt.Sprintf(format, v)
	if currency == "" || currency == "USD" {
		return "$" + s
	}
	return s + " " + currency
}

// fmtInt renders n with thousands separators (1234567 → "1,234,567").
func fmtInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
