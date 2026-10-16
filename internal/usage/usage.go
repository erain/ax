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

// Package usage is the FinOps accounting model for AX tasks: it turns Substrate
// usage samples and state changes plus runner token reports into per-task,
// per-UTC-day usage amounts, keeps priced (frozen) costs next to them, and
// persists the result in a Store.
//
// The accounting is pure and deterministic: every time value comes from an
// event or report timestamp, or from an explicit now argument. Nothing in this
// package calls time.Now in the accounting path.
//
// Per-day model: every Apply* method returns a Delta whose Days map holds one
// DayDelta per UTC date ("YYYY-MM-DD") touched. Intervals (memory, active,
// idle, storage) are split at UTC midnight, so a single Delta may span two
// days. Pricing is applied per day bucket (TaskUsage.ChargeDelta calls
// Pricer.Price once per day and adds each day's Cost to that day and to the
// lifetime totals), so day buckets always carry their own frozen cost.
package usage

import (
	"context"
	"sort"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DateLayout is the layout of day bucket keys (UTC dates).
const DateLayout = "2006-01-02"

// Key identifies the usage record of one task.
type Key struct {
	Atespace string
	Name     string
}

func (k Key) normalize() Key {
	if k.Atespace == "" {
		k.Atespace = "default"
	}
	return k
}

// TokenCounts are token counts for one model.
type TokenCounts struct {
	Input      int64 `json:"input,omitempty"`
	Output     int64 `json:"output,omitempty"`
	CacheRead  int64 `json:"cacheRead,omitempty"`
	CacheWrite int64 `json:"cacheWrite,omitempty"`
	Reasoning  int64 `json:"reasoning,omitempty"`
}

// IsZero reports whether all counts are zero.
func (t TokenCounts) IsZero() bool { return t == TokenCounts{} }

// Add returns t + o.
func (t TokenCounts) Add(o TokenCounts) TokenCounts {
	return TokenCounts{
		Input:      t.Input + o.Input,
		Output:     t.Output + o.Output,
		CacheRead:  t.CacheRead + o.CacheRead,
		CacheWrite: t.CacheWrite + o.CacheWrite,
		Reasoning:  t.Reasoning + o.Reasoning,
	}
}

// Money is an amount in a currency.
type Money struct {
	Currency string  `json:"currency,omitempty"`
	Amount   float64 `json:"amount,omitempty"`
}

// Cost is a priced amount. Idle is a subset of Compute; Total = Compute +
// Tokens + Storage.
type Cost struct {
	Currency                string  `json:"currency,omitempty"`
	Total                   float64 `json:"total,omitempty"`
	Compute                 float64 `json:"compute,omitempty"`
	Tokens                  float64 `json:"tokens,omitempty"`
	Storage                 float64 `json:"storage,omitempty"`
	Idle                    float64 `json:"idle,omitempty"`
	TokensPartiallyUnpriced bool    `json:"tokensPartiallyUnpriced,omitempty"`
}

// Add adds o to c. An empty currency on c is taken from o.
func (c *Cost) Add(o Cost) {
	if c.Currency == "" {
		c.Currency = o.Currency
	}
	c.Total += o.Total
	c.Compute += o.Compute
	c.Tokens += o.Tokens
	c.Storage += o.Storage
	c.Idle += o.Idle
	c.TokensPartiallyUnpriced = c.TokensPartiallyUnpriced || o.TokensPartiallyUnpriced
}

// Amounts are the usage quantities kept for a task lifetime, per day, and in
// a DayDelta. Idle* amounts are subsets of the corresponding totals.
type Amounts struct {
	ActiveSeconds        float64                `json:"activeSeconds,omitempty"`
	CPUSeconds           float64                `json:"cpuSeconds,omitempty"`
	MemoryGiBSeconds     float64                `json:"memoryGibSeconds,omitempty"`
	IdleSeconds          float64                `json:"idleSeconds,omitempty"`
	IdleCPUSeconds       float64                `json:"idleCpuSeconds,omitempty"`
	IdleMemoryGiBSeconds float64                `json:"idleMemoryGibSeconds,omitempty"`
	StorageGiBSeconds    float64                `json:"storageGibSeconds,omitempty"`
	Tokens               map[string]TokenCounts `json:"tokens,omitempty"`
}

// IsZero reports whether every amount is zero.
func (a *Amounts) IsZero() bool {
	if a.ActiveSeconds != 0 || a.CPUSeconds != 0 || a.MemoryGiBSeconds != 0 ||
		a.IdleSeconds != 0 || a.IdleCPUSeconds != 0 || a.IdleMemoryGiBSeconds != 0 ||
		a.StorageGiBSeconds != 0 {
		return false
	}
	for _, t := range a.Tokens {
		if !t.IsZero() {
			return false
		}
	}
	return true
}

// Add adds o to a.
func (a *Amounts) Add(o Amounts) {
	a.ActiveSeconds += o.ActiveSeconds
	a.CPUSeconds += o.CPUSeconds
	a.MemoryGiBSeconds += o.MemoryGiBSeconds
	a.IdleSeconds += o.IdleSeconds
	a.IdleCPUSeconds += o.IdleCPUSeconds
	a.IdleMemoryGiBSeconds += o.IdleMemoryGiBSeconds
	a.StorageGiBSeconds += o.StorageGiBSeconds
	for m, t := range o.Tokens {
		if t.IsZero() {
			continue
		}
		if a.Tokens == nil {
			a.Tokens = map[string]TokenCounts{}
		}
		a.Tokens[m] = a.Tokens[m].Add(t)
	}
}

// DayDelta is the increment for one UTC day. ProviderCost holds, per model,
// the token cost reported by the harness/provider for this increment (only
// models whose cumulative reported cost grew are present); the Pricer uses it
// instead of rate-card token prices.
type DayDelta struct {
	Amounts
	ProviderCost map[string]Money `json:"providerCost,omitempty"`
}

// IsZero reports whether the day delta carries nothing.
func (d *DayDelta) IsZero() bool {
	if !d.Amounts.IsZero() {
		return false
	}
	for _, m := range d.ProviderCost {
		if m.Amount != 0 {
			return false
		}
	}
	return true
}

// Add adds o to d.
func (d *DayDelta) Add(o DayDelta) {
	d.Amounts.Add(o.Amounts)
	for model, m := range o.ProviderCost {
		if m.Amount == 0 {
			continue
		}
		if d.ProviderCost == nil {
			d.ProviderCost = map[string]Money{}
		}
		cur := d.ProviderCost[model]
		if cur.Currency == "" {
			cur.Currency = m.Currency
		}
		cur.Amount += m.Amount
		d.ProviderCost[model] = cur
	}
}

// Delta is the usage increment produced by one Apply* call, keyed by UTC date
// ("YYYY-MM-DD"). Days never contains zero DayDeltas.
type Delta struct {
	Days map[string]*DayDelta `json:"days,omitempty"`
}

// IsZero reports whether the delta carries nothing.
func (d Delta) IsZero() bool {
	for _, dd := range d.Days {
		if dd != nil && !dd.IsZero() {
			return false
		}
	}
	return true
}

// Dates returns the dates of d in ascending order.
func (d Delta) Dates() []string {
	dates := make([]string, 0, len(d.Days))
	for k := range d.Days {
		dates = append(dates, k)
	}
	sort.Strings(dates)
	return dates
}

// Sum returns the sum of all day deltas (useful for budgets and metrics,
// which do not care about day boundaries).
func (d Delta) Sum() DayDelta {
	var s DayDelta
	for _, date := range d.Dates() {
		if dd := d.Days[date]; dd != nil {
			s.Add(*dd)
		}
	}
	return s
}

// Add merges o into d (e.g. to accumulate the deltas of a batch of events).
func (d *Delta) Add(o Delta) {
	for _, date := range o.Dates() {
		if dd := o.Days[date]; dd != nil && !dd.IsZero() {
			d.day(date).Add(*dd)
		}
	}
}

// day returns the bucket for date, creating it.
func (d *Delta) day(date string) *DayDelta {
	if d.Days == nil {
		d.Days = map[string]*DayDelta{}
	}
	dd := d.Days[date]
	if dd == nil {
		dd = &DayDelta{}
		d.Days[date] = dd
	}
	return dd
}

// prune removes zero day deltas.
func (d *Delta) prune() {
	for k, dd := range d.Days {
		if dd == nil || dd.IsZero() {
			delete(d.Days, k)
		}
	}
	if len(d.Days) == 0 {
		d.Days = nil
	}
}

// Pricer prices a usage delta. internal/cost implements it. pool is the
// Substrate worker pool (rate cards may price pools differently). Price must
// be linear in d: ChargeDelta calls it once per day bucket.
type Pricer interface {
	Price(d Delta, pool string) Cost
}

// Hooks receives every recorded delta together with its cost and the updated
// usage (after the cost was added). It is called after the store update
// committed. Budgets and metrics implement it.
type Hooks interface {
	OnDelta(ctx context.Context, key Key, d Delta, c Cost, u *TaskUsage)
}

// DayUsage is the usage of a task on one UTC day.
type DayUsage struct {
	Amounts
	Cost Cost `json:"cost,omitzero"`
}

// TaskUsage is the persisted usage record of one task: lifetime totals,
// per-day buckets, and the accounting state needed to turn cumulative and
// out-of-order inputs into exact increments.
type TaskUsage struct {
	// Lifetime totals.
	Amounts
	// WorkspaceBytes is the last reported workspace size (a gauge).
	WorkspaceBytes int64 `json:"workspaceBytes,omitempty"`
	// Cost is the lifetime frozen cost (sum of every AddCost).
	Cost Cost `json:"cost,omitzero"`
	// Days are per-UTC-day buckets keyed by "YYYY-MM-DD".
	Days map[string]*DayUsage `json:"days,omitempty"`
	// State is the accounting state.
	State AccountingState `json:"state,omitzero"`
	// UpdateTime is the latest event/report time applied.
	UpdateTime time.Time `json:"updateTime,omitzero"`
}

// SamplePoint is the last usage sample seen for an epoch.
type SamplePoint struct {
	Time            time.Time `json:"time"`
	CPUSeconds      *float64  `json:"cpuSeconds,omitempty"`
	WorkingSetBytes *int64    `json:"workingSetBytes,omitempty"`
}

// AccountingState is the bookkeeping that makes the accounting idempotent
// under replays and robust to out-of-order inputs.
type AccountingState struct {
	// ActorUID is the Substrate actor uid; once set, inputs carrying a
	// different uid are ignored.
	ActorUID string `json:"actorUid,omitempty"`
	// Pool is the last worker pool seen in a usage sample.
	Pool string `json:"pool,omitempty"`

	// EpochMaxCPU is the max cumulative cpu.time seen per epoch.
	EpochMaxCPU map[int64]float64 `json:"epochMaxCpu,omitempty"`
	// LastSample is the latest (by time) sample per epoch.
	LastSample map[int64]SamplePoint `json:"lastSample,omitempty"`
	// CurrentEpoch is the highest epoch seen.
	CurrentEpoch int64 `json:"currentEpoch,omitempty"`
	// EpochFloor: epochs <= EpochFloor were pruned; their samples are ignored.
	EpochFloor int64 `json:"epochFloor,omitempty"`

	// State is the last normalized actor state ("running", "suspended", ...);
	// empty means unknown (no state event seen yet).
	State     string    `json:"state,omitempty"`
	StateTime time.Time `json:"stateTime,omitzero"`
	// ActiveSince is the start of the open RUNNING interval (zero = closed).
	ActiveSince time.Time `json:"activeSince,omitzero"`
	// [ActiveCoveredFrom, ActiveAccruedTo] is the latest contiguous range
	// already accounted as active. ActiveAccruedTo is a high-water mark: it
	// survives closing the interval, so late state events whose times fall
	// inside that range (samples and state events travel separate pipelines
	// and may arrive in either order) never count the same time twice.
	ActiveCoveredFrom time.Time `json:"activeCoveredFrom,omitzero"`
	ActiveAccruedTo   time.Time `json:"activeAccruedTo,omitzero"`
	// IdleStreakStart is the start of the current low-cpu streak (zero = none).
	IdleStreakStart time.Time `json:"idleStreakStart,omitzero"`

	// RunnerSession is the session of the last applied runner report.
	RunnerSession string `json:"runnerSession,omitempty"`
	// RunnerReportTime is the report time of the last applied runner report.
	RunnerReportTime time.Time `json:"runnerReportTime,omitzero"`
	// RunnerTokens are the last cumulative token counters (per model).
	RunnerTokens map[string]RunnerTokens `json:"runnerTokens,omitempty"`
	// CommandExited is set once the runner reported the command exited, until
	// a new session starts.
	CommandExited bool `json:"commandExited,omitempty"`
	ExitCode      int  `json:"exitCode,omitempty"`
	// LastPullTime is the now of the last ApplyRunnerReport.
	LastPullTime time.Time `json:"lastPullTime,omitzero"`
	// StorageAccruedAt is how far storage GiB·s were already accounted.
	StorageAccruedAt time.Time `json:"storageAccruedAt,omitzero"`
}

// NewTaskUsage returns an empty usage record.
func NewTaskUsage() *TaskUsage {
	return &TaskUsage{Days: map[string]*DayUsage{}}
}

// Day returns the bucket for date ("YYYY-MM-DD"), or nil.
func (u *TaskUsage) Day(date string) *DayUsage {
	return u.Days[date]
}

func (u *TaskUsage) day(date string) *DayUsage {
	if u.Days == nil {
		u.Days = map[string]*DayUsage{}
	}
	d := u.Days[date]
	if d == nil {
		d = &DayUsage{}
		u.Days[date] = d
	}
	return d
}

// AddCost adds a priced cost to the day bucket and the lifetime total.
func (u *TaskUsage) AddCost(day string, c Cost) {
	u.Cost.Add(c)
	u.day(day).Cost.Add(c)
}

// ChargeDelta prices d per day bucket with p, adds each day's cost to that
// day and to the lifetime total, and returns the summed cost. A nil Pricer
// charges nothing. Costs are frozen: they are never recomputed later.
func (u *TaskUsage) ChargeDelta(d Delta, p Pricer, pool string) Cost {
	var total Cost
	if p == nil {
		return total
	}
	for _, date := range d.Dates() {
		dd := d.Days[date]
		if dd == nil || dd.IsZero() {
			continue
		}
		c := p.Price(Delta{Days: map[string]*DayDelta{date: dd}}, pool)
		u.AddCost(date, c)
		total.Add(c)
	}
	return total
}

// commit adds d's amounts into the lifetime totals and day buckets, in date
// order so float sums are deterministic.
func (u *TaskUsage) commit(d *Delta) {
	d.prune()
	for _, date := range d.Dates() {
		dd := d.Days[date]
		u.Amounts.Add(dd.Amounts)
		u.day(date).Amounts.Add(dd.Amounts)
	}
}

func (u *TaskUsage) touch(t time.Time) {
	if t.After(u.UpdateTime) {
		u.UpdateTime = t
	}
}

// ToStatus converts lifetime usage to the API UsageStatus (tokens sorted by
// model). A nil usage yields nil.
func ToStatus(u *TaskUsage) *v1alpha1.UsageStatus {
	if u == nil {
		return nil
	}
	s := &v1alpha1.UsageStatus{
		ActiveDuration:   secondsToDuration(u.ActiveSeconds),
		CpuSeconds:       u.CPUSeconds,
		MemoryGibSeconds: u.MemoryGiBSeconds,
		WorkspaceBytes:   u.WorkspaceBytes,
		IdleDuration:     secondsToDuration(u.IdleSeconds),
		Tokens:           TokensToProto(u.Tokens),
		Cost:             CostToProto(u.Cost),
	}
	if !u.UpdateTime.IsZero() {
		s.UpdateTime = timestamppb.New(u.UpdateTime)
	}
	return s
}

// TokensToProto converts per-model token counts to the API form, sorted by
// model; zero entries are dropped.
func TokensToProto(m map[string]TokenCounts) []*v1alpha1.ModelTokenUsage {
	models := make([]string, 0, len(m))
	for k, t := range m {
		if !t.IsZero() {
			models = append(models, k)
		}
	}
	sort.Strings(models)
	var out []*v1alpha1.ModelTokenUsage
	for _, k := range models {
		t := m[k]
		out = append(out, &v1alpha1.ModelTokenUsage{
			Model:      k,
			Input:      t.Input,
			Output:     t.Output,
			CacheRead:  t.CacheRead,
			CacheWrite: t.CacheWrite,
			Reasoning:  t.Reasoning,
		})
	}
	return out
}

// CostToProto converts a Cost to the API CostBreakdown.
func CostToProto(c Cost) *v1alpha1.CostBreakdown {
	return &v1alpha1.CostBreakdown{
		Currency:                c.Currency,
		Total:                   c.Total,
		Compute:                 c.Compute,
		Tokens:                  c.Tokens,
		Storage:                 c.Storage,
		Idle:                    c.Idle,
		TokensPartiallyUnpriced: c.TokensPartiallyUnpriced,
	}
}

func secondsToDuration(s float64) *durationpb.Duration {
	return durationpb.New(time.Duration(s * float64(time.Second)).Round(time.Millisecond))
}
