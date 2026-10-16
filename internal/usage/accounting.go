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
	"strings"
	"time"
)

const gib = float64(1 << 30)

// Normalized actor states (Substrate ate.actor.state values; an
// "ACTOR_STATE_" prefix and case are ignored on input).
const (
	StateRunning   = "running"
	StateCrashed   = "crashed"
	StateSuspended = "suspended"
)

// Defaults for AccountingConfig.
const (
	DefaultIdleCPUThreshold = 0.02
	DefaultIdleAfter        = 5 * time.Minute
	DefaultSampleGapCap     = 120 * time.Second
)

// AccountingConfig tunes the sample accounting. Zero fields take defaults.
type AccountingConfig struct {
	// IdleCPUThreshold is the cpu rate (vCPU) below which a sample interval
	// is "low". Default 0.02.
	IdleCPUThreshold float64
	// IdleAfter is how long a low streak must last before intervals count as
	// idle. Default 5m.
	IdleAfter time.Duration
	// SampleGapCap caps the Δt between consecutive samples of an epoch used
	// for memory/idle integration. Default 120s (2× the 60s sample period).
	SampleGapCap time.Duration
}

func (c AccountingConfig) withDefaults() AccountingConfig {
	if c.IdleCPUThreshold <= 0 {
		c.IdleCPUThreshold = DefaultIdleCPUThreshold
	}
	if c.IdleAfter <= 0 {
		c.IdleAfter = DefaultIdleAfter
	}
	if c.SampleGapCap <= 0 {
		c.SampleGapCap = DefaultSampleGapCap
	}
	return c
}

// UsageSample is one ate.actor.usage_sampled event. Nil measurements mean
// absent (not measurable yet).
type UsageSample struct {
	Time            time.Time
	ActorUID        string
	Pool            string
	Epoch           int64
	Kind            string
	CPUSeconds      *float64
	WorkingSetBytes *int64
}

// StateChange is one ate.actor.state_changed / ate.actor.crashed event.
type StateChange struct {
	Time      time.Time
	ActorUID  string
	State     string
	Operation string
}

// RunnerTokens are cumulative token counters for one model as reported by
// the runner (GET :80/usage).
type RunnerTokens struct {
	Input        int64   `json:"input,omitempty"`
	Output       int64   `json:"output,omitempty"`
	CacheRead    int64   `json:"cacheRead,omitempty"`
	CacheWrite   int64   `json:"cacheWrite,omitempty"`
	Reasoning    int64   `json:"reasoning,omitempty"`
	CostAmount   float64 `json:"costAmount,omitempty"`
	CostCurrency string  `json:"costCurrency,omitempty"`
}

// geq reports whether every counter of t is >= the one of o.
func (t RunnerTokens) geq(o RunnerTokens) bool {
	return t.Input >= o.Input && t.Output >= o.Output && t.CacheRead >= o.CacheRead &&
		t.CacheWrite >= o.CacheWrite && t.Reasoning >= o.Reasoning && t.CostAmount >= o.CostAmount
}

// RunnerReport is one runner usage report (cumulative since the counters
// file was created; Session changes on every runner process start).
type RunnerReport struct {
	Session        string
	ReportTime     time.Time
	CommandExited  bool
	ExitCode       int
	WorkspaceBytes int64
	Tokens         map[string]RunnerTokens
}

// NormalizeState lowercases s and strips an "actor_state_" prefix.
func NormalizeState(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return strings.TrimPrefix(s, "actor_state_")
}

// DateOf returns the UTC day bucket key of t.
func DateOf(t time.Time) string { return t.UTC().Format(DateLayout) }

// forEachDay calls fn for each UTC-day segment of [from, to).
func forEachDay(from, to time.Time, fn func(date string, from, to time.Time)) {
	from, to = from.UTC(), to.UTC()
	for from.Before(to) {
		y, m, d := from.Date()
		midnight := time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC)
		end := to
		if midnight.Before(end) {
			end = midnight
		}
		fn(from.Format(DateLayout), from, end)
		from = end
	}
}

// acceptUID applies the actor uid guard and records the first uid seen.
func (u *TaskUsage) acceptUID(uid string) bool {
	if uid == "" {
		return true
	}
	if u.State.ActorUID == "" {
		u.State.ActorUID = uid
		return true
	}
	return uid == u.State.ActorUID
}

// maxEpochs bounds the per-epoch state kept in a record.
const maxEpochs = 8

// ApplyUsageSample folds one usage sample into u and returns the increment.
//
// CPU = Σ_epochs max(cpu.time) (replays and out-of-order samples add nothing);
// memory GiB·s and idle are integrated over the interval between the
// previous (by time) sample of the same epoch and this one, with Δt capped
// at cfg.SampleGapCap and the interval split at UTC midnight; the cpu
// increment is spread over the same interval. An interval is idle when the
// command has exited, or for the part after a low-cpu streak (rate below
// cfg.IdleCPUThreshold) has lasted cfg.IdleAfter. A sample while the actor
// state is still unknown opens an active interval at the sample time; an open
// active interval is accrued up to the sample time.
//
// Persist the record after every Apply* call, even when the returned Delta is
// zero: the call may still have advanced accounting state (last sample per
// epoch, open interval, actor uid, runner session/baseline, LastPullTime,
// WorkspaceBytes, CommandExited). Do not return ErrNoChange based on
// Delta.IsZero().
func (u *TaskUsage) ApplyUsageSample(s UsageSample, cfg AccountingConfig) Delta {
	var d Delta
	if s.Time.IsZero() || !u.acceptUID(s.ActorUID) {
		return d
	}
	cfg = cfg.withDefaults()
	st := &u.State
	if st.EpochFloor != 0 && s.Epoch <= st.EpochFloor {
		return d
	}
	if s.Pool != "" {
		st.Pool = s.Pool
	}
	if st.EpochMaxCPU == nil {
		st.EpochMaxCPU = map[int64]float64{}
	}
	if st.LastSample == nil {
		st.LastSample = map[int64]SamplePoint{}
	}
	if s.Epoch > st.CurrentEpoch || len(st.LastSample) == 0 {
		if s.Epoch > st.CurrentEpoch {
			st.CurrentEpoch = s.Epoch
		}
		// A new activation starts a fresh low-cpu streak.
		st.IdleStreakStart = time.Time{}
	}

	// Active time.
	if st.State == "" && st.ActiveSince.IsZero() {
		u.openActive(&d, s.Time)
	}
	u.accrueActive(&d, s.Time)

	// CPU: max per epoch.
	var cpuInc float64
	if s.CPUSeconds != nil {
		maxCPU, seen := st.EpochMaxCPU[s.Epoch]
		if !seen || *s.CPUSeconds > maxCPU {
			if *s.CPUSeconds > maxCPU {
				cpuInc = *s.CPUSeconds - maxCPU
			}
			st.EpochMaxCPU[s.Epoch] = max(*s.CPUSeconds, maxCPU)
		}
	}

	prev, hasPrev := st.LastSample[s.Epoch]
	if hasPrev && s.Time.After(prev.Time) {
		raw := s.Time.Sub(prev.Time)
		dt := min(raw, cfg.SampleGapCap)
		start := s.Time.Add(-dt)
		wsGiB := workingSetGiB(prev.WorkingSetBytes, s.WorkingSetBytes)

		// Idle classification. Only the current epoch drives the low-cpu
		// streak: a late sample of an older epoch must not start or reset
		// the streak of the current activation.
		idle := false
		var idleFrom time.Time
		if s.Epoch == st.CurrentEpoch && prev.CPUSeconds != nil && s.CPUSeconds != nil {
			rate := max(0, *s.CPUSeconds-*prev.CPUSeconds) / raw.Seconds()
			if rate < cfg.IdleCPUThreshold {
				if st.IdleStreakStart.IsZero() {
					st.IdleStreakStart = prev.Time
				}
				idle, idleFrom = true, st.IdleStreakStart.Add(cfg.IdleAfter)
			} else {
				st.IdleStreakStart = time.Time{}
			}
		}
		if st.CommandExited {
			idle, idleFrom = true, start
		}

		total := dt.Seconds()
		forEachDay(start, s.Time, func(date string, from, to time.Time) {
			secs := to.Sub(from).Seconds()
			dd := d.day(date)
			dd.MemoryGiBSeconds += wsGiB * secs
			dd.CPUSeconds += cpuInc * secs / total
			if !idle {
				return
			}
			if idleFrom.After(from) {
				from = idleFrom
			}
			if ov := to.Sub(from).Seconds(); ov > 0 {
				dd.IdleSeconds += ov
				dd.IdleMemoryGiBSeconds += wsGiB * ov
				dd.IdleCPUSeconds += cpuInc * ov / total
			}
		})
	} else if cpuInc > 0 {
		d.day(DateOf(s.Time)).CPUSeconds += cpuInc
	}

	if !hasPrev || s.Time.After(prev.Time) {
		st.LastSample[s.Epoch] = SamplePoint{Time: s.Time, CPUSeconds: s.CPUSeconds, WorkingSetBytes: s.WorkingSetBytes}
	}
	u.pruneEpochs()
	u.touch(s.Time)
	u.commit(&d)
	return d
}

// pruneEpochs keeps state for at most maxEpochs epochs (the newest).
func (u *TaskUsage) pruneEpochs() {
	st := &u.State
	for len(st.EpochMaxCPU) > maxEpochs || len(st.LastSample) > maxEpochs {
		oldest := int64(0)
		first := true
		for e := range st.EpochMaxCPU {
			if first || e < oldest {
				oldest, first = e, false
			}
		}
		for e := range st.LastSample {
			if first || e < oldest {
				oldest, first = e, false
			}
		}
		delete(st.EpochMaxCPU, oldest)
		delete(st.LastSample, oldest)
		if oldest > st.EpochFloor {
			st.EpochFloor = oldest
		}
	}
}

func workingSetGiB(a, b *int64) float64 {
	switch {
	case a != nil && b != nil:
		return (float64(*a) + float64(*b)) / 2 / gib
	case a != nil:
		return float64(*a) / gib
	case b != nil:
		return float64(*b) / gib
	}
	return 0
}

// accrueActive accounts the open active interval up to t.
func (u *TaskUsage) accrueActive(d *Delta, t time.Time) {
	st := &u.State
	if st.ActiveSince.IsZero() || !t.After(st.ActiveAccruedTo) {
		return
	}
	forEachDay(st.ActiveAccruedTo, t, func(date string, from, to time.Time) {
		d.day(date).ActiveSeconds += to.Sub(from).Seconds()
	})
	st.ActiveAccruedTo = t
}

// openActive opens the active interval at r. If r is past the high-water
// mark a new covered range starts at r; otherwise only the part of
// [r, ActiveCoveredFrom) not yet accounted is added (a late RUNNING event for
// an interval that samples already accounted).
func (u *TaskUsage) openActive(d *Delta, r time.Time) {
	st := &u.State
	if st.ActiveAccruedTo.IsZero() || r.After(st.ActiveAccruedTo) {
		st.ActiveCoveredFrom, st.ActiveAccruedTo = r, r
	} else {
		u.extendActiveBack(d, r)
	}
	st.ActiveSince = r
}

// extendActiveBack accounts [r, ActiveCoveredFrom) when r is earlier than the
// covered range.
func (u *TaskUsage) extendActiveBack(d *Delta, r time.Time) {
	st := &u.State
	if st.ActiveCoveredFrom.IsZero() || !r.Before(st.ActiveCoveredFrom) {
		return
	}
	forEachDay(r, st.ActiveCoveredFrom, func(date string, from, to time.Time) {
		d.day(date).ActiveSeconds += to.Sub(from).Seconds()
	})
	st.ActiveCoveredFrom = r
}

// accrueStorage accounts WorkspaceBytes × Δt since the last accrual up to t.
func (u *TaskUsage) accrueStorage(d *Delta, t time.Time) {
	st := &u.State
	if st.StorageAccruedAt.IsZero() {
		st.StorageAccruedAt = t
		return
	}
	if !t.After(st.StorageAccruedAt) {
		return
	}
	if u.WorkspaceBytes > 0 {
		g := float64(u.WorkspaceBytes) / gib
		forEachDay(st.StorageAccruedAt, t, func(date string, from, to time.Time) {
			d.day(date).StorageGiBSeconds += g * to.Sub(from).Seconds()
		})
	}
	st.StorageAccruedAt = t
}

// ApplyStateChange folds one actor state event into u and returns the
// increment. "running" opens an active interval; any other state closes it.
// Events not newer than the last applied state event are ignored (replays,
// reordering). Storage is accrued up to the event time.
//
// Persist the record after every Apply* call, even when the returned Delta is
// zero: the call may still have advanced accounting state (last sample per
// epoch, open interval, actor uid, runner session/baseline, LastPullTime,
// WorkspaceBytes, CommandExited). Do not return ErrNoChange based on
// Delta.IsZero().
func (u *TaskUsage) ApplyStateChange(e StateChange) Delta {
	var d Delta
	state := NormalizeState(e.State)
	if e.Time.IsZero() || state == "" || !u.acceptUID(e.ActorUID) {
		return d
	}
	st := &u.State
	if !st.StateTime.IsZero() && !e.Time.After(st.StateTime) {
		return d
	}
	u.accrueStorage(&d, e.Time)
	if state == StateRunning {
		if st.ActiveSince.IsZero() {
			u.openActive(&d, e.Time)
		} else {
			// Already open (e.g. by a sample before any state event): extend
			// back to the real start if earlier, else accrue up to e.Time.
			u.extendActiveBack(&d, e.Time)
			if e.Time.Before(st.ActiveSince) {
				st.ActiveSince = e.Time
			}
			u.accrueActive(&d, e.Time)
		}
	} else {
		u.accrueActive(&d, e.Time)
		// Close; ActiveCoveredFrom/ActiveAccruedTo stay as the high-water mark.
		st.ActiveSince = time.Time{}
		st.IdleStreakStart = time.Time{}
	}
	st.State, st.StateTime = state, e.Time
	u.touch(e.Time)
	u.commit(&d)
	return d
}

// ApplyRunnerReport folds one runner usage report into u and returns the
// increment. now is the pull time (recorded as LastPullTime and used as the
// report time when ReportTime is zero). Token deltas are cumulative diffs:
// within a session against the last report (a model whose counters went
// down restarts from zero); on a new session against the previous session's
// last values if every model's counters are >= them (same counters file),
// else against zero. Reported provider cost passes through in
// DayDelta.ProviderCost. Storage accrues the previous WorkspaceBytes up to
// the report time. Reports older than the last applied one are ignored (only
// LastPullTime advances).
//
// Persist the record after every Apply* call, even when the returned Delta is
// zero: the call may still have advanced accounting state (last sample per
// epoch, open interval, actor uid, runner session/baseline, LastPullTime,
// WorkspaceBytes, CommandExited). Do not return ErrNoChange based on
// Delta.IsZero().
func (u *TaskUsage) ApplyRunnerReport(r RunnerReport, now time.Time) Delta {
	var d Delta
	st := &u.State
	if now.After(st.LastPullTime) {
		st.LastPullTime = now
	}
	t := r.ReportTime
	if t.IsZero() {
		t = now
	}
	if t.IsZero() || (!st.RunnerReportTime.IsZero() && t.Before(st.RunnerReportTime)) {
		return d
	}

	sameSession := st.RunnerSession == r.Session && !st.RunnerReportTime.IsZero()
	base := st.RunnerTokens
	if !sameSession {
		for m, last := range st.RunnerTokens {
			if !r.Tokens[m].geq(last) {
				base = nil // counters file was reset
				break
			}
		}
	}

	date := DateOf(t)
	next := make(map[string]RunnerTokens, len(base)+len(r.Tokens))
	for m, v := range base {
		next[m] = v
	}
	for m, cur := range r.Tokens {
		prev := base[m]
		if !cur.geq(prev) {
			prev = RunnerTokens{}
		}
		tc := TokenCounts{
			Input:      cur.Input - prev.Input,
			Output:     cur.Output - prev.Output,
			CacheRead:  cur.CacheRead - prev.CacheRead,
			CacheWrite: cur.CacheWrite - prev.CacheWrite,
			Reasoning:  cur.Reasoning - prev.Reasoning,
		}
		if !tc.IsZero() {
			dd := d.day(date)
			if dd.Tokens == nil {
				dd.Tokens = map[string]TokenCounts{}
			}
			dd.Tokens[m] = dd.Tokens[m].Add(tc)
		}
		if diff := cur.CostAmount - prev.CostAmount; diff > 0 {
			dd := d.day(date)
			if dd.ProviderCost == nil {
				dd.ProviderCost = map[string]Money{}
			}
			dd.ProviderCost[m] = Money{Currency: cur.CostCurrency, Amount: diff}
		}
		next[m] = cur
	}
	st.RunnerTokens = next
	if len(next) == 0 {
		st.RunnerTokens = nil
	}

	u.accrueStorage(&d, t)
	u.WorkspaceBytes = r.WorkspaceBytes

	if sameSession {
		if r.CommandExited && !st.CommandExited {
			st.CommandExited, st.ExitCode = true, r.ExitCode
		}
	} else {
		st.CommandExited, st.ExitCode = r.CommandExited, r.ExitCode
	}
	st.RunnerSession, st.RunnerReportTime = r.Session, t
	u.touch(t)
	u.commit(&d)
	return d
}
