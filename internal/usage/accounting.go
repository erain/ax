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
	"slices"
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
// cfg.IdleCPUThreshold) has lasted cfg.IdleAfter; the streak never starts
// before State.LastTokenActivity (see ApplyRunnerReport). Active time is derived
// from epochs and state events (see reconcileActive); the sample extends its
// epoch's activation interval.
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

	st.SampleGapCap = cfg.SampleGapCap

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
				// New tokens are activity: the streak starts no earlier
				// than the last token-bearing runner report.
				if st.IdleStreakStart.Before(st.LastTokenActivity) {
					st.IdleStreakStart = st.LastTokenActivity
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
	if st.EpochFirstSample == nil {
		st.EpochFirstSample = map[int64]time.Time{}
	}
	if first, ok := st.EpochFirstSample[s.Epoch]; !ok || s.Time.Before(first) {
		st.EpochFirstSample[s.Epoch] = s.Time
	}
	if s.Kind == "final" {
		if st.EpochFinal == nil {
			st.EpochFinal = map[int64]bool{}
		}
		st.EpochFinal[s.Epoch] = true
	}
	u.pruneEpochs()
	u.reconcileActive(&d)
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
		delete(st.EpochFirstSample, oldest)
		delete(st.EpochFinal, oldest)
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

// Active-time derivation limits.
const (
	// maxStateMarks bounds the recorded state events.
	maxStateMarks = 32
	// maxActiveSpans bounds the recorded covered ranges.
	maxActiveSpans = 64
	// minEpochUnixNano: smaller epoch values are not treated as unix-nano
	// activation start times (2001-09-09).
	minEpochUnixNano = int64(1e18)
	// epochStartSkew tolerates clock skew between the epoch value and the
	// sample timestamps.
	epochStartSkew = 5 * time.Second
	// An epoch belongs to the RUNNING event at R when its start lies in
	// [R-runningMatchBefore, R+runningMatchAfter]. In Substrate v0.5.0 the
	// epoch precedes the running commit by well under a second; a fallback
	// start (first sample) can be up to one sample period after it.
	runningMatchBefore = 60 * time.Second
	runningMatchAfter  = 65 * time.Second
)

// isClosingState reports whether state ends an activation.
func isClosingState(state string) bool {
	switch state {
	case "suspending", StateSuspended, StateCrashed, "deleting", "deleted", "reverting":
		return true
	}
	return false
}

// epochStart is the activation start of epoch e: the epoch value itself
// (Substrate's ate.actor.epoch is the unix-nano time the activation began)
// when it is a plausible timestamp not later than the epoch's first sample,
// else the epoch's first sample time.
func (u *TaskUsage) epochStart(e int64) time.Time {
	st := &u.State
	first, ok := st.EpochFirstSample[e]
	if !ok {
		first = st.LastSample[e].Time
	}
	if e >= minEpochUnixNano {
		if start := time.Unix(0, e).UTC(); !start.After(first.Add(epochStartSkew)) {
			return start
		}
	}
	return first
}

// ActorStopped reports whether the newest recorded actor event says the actor
// is not running: the last state mark is a closing state (suspending,
// suspended, crashed, deleting, deleted, reverting) and no usage sample newer
// than it belongs to an epoch still live (no final sample). False when no
// state event was recorded.
func (u *TaskUsage) ActorStopped() bool {
	st := &u.State
	n := len(st.Marks)
	if n == 0 || !isClosingState(st.Marks[n-1].State) {
		return false
	}
	closed := st.Marks[n-1].Time
	for e, p := range st.LastSample {
		if p.Time.After(closed) && !st.EpochFinal[e] {
			return false
		}
	}
	return true
}

// ActorLastSeen is the time of the newest accepted actor event (state event
// or usage sample) still recorded; zero if none.
func (u *TaskUsage) ActorLastSeen() time.Time {
	var t time.Time
	if n := len(u.State.Marks); n > 0 {
		t = u.State.Marks[n-1].Time
	}
	for _, p := range u.State.LastSample {
		if p.Time.After(t) {
			t = p.Time
		}
	}
	return t
}

// reconcileActive adds to d (and to the covered spans) every part of the
// activation intervals implied by the recorded epochs and state events that
// is not covered yet. Active time is the measure of the union of:
//
//   - per epoch E with samples: [start_E, end_E) where end_E = last sample of
//     E if E's "final" sample was seen (the ateom tore the activation down),
//     else max(last sample of E, min(first closing state event in [start_E,
//     start of the next epoch), last sample of E + gap cap)) (crash, lost
//     worker: no final sample);
//   - per RUNNING event R: [R, next state event) when that event closes the
//     activation (suspending, suspended, crashed, deleting, deleted,
//     reverting), else (still open, or the close is missing/late) [R, last
//     sample of the epochs belonging to R). When epochs belong to R, the end
//     is capped at their last sample (+ gap cap unless they all have a final
//     sample), and it never extends past the start of a later epoch.
//
// Intervals only grow as inputs arrive and coverage is a union, so the
// result is idempotent and independent of arrival order up to the bounded
// error of a close paired before its real predecessor arrived (≤ gap cap
// when samples exist). A lost or late close costs at most one gap cap.
func (u *TaskUsage) reconcileActive(d *Delta) {
	st := &u.State
	gapCap := st.SampleGapCap
	if gapCap <= 0 {
		gapCap = DefaultSampleGapCap
	}
	type epochSpan struct {
		start, last time.Time
		final       bool
	}
	epochs := make([]epochSpan, 0, len(st.LastSample))
	for e, p := range st.LastSample {
		epochs = append(epochs, epochSpan{start: u.epochStart(e), last: p.Time, final: st.EpochFinal[e]})
	}
	slices.SortFunc(epochs, func(a, b epochSpan) int { return a.start.Compare(b.start) })

	for i, ep := range epochs {
		var next time.Time
		for _, o := range epochs[i+1:] {
			if o.start.After(ep.start) {
				next = o.start
				break
			}
		}
		end := ep.last
		for _, m := range st.Marks {
			if ep.final {
				break
			}
			if m.Time.Before(ep.start) || !isClosingState(m.State) {
				continue
			}
			if !next.IsZero() && !m.Time.Before(next) {
				break
			}
			ce := m.Time
			if lim := ep.last.Add(gapCap); ce.After(lim) {
				ce = lim
			}
			if ce.After(end) {
				end = ce
			}
			break
		}
		u.cover(d, ep.start, end)
	}

	for i, m := range st.Marks {
		if m.State != StateRunning {
			continue
		}
		var lastEv time.Time
		belongs, final := false, true
		for _, ep := range epochs {
			if !ep.start.Before(m.Time.Add(-runningMatchBefore)) && !ep.start.After(m.Time.Add(runningMatchAfter)) {
				belongs = true
				final = final && ep.final
				if ep.last.After(lastEv) {
					lastEv = ep.last
				}
			}
		}
		var end time.Time
		switch {
		case i+1 < len(st.Marks) && isClosingState(st.Marks[i+1].State):
			end = st.Marks[i+1].Time
			lim := lastEv.Add(gapCap)
			if final {
				lim = lastEv
			}
			if belongs && end.After(lim) {
				end = lim
			}
			// A later activation's epoch inside [R, close) proves the close
			// belongs to it (this activation's own close has not arrived).
			for _, ep := range epochs {
				if ep.start.After(m.Time.Add(runningMatchAfter)) && ep.start.Before(end) {
					end = ep.start
					break
				}
			}
		case belongs:
			end = lastEv
		default:
			continue
		}
		u.cover(d, m.Time, end)
	}
}

// cover adds the part of [from, to) not yet covered to d (split at UTC
// midnight) and merges [from, to) into the covered spans.
func (u *TaskUsage) cover(d *Delta, from, to time.Time) {
	st := &u.State
	if from.Before(st.ActiveFloor) {
		from = st.ActiveFloor
	}
	if !to.After(from) {
		return
	}
	from, to = from.UTC(), to.UTC()
	added := false
	add := func(a, b time.Time) {
		added = true
		forEachDay(a, b, func(date string, x, y time.Time) {
			d.day(date).ActiveSeconds += y.Sub(x).Seconds()
		})
	}
	cur := from
	for _, s := range st.ActiveSpans {
		if !s.To.After(cur) {
			continue
		}
		if !s.From.Before(to) {
			break
		}
		if s.From.After(cur) {
			add(cur, s.From)
		}
		if s.To.After(cur) {
			cur = s.To
		}
		if !cur.Before(to) {
			break
		}
	}
	if cur.Before(to) {
		add(cur, to)
	}
	if !added {
		return
	}

	spans := append(st.ActiveSpans, Span{From: from, To: to})
	slices.SortFunc(spans, func(a, b Span) int { return a.From.Compare(b.From) })
	merged := spans[:1]
	for _, s := range spans[1:] {
		last := &merged[len(merged)-1]
		if !s.From.After(last.To) {
			if s.To.After(last.To) {
				last.To = s.To
			}
			continue
		}
		merged = append(merged, s)
	}
	for len(merged) > maxActiveSpans {
		st.ActiveFloor = merged[0].To
		merged = merged[1:]
	}
	st.ActiveSpans = merged
}

// addMark records a state event; it reports false for a replay or an event
// older than every kept mark when the list is full.
func (u *TaskUsage) addMark(t time.Time, state string) bool {
	st := &u.State
	t = t.UTC()
	i, _ := slices.BinarySearchFunc(st.Marks, t, func(m StateMark, t time.Time) int { return m.Time.Compare(t) })
	for j := i; j < len(st.Marks) && st.Marks[j].Time.Equal(t); j++ {
		if st.Marks[j].State == state {
			return false
		}
	}
	if len(st.Marks) >= maxStateMarks && i == 0 {
		return false
	}
	st.Marks = slices.Insert(st.Marks, i, StateMark{Time: t, State: state})
	if n := len(st.Marks) - maxStateMarks; n > 0 {
		st.Marks = slices.Delete(st.Marks, 0, n)
	}
	return true
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
// increment. The event is recorded (replays are ignored) and active time is
// re-derived order-independently (see reconcileActive), so a late event still
// counts. State/StateTime track the newest event; storage accrues and a
// closing state resets the low-cpu streak only for an event newer than it.
//
// Persist the record after every Apply* call, even when the returned Delta is
// zero: the call may still have advanced accounting state (last sample per
// epoch, state marks, actor uid, runner session/baseline, LastPullTime,
// WorkspaceBytes, CommandExited). Do not return ErrNoChange based on
// Delta.IsZero().
func (u *TaskUsage) ApplyStateChange(e StateChange) Delta {
	var d Delta
	state := NormalizeState(e.State)
	if e.Time.IsZero() || state == "" || !u.acceptUID(e.ActorUID) {
		return d
	}
	if !u.addMark(e.Time, state) {
		return d
	}
	st := &u.State
	if st.StateTime.IsZero() || e.Time.After(st.StateTime) {
		u.accrueStorage(&d, e.Time)
		if state != StateRunning {
			st.IdleStreakStart = time.Time{}
		}
		st.State, st.StateTime = state, e.Time
	}
	u.reconcileActive(&d)
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
// A report with a non-zero token delta is activity (an agent waiting on an
// LLM call is below the idle cpu threshold): State.LastTokenActivity becomes
// the report time and a running low-cpu streak restarts there. Idle already
// accounted before the report is kept (deltas are never negative); only
// later sample intervals see the restarted streak.
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
	newTokens := false
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
			newTokens = true
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

	if newTokens {
		u.noteTokenActivity(t)
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

// noteTokenActivity records token activity at t and restarts a running
// low-cpu streak there, so the streak never starts before the last
// token-bearing report. Nothing already accounted changes.
func (u *TaskUsage) noteTokenActivity(t time.Time) {
	st := &u.State
	if t.After(st.LastTokenActivity) {
		st.LastTokenActivity = t
	}
	if !st.IdleStreakStart.IsZero() && st.IdleStreakStart.Before(st.LastTokenActivity) {
		st.IdleStreakStart = st.LastTokenActivity
	}
}
