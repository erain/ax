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

package server

// FinOps integration: usage accounting (ingestor + puller), pricing, soft
// budgets / Idle conditions, OTel metrics and usage summary logs.
//
// Data flow:
//
//	OTLP /v1/logs (ingest) ─┐                       ┌─> metrics.Recorder.OnDelta
//	runner GET /usage (puller)┴─> usage.Store.Update ─┤   (hooks fan-out, non-zero deltas)
//	                                                 └─> budget check (hooks fan-out AND every
//	                                                     committed write, incl. zero-delta state)
//
// Budget check: an unlocked pre-check evaluates the snapshot against the
// task's stored conditions; only when a transition is due it takes the task
// lock, re-reads the task and the usage record (R1: never trusts the hook's
// snapshot), evaluates again and persists the conditions with
// UpdateTaskStatus. Crossings (metric + slog + summary line) are emitted only
// after a successful persist, so each fires once per transition.
//
// Lock order (deadlock freedom): the task lock may be held while calling the
// task store and usage Store.Get/List/Delete, never while calling
// usage Store.Update, the puller (Flush) or usage hooks. Update callbacks
// never take the task lock, and hooks / commit notifications run after Update
// returns. SuspendTask/DeleteTask therefore Flush before taking the task lock.

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/internal/usage/ingest"
	"github.com/google/ax/internal/usage/metrics"
	"github.com/google/ax/internal/usage/puller"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// UsageMetrics receives usage deltas and budget threshold crossings
// (*metrics.Recorder implements it).
type UsageMetrics interface {
	usage.Hooks
	OnThreshold(ctx context.Context, key usage.Key, dimension, level string)
}

// UsageFlusher pulls a task's latest runner usage synchronously
// (*puller.Puller implements it).
type UsageFlusher interface {
	Flush(ctx context.Context, task *v1alpha1.Task) error
}

// UsageOptions enables usage accounting in the server. Only Store is
// required.
type UsageOptions struct {
	// Store persists per-task usage (required).
	Store usage.Store
	// Pricer prices usage deltas (nil = usage without cost).
	Pricer usage.Pricer
	// RateCard returns the current rate card for idle accounting and budget
	// settings (nil = built-in defaults). Called per evaluation, so rate-card
	// reloads apply.
	RateCard func() *cost.RateCard
	// Metrics records usage metrics and threshold crossings (may be nil).
	Metrics UsageMetrics
	// Labels resolves task labels for metrics (nil = a LabelCache over the
	// task store). The server invalidates entries of deleted tasks.
	Labels *LabelCache
	// Summary writes ax.task.usage_summary lines (nil = slog.Default()).
	Summary *metrics.SummaryLogger
	// Flusher pulls runner usage before Suspend/Delete (may be nil; set by
	// NewUsagePuller).
	Flusher UsageFlusher
	// Clock is used for budget evaluation (nil = time.Now).
	Clock func() time.Time
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

const (
	// budgetLockTimeout bounds how long a usage-triggered budget check waits
	// for the task lock (e.g. while a Suspend reconciles). On timeout the
	// check is skipped: the RPC holding the lock evaluates itself, and the
	// next write or periodic sweep retries.
	budgetLockTimeout = 2 * time.Second
	// budgetCheckTimeout bounds one usage-triggered budget check.
	budgetCheckTimeout = 5 * time.Second
	// flushTimeout bounds a Flush before Suspend/Delete (the puller has its
	// own, shorter per-pull timeout).
	flushTimeout = 10 * time.Second
	// maxRunningPages caps the paging loop of ListRunningTasks.
	maxRunningPages = 10000
)

// runningPageSize is the ListTasks page size used by ListRunningTasks (a var
// for tests).
var runningPageSize int64 = 500

type finops struct {
	store    usage.Store // commit-observing wrapper of opts.Store
	pricer   usage.Pricer
	rateCard func() *cost.RateCard
	metrics  UsageMetrics
	labels   *LabelCache
	summary  *metrics.SummaryLogger
	hooks    usage.Hooks
	clock    func() time.Time
	logger   *slog.Logger

	mu      sync.RWMutex
	flusher UsageFlusher

	// checked holds, per task, the record of the last write whose budget
	// check the commit observer already ran, so the hook that follows the
	// same write skips its redundant pre-check (one entry per task).
	checkedMu sync.Mutex
	checked   map[usage.Key]*usage.TaskUsage
}

func (f *finops) markChecked(key usage.Key, u *usage.TaskUsage) {
	f.checkedMu.Lock()
	defer f.checkedMu.Unlock()
	if f.checked == nil {
		f.checked = map[usage.Key]*usage.TaskUsage{}
	}
	f.checked[normKey(key)] = u
}

// takeChecked reports (and forgets) whether the commit observer already
// checked the write that produced snapshot u.
func (f *finops) takeChecked(key usage.Key, u *usage.TaskUsage) bool {
	if u == nil {
		return false
	}
	f.checkedMu.Lock()
	defer f.checkedMu.Unlock()
	key = normKey(key)
	if f.checked[key] != u {
		return false
	}
	delete(f.checked, key)
	return true
}

func (f *finops) forgetChecked(key usage.Key) {
	f.checkedMu.Lock()
	delete(f.checked, normKey(key))
	f.checkedMu.Unlock()
}

func newFinops(s *Server, o *UsageOptions) *finops {
	f := &finops{
		pricer:   o.Pricer,
		rateCard: o.RateCard,
		metrics:  o.Metrics,
		labels:   o.Labels,
		summary:  o.Summary,
		clock:    o.Clock,
		logger:   o.Logger,
		flusher:  o.Flusher,
	}
	if f.labels == nil {
		f.labels = NewLabelCache(s.store, DefaultLabelCacheTTL)
	}
	if f.summary == nil {
		f.summary = metrics.NewSummaryLogger(nil)
	}
	if f.clock == nil {
		f.clock = time.Now
	}
	if f.logger == nil {
		f.logger = slog.Default()
	}
	f.store = &observedStore{Store: o.Store, onCommit: func(ctx context.Context, key usage.Key, u *usage.TaskUsage) {
		s.checkBudget(ctx, key, nil)
		f.markChecked(key, u)
	}}
	var hooks hookFanout
	if f.metrics != nil {
		hooks = append(hooks, f.metrics)
	}
	hooks = append(hooks, budgetHook{s: s})
	f.hooks = hooks
	return f
}

func (f *finops) card() *cost.RateCard {
	if f.rateCard == nil {
		return nil
	}
	return f.rateCard()
}

func (f *finops) budgetConfig() budget.Config {
	return budget.ConfigFromRateCard(f.card())
}

func (f *finops) accounting() usage.AccountingConfig {
	if c := f.card(); c != nil {
		return c.AccountingConfig()
	}
	return usage.AccountingConfig{}
}

// get returns the usage record of key, or nil when there is none yet.
func (f *finops) get(ctx context.Context, key usage.Key) (*usage.TaskUsage, error) {
	u, err := f.store.Get(ctx, key)
	if errors.Is(err, usage.ErrNotFound) {
		return nil, nil
	}
	return u, err
}

func (f *finops) getFlusher() UsageFlusher {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.flusher
}

// observedStore notifies onCommit after every Update that wrote the record,
// including writes whose delta is zero (state-only changes such as a runner
// reporting commandExited), which usage hooks never see (R2).
//
// onCommit also receives the committed record: the same pointer the writer
// (ingestor, puller) passes to its hooks as the snapshot.
type observedStore struct {
	usage.Store
	onCommit func(ctx context.Context, key usage.Key, u *usage.TaskUsage)
}

func (o *observedStore) Update(ctx context.Context, key usage.Key, fn func(*usage.TaskUsage) error) error {
	skipped := false
	var last *usage.TaskUsage
	err := o.Store.Update(ctx, key, func(u *usage.TaskUsage) error {
		ferr := fn(u)
		skipped = errors.Is(ferr, usage.ErrNoChange)
		last = u
		return ferr
	})
	if err == nil && !skipped {
		o.onCommit(ctx, key, last)
	}
	return err
}

// hookFanout calls every hook in order.
type hookFanout []usage.Hooks

func (h hookFanout) OnDelta(ctx context.Context, key usage.Key, d usage.Delta, c usage.Cost, u *usage.TaskUsage) {
	for _, x := range h {
		x.OnDelta(ctx, key, d, c, u)
	}
}

// budgetHook evaluates budgets on every non-zero usage delta. Writes made
// through UsageStore() were already checked by the commit observer (on a
// fresh read, at least as new as the hook's snapshot), so the hook skips
// those and only checks deltas from other writers.
type budgetHook struct{ s *Server }

func (b budgetHook) OnDelta(ctx context.Context, key usage.Key, _ usage.Delta, _ usage.Cost, u *usage.TaskUsage) {
	if b.s.fin.takeChecked(key, u) {
		return
	}
	b.s.checkBudget(ctx, key, u)
}

// UsageEnabled reports whether the server was configured with UsageOptions.
func (s *Server) UsageEnabled() bool { return s.fin != nil }

// UsageStore returns the usage store the ingestor and puller must write
// through (it triggers budget evaluation on every committed write), or nil
// when usage is disabled.
func (s *Server) UsageStore() usage.Store {
	if s.fin == nil {
		return nil
	}
	return s.fin.store
}

// UsageHooks returns the hooks fan-out (metrics + budget evaluation) for the
// ingestor and puller, or nil when usage is disabled.
func (s *Server) UsageHooks() usage.Hooks {
	if s.fin == nil {
		return nil
	}
	return s.fin.hooks
}

// SetUsageFlusher sets the flusher used before Suspend/Delete. Call it before
// serving requests.
func (s *Server) SetUsageFlusher(fl UsageFlusher) {
	if s.fin == nil {
		return
	}
	s.fin.mu.Lock()
	s.fin.flusher = fl
	s.fin.mu.Unlock()
}

// IngestHandler returns the HTTP handler for the OTLP listener: POST
// /v1/logs (Substrate ate.actor.* events) and GET /healthz. Without usage
// only /healthz is served.
func (s *Server) IngestHandler() http.Handler {
	mux := http.NewServeMux()
	if s.fin != nil {
		mux.Handle("/v1/logs", ingest.NewHandler(ingest.Options{
			Store:      s.fin.store,
			Tasks:      s.store,
			Pricer:     s.fin.pricer,
			Hooks:      s.fin.hooks,
			Accounting: s.fin.accounting,
			Logger:     s.fin.logger,
		}))
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	return mux
}

// NewUsagePuller returns a runner usage puller wired to the server (task
// lister, usage store, pricer, hooks) and registers it as the flusher. o
// supplies the remaining settings (Interval, Port, Client, ...). It returns
// nil when usage is disabled. Start it with go p.Run(ctx).
func (s *Server) NewUsagePuller(o puller.Options) *puller.Puller {
	if s.fin == nil {
		return nil
	}
	o.Tasks = s
	o.Store = s.fin.store
	o.Pricer = s.fin.pricer
	o.Hooks = s.fin.hooks
	if o.Logger == nil {
		o.Logger = s.fin.logger
	}
	p := puller.New(o)
	s.SetUsageFlusher(p)
	return p
}

// ListRunningTasks returns the tasks with phase Running across all atespaces
// (puller.TaskLister). It pages through the store's all-atespaces listing;
// a task created or deleted concurrently may be missed or seen twice in one
// call (duplicates are dropped), which the next sweep corrects.
func (s *Server) ListRunningTasks(ctx context.Context) ([]*v1alpha1.Task, error) {
	seen := map[usage.Key]bool{}
	var out []*v1alpha1.Task
	for page, offset := 0, int64(0); page < maxRunningPages; page, offset = page+1, offset+runningPageSize {
		tasks, err := s.store.ListTasks(ctx, "", v1alpha1.LabelSelector{}, runningPageSize, offset)
		if err != nil {
			return nil, err
		}
		// Stop on an empty page, not a short one: the Redis store skips
		// index entries whose record is missing, so a short page is not
		// necessarily the last.
		if len(tasks) == 0 {
			break
		}
		for _, t := range tasks {
			if t.GetStatus().GetPhase() != puller.PhaseRunning {
				continue
			}
			k := keyOf(t)
			if seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, t)
		}
	}
	return out, nil
}

// RunUsageSummaries writes a periodic ax.task.usage_summary line for every
// Running task and re-evaluates its budget/Idle conditions (a safety net for
// skipped checks) every interval (metrics.DefaultSummaryInterval when <= 0)
// until ctx is done. Start it in a goroutine.
func (s *Server) RunUsageSummaries(ctx context.Context, interval time.Duration) {
	if s.fin == nil {
		return
	}
	if interval <= 0 {
		interval = metrics.DefaultSummaryInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.UsageSweep(ctx); err != nil && ctx.Err() == nil {
				s.fin.logger.WarnContext(ctx, "usage summary sweep failed", "error", err)
			}
		}
	}
}

// UsageSweep is one RunUsageSummaries iteration.
func (s *Server) UsageSweep(ctx context.Context) error {
	if s.fin == nil {
		return nil
	}
	tasks, err := s.ListRunningTasks(ctx)
	if err != nil {
		return err
	}
	all, err := s.fin.store.List(ctx, "")
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		key := keyOf(t)
		u := all[key]
		s.checkBudget(ctx, key, u)
		s.fin.summary.Log(ctx, metrics.ReasonPeriodic, metrics.TaskSummary{Key: key, Labels: t.GetMetadata().GetLabels(), Usage: u})
	}
	return nil
}

func keyOf(t *v1alpha1.Task) usage.Key {
	atespace := t.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	return usage.Key{Atespace: atespace, Name: t.GetMetadata().GetName()}
}

// --- read-time merge ---

// mergeUsage folds usage record u (may be nil) into task's status in place:
// status.usage, status.actorUid (when the task does not record one yet) and
// fresh budget/Idle conditions. Nothing is persisted.
func (s *Server) mergeUsage(task *v1alpha1.Task, u *usage.TaskUsage) {
	if s.fin == nil || task == nil {
		return
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	if u != nil {
		task.Status.Usage = usage.ToStatus(u)
		if task.Status.ActorUid == "" {
			task.Status.ActorUid = u.State.ActorUID
		}
	}
	res := budget.Evaluate(task, u, s.fin.budgetConfig(), s.fin.clock())
	task.Status.Conditions, _ = budget.MergeConditions(task.Status.Conditions, res.Conditions())
}

// attachUsage reads task's usage record and merges it (read path). Usage
// store errors are logged and the task is returned without usage.
func (s *Server) attachUsage(ctx context.Context, task *v1alpha1.Task) {
	if s.fin == nil || task == nil {
		return
	}
	u, err := s.fin.get(ctx, keyOf(task))
	if err != nil {
		s.fin.logger.WarnContext(ctx, "reading task usage failed", "atespace", task.GetMetadata().GetAtespace(),
			"task", task.GetMetadata().GetName(), "error", err)
		return
	}
	s.mergeUsage(task, u)
}

// attachUsageList merges usage into tasks with a single usage List call.
func (s *Server) attachUsageList(ctx context.Context, atespace string, tasks []*v1alpha1.Task) {
	if s.fin == nil || len(tasks) == 0 {
		return
	}
	if atespace == "*" {
		atespace = ""
	}
	all, err := s.fin.store.List(ctx, atespace)
	if err != nil {
		s.fin.logger.WarnContext(ctx, "listing task usage failed", "atespace", atespace, "error", err)
		return
	}
	for _, t := range tasks {
		s.mergeUsage(t, all[keyOf(t)])
	}
}

// withUsage returns a copy of task with usage merged (for RPC responses of a
// task that is also persisted).
func (s *Server) withUsage(ctx context.Context, task *v1alpha1.Task) *v1alpha1.Task {
	if s.fin == nil || task == nil {
		return task
	}
	cp := proto.Clone(task).(*v1alpha1.Task)
	s.attachUsage(ctx, cp)
	return cp
}

// --- budget evaluation (write path) ---

// budgetEval is the outcome of evaluateLocked.
type budgetEval struct {
	changed   bool
	crossings []budget.Crossing
	usage     *usage.TaskUsage
	ok        bool // the usage record could be read
}

// evaluateLocked evaluates budgets/Idle for task, whose lock the caller
// holds, against a fresh read of its usage record (R1) and stores changed
// conditions in task.Status (the caller persists them and then calls
// emitCrossings).
func (s *Server) evaluateLocked(ctx context.Context, task *v1alpha1.Task) budgetEval {
	if s.fin == nil || task == nil {
		return budgetEval{}
	}
	u, err := s.fin.get(ctx, keyOf(task))
	if err != nil {
		s.fin.logger.WarnContext(ctx, "budget evaluation: reading usage failed", "atespace", task.GetMetadata().GetAtespace(),
			"task", task.GetMetadata().GetName(), "error", err)
		return budgetEval{}
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	res := budget.Evaluate(task, u, s.fin.budgetConfig(), s.fin.clock())
	merged, changed := budget.MergeConditions(task.Status.Conditions, res.Conditions())
	ev := budgetEval{usage: u, ok: true, changed: changed}
	if changed {
		task.Status.Conditions = merged
		ev.crossings = res.Crossings
	}
	return ev
}

// emitCrossings reports the crossings of a persisted evaluation: a slog line
// and a threshold metric per crossing, plus one summary line.
func (s *Server) emitCrossings(ctx context.Context, task *v1alpha1.Task, ev budgetEval) {
	if s.fin == nil || !ev.changed || len(ev.crossings) == 0 {
		return
	}
	key := keyOf(task)
	for _, c := range ev.crossings {
		s.fin.logger.InfoContext(ctx, "budget threshold crossed",
			"atespace", key.Atespace, "task", key.Name,
			"dimension", c.Dimension, "level", string(c.Level),
			"value", c.Value, "limit", c.Limit, "percent", c.Percent, "message", c.Message)
		if s.fin.metrics != nil {
			s.fin.metrics.OnThreshold(ctx, key, c.Dimension, string(c.Level))
		}
	}
	s.fin.summary.Log(ctx, metrics.ReasonThreshold, metrics.TaskSummary{Key: key, Labels: task.GetMetadata().GetLabels(), Usage: ev.usage})
}

// logSummary writes a suspend/delete summary line for task.
func (s *Server) logSummary(ctx context.Context, reason string, task *v1alpha1.Task, u *usage.TaskUsage) {
	if s.fin == nil {
		return
	}
	s.fin.summary.Log(ctx, reason, metrics.TaskSummary{Key: keyOf(task), Labels: task.GetMetadata().GetLabels(), Usage: u})
}

// checkBudget evaluates the budget/Idle conditions of key after a usage write
// and persists transitions. snap (may be nil) is only used for the unlocked
// pre-check; the decision is made under the task lock on fresh reads.
func (s *Server) checkBudget(ctx context.Context, key usage.Key, snap *usage.TaskUsage) {
	f := s.fin
	if f == nil {
		return
	}
	// The write is committed: finish the check even if the caller's context
	// (an ingest request, a nearly spent pull timeout) ends, within a bound.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budgetCheckTimeout)
	defer cancel()
	task, err := s.store.GetTask(ctx, key.Atespace, key.Name)
	if err != nil {
		return // deleted, or the store is unavailable: the next write retries
	}
	if snap == nil {
		if snap, err = f.get(ctx, key); err != nil {
			f.logger.DebugContext(ctx, "budget check: reading usage failed", "atespace", key.Atespace, "task", key.Name, "error", err)
			return
		}
	}
	res := budget.Evaluate(task, snap, f.budgetConfig(), f.clock())
	if _, changed := budget.MergeConditions(task.GetStatus().GetConditions(), res.Conditions()); !changed {
		return
	}

	lockCtx, lockCancel := context.WithTimeout(ctx, budgetLockTimeout)
	unlock, err := s.locker.Lock(lockCtx, "task", key.Atespace, key.Name)
	lockCancel()
	if err != nil {
		f.logger.DebugContext(ctx, "budget check skipped: task lock busy", "atespace", key.Atespace, "task", key.Name, "error", err)
		return
	}
	defer unlock()

	task, err = s.store.GetTask(ctx, key.Atespace, key.Name)
	if err != nil {
		return
	}
	ev := s.evaluateLocked(ctx, task)
	if !ev.changed {
		return
	}
	if err := s.store.UpdateTaskStatus(ctx, key.Atespace, key.Name, task.Status); err != nil {
		f.logger.WarnContext(ctx, "persisting budget conditions failed", "atespace", key.Atespace, "task", key.Name, "error", err)
		return
	}
	s.emitCrossings(ctx, task, ev)
}

// flushUsage pulls the task's latest runner usage (Running tasks only) before
// a Suspend/Delete. It must not be called with the task lock held (the
// resulting hooks take it). Errors are logged, never returned.
func (s *Server) flushUsage(ctx context.Context, task *v1alpha1.Task) {
	if s.fin == nil || task == nil {
		return
	}
	fl := s.fin.getFlusher()
	if fl == nil {
		return
	}
	fctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	if err := fl.Flush(fctx, task); err != nil {
		s.fin.logger.WarnContext(ctx, "flushing runner usage failed", "atespace", task.GetMetadata().GetAtespace(),
			"task", task.GetMetadata().GetName(), "error", err)
	}
}

// deleteUsage removes the usage record of a deleted task and its cached
// labels. Called after store.DeleteTask, so late events find no task and do
// not recreate the record.
func (s *Server) deleteUsage(ctx context.Context, atespace, name string) {
	if s.fin == nil {
		return
	}
	key := usage.Key{Atespace: atespace, Name: name}
	if err := s.fin.store.Delete(ctx, key); err != nil {
		s.fin.logger.WarnContext(ctx, "deleting task usage failed", "atespace", atespace, "task", name, "error", err)
	}
	s.fin.labels.Invalidate(key)
	s.fin.forgetChecked(key)
}

// --- labels ---

// DefaultLabelCacheTTL bounds how long LabelCache serves an entry (labels are
// immutable for a task's lifetime; the TTL covers deletes on other replicas).
const DefaultLabelCacheTTL = 5 * time.Minute

// maxLabelCacheEntries bounds the cache; it is cleared when full.
const maxLabelCacheEntries = 10000

// TaskGetter reads a task (store.Store implements it).
type TaskGetter interface {
	GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
}

// LabelCache resolves task labels for metric attributes from the task store
// with a small TTL cache (metrics.LabelLookup). Lookup failures are not
// cached and yield nil (metric labels "none").
type LabelCache struct {
	tasks TaskGetter
	ttl   time.Duration
	now   func() time.Time

	mu      sync.Mutex
	entries map[usage.Key]labelEntry
}

type labelEntry struct {
	labels  map[string]string
	expires time.Time
}

var _ metrics.LabelLookup = (*LabelCache)(nil)

// NewLabelCache returns a LabelCache over tasks (ttl <= 0 → DefaultLabelCacheTTL).
func NewLabelCache(tasks TaskGetter, ttl time.Duration) *LabelCache {
	if ttl <= 0 {
		ttl = DefaultLabelCacheTTL
	}
	return &LabelCache{tasks: tasks, ttl: ttl, now: time.Now, entries: map[usage.Key]labelEntry{}}
}

func normKey(k usage.Key) usage.Key {
	if k.Atespace == "" {
		k.Atespace = "default"
	}
	return k
}

// Labels returns the labels of the task key (callers must not mutate the map).
func (c *LabelCache) Labels(ctx context.Context, key usage.Key) map[string]string {
	key = normKey(key)
	now := c.now()
	c.mu.Lock()
	if e, ok := c.entries[key]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.labels
	}
	c.mu.Unlock()

	t, err := c.tasks.GetTask(ctx, key.Atespace, key.Name)
	if err != nil || t == nil {
		return nil
	}
	labels := maps.Clone(t.GetMetadata().GetLabels())
	c.mu.Lock()
	if len(c.entries) >= maxLabelCacheEntries {
		clear(c.entries)
	}
	c.entries[key] = labelEntry{labels: labels, expires: now.Add(c.ttl)}
	c.mu.Unlock()
	return labels
}

// Invalidate drops the cached labels of key.
func (c *LabelCache) Invalidate(key usage.Key) {
	key = normKey(key)
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// flushBeforeLock reads the task (without its lock) and flushes its runner
// usage. Callers take the task lock afterwards.
func (s *Server) flushBeforeLock(ctx context.Context, atespace, name string) {
	if s.fin == nil || s.fin.getFlusher() == nil {
		return
	}
	task, err := s.store.GetTask(ctx, atespace, name)
	if err != nil {
		return
	}
	s.flushUsage(ctx, task)
}

// afterSuspend emits the crossings of a persisted suspend evaluation and the
// suspend summary line.
func (s *Server) afterSuspend(ctx context.Context, task *v1alpha1.Task, ev budgetEval) {
	if s.fin == nil {
		return
	}
	s.emitCrossings(ctx, task, ev)
	u := ev.usage
	if !ev.ok {
		u, _ = s.fin.get(ctx, keyOf(task))
	}
	s.logSummary(ctx, metrics.ReasonSuspend, task, u)
}

// dropOrphanUsage deletes a leftover usage record for a task that is being
// created (the caller holds the task lock; usage Delete is allowed under it).
func (s *Server) dropOrphanUsage(ctx context.Context, atespace, name string) error {
	if s.fin == nil {
		return nil
	}
	key := usage.Key{Atespace: atespace, Name: name}
	if err := s.fin.store.Delete(ctx, key); err != nil {
		return err
	}
	s.fin.labels.Invalidate(key)
	s.fin.forgetChecked(key)
	return nil
}
