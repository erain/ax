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

package server_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/internal/usage/metrics"
	"github.com/google/ax/internal/usage/puller"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
)

// fakeRunner serves GET /usage with a switchable session / exit state.
type fakeRunner struct {
	srv  *httptest.Server
	addr netip.AddrPort
	hits atomic.Int64

	mu       sync.Mutex
	session  string
	exited   bool
	exitCode int
}

func newFakeRunner(t *testing.T, session string) *fakeRunner {
	t.Helper()
	r := &fakeRunner{session: session}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != puller.UsagePath {
			http.NotFound(w, req)
			return
		}
		r.hits.Add(1)
		r.mu.Lock()
		session, exited, code := r.session, r.exited, r.exitCode
		r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"session":%q,"reportTime":%q,"commandExited":%t,"exitCode":%d,"tokens":{"gemini-2.5-flash":{"input":100,"output":50}}}`,
			session, time.Now().UTC().Format(time.RFC3339Nano), exited, code)
	}))
	t.Cleanup(r.srv.Close)
	r.addr = netip.MustParseAddrPort(r.srv.Listener.Addr().String())
	return r
}

func (r *fakeRunner) set(session string, exited bool, code int) {
	r.mu.Lock()
	r.session, r.exited, r.exitCode = session, exited, code
	r.mu.Unlock()
}

func (r *fakeRunner) ip() string { return r.addr.Addr().String() }

// completionReconciler is a fake reconciler counting suspends and resumes; a
// resume assigns workerIP. delay slows each reconcile, failSuspends makes the
// next n suspends fail.
type completionReconciler struct {
	workerIP string
	delay    time.Duration

	mu           sync.Mutex
	suspends     int
	resumes      int
	failSuspends int
}

func (r *completionReconciler) Reconcile(_ context.Context, task *v1alpha1.Task, _ ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if task.Status.Phase == "Suspended" {
		if r.failSuspends > 0 {
			r.failSuspends--
			return task, errors.New("substrate says no")
		}
		r.suspends++
		task.Status.WorkerIp = ""
		return task, nil
	}
	r.resumes++
	task.Status.Phase = "Running"
	task.Status.WorkerIp = r.workerIP
	return task, nil
}

func (r *completionReconciler) ReconcileDelete(context.Context, string, string) error { return nil }

func (r *completionReconciler) counts() (suspends, resumes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.suspends, r.resumes
}

func withOnCompletion(p v1alpha1.CompletionPolicy) taskOpt {
	return func(t *v1alpha1.Task) { t.Spec.OnCompletion = p }
}

// sweep runs one puller sweep and waits for the completions it triggered.
func sweep(e *finopsEnv, p *puller.Puller) {
	p.Sweep(context.Background())
	e.srv.WaitCompletions()
}

// A runner report with commandExited for a SUSPEND_ON_EXIT task suspends the
// actor once (Flush first, lock free), sets phase Completed with
// Completed=True/SuspendedOnExit (exit code in the message), Idle False and a
// "completed" summary. Later writes do not suspend again.
func TestCompletion_SuspendOnExit(t *testing.T) {
	runner := newFakeRunner(t, "s1")
	rec := &completionReconciler{workerIP: runner.ip()}
	e := newFinopsEnv(t, envOptions{reconciler: rec})
	p := e.srv.NewUsagePuller(puller.Options{Port: int(runner.addr.Port())})
	e.saveTask(t, "default", "t1", "Running", withWorker(runner.ip()), withLabels("agent", "triage"),
		withOnCompletion(v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT))

	// Still running: nothing happens.
	sweep(e, p)
	if got := e.stored(t, "default", "t1").GetStatus().GetPhase(); got != "Running" {
		t.Fatalf("phase before exit = %s, want Running", got)
	}

	runner.set("s1", true, 3)
	sweep(e, p)

	stored := e.stored(t, "default", "t1")
	if got := stored.GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Fatalf("phase = %s, want Completed", got)
	}
	if stored.GetStatus().GetWorkerIp() != "" {
		t.Errorf("workerIp = %q, want cleared by the suspend", stored.GetStatus().GetWorkerIp())
	}
	requireCondition(t, stored, server.ConditionCompleted, budget.StatusTrue, server.ReasonSuspendedOnExit)
	if msg := condition(stored, server.ConditionCompleted).GetMessage(); !strings.Contains(msg, "code 3") || !strings.Contains(msg, "s1") {
		t.Errorf("Completed message = %q, want exit code 3 and session s1", msg)
	}
	requireCondition(t, stored, v1alpha1.ConditionIdle, budget.StatusFalse, budget.ReasonNotRunning)
	if s, _ := rec.counts(); s != 1 {
		t.Errorf("suspends = %d, want 1", s)
	}
	sums := e.logs.summaries(t, metrics.ReasonCompleted, "t1")
	if len(sums) != 1 || sums[0]["tokens_total"] != float64(150) {
		t.Errorf("completed summaries = %v, want one with tokens_total 150", sums)
	}
	// The read path shows it too.
	if got := e.get(t, "default", "t1").GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Errorf("GetTask phase = %s", got)
	}

	// Completed tasks are not pulled; further writes (late ingest, sweeps)
	// do not suspend again.
	hits := runner.hits.Load()
	sweep(e, p)
	if runner.hits.Load() != hits {
		t.Errorf("runner contacted after completion")
	}
	key := usage.Key{Atespace: "default", Name: "t1"}
	if err := e.srv.UsageStore().Update(context.Background(), key, func(u *usage.TaskUsage) error {
		u.State.LastPullTime = time.Now()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.UsageSweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.srv.WaitCompletions()
	if s, _ := rec.counts(); s != 1 {
		t.Errorf("suspends after later writes = %d, want 1", s)
	}
	if n := len(e.logs.summaries(t, metrics.ReasonCompleted, "t1")); n != 1 {
		t.Errorf("completed summaries = %d, want 1", n)
	}
}

// KEEP and unspecified policies are only marked Idle (T09): no suspend, no
// Completed condition.
func TestCompletion_KeepOnlyIdle(t *testing.T) {
	runner := newFakeRunner(t, "s1")
	runner.set("s1", true, 0)
	rec := &completionReconciler{}
	e := newFinopsEnv(t, envOptions{reconciler: rec})
	p := e.srv.NewUsagePuller(puller.Options{Port: int(runner.addr.Port())})
	e.saveTask(t, "default", "keep", "Running", withWorker(runner.ip()), withOnCompletion(v1alpha1.CompletionPolicy_KEEP))
	e.saveTask(t, "default", "unset", "Running", withWorker(runner.ip()))

	sweep(e, p)
	if err := e.srv.UsageSweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.srv.WaitCompletions()

	for _, name := range []string{"keep", "unset"} {
		stored := e.stored(t, "default", name)
		if got := stored.GetStatus().GetPhase(); got != "Running" {
			t.Errorf("%s: phase = %s, want Running", name, got)
		}
		requireCondition(t, stored, v1alpha1.ConditionIdle, budget.StatusTrue, budget.ReasonCommandExited)
		if c := condition(stored, server.ConditionCompleted); c != nil {
			t.Errorf("%s: unexpected Completed condition %v", name, c)
		}
	}
	if s, _ := rec.counts(); s != 0 {
		t.Errorf("suspends = %d, want 0", s)
	}
	if n := len(e.logs.summaries(t, metrics.ReasonCompleted, "keep")) + len(e.logs.summaries(t, metrics.ReasonCompleted, "unset")); n != 0 {
		t.Errorf("completed summaries = %d, want 0", n)
	}
}

// Resuming a Completed task works like resuming a Suspended one. The runner
// session that already completed is not completed again (gVisor restores the
// process tree, so the same session keeps reporting commandExited); a new
// session whose command exits completes the task again.
func TestCompletion_ResumeCompleted(t *testing.T) {
	runner := newFakeRunner(t, "s1")
	runner.set("s1", true, 0)
	rec := &completionReconciler{workerIP: runner.ip()}
	e := newFinopsEnv(t, envOptions{reconciler: rec})
	p := e.srv.NewUsagePuller(puller.Options{Port: int(runner.addr.Port())})
	e.saveTask(t, "default", "t1", "Running", withWorker(runner.ip()), withOnCompletion(v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT))
	ctx := context.Background()

	sweep(e, p)
	if got := e.stored(t, "default", "t1").GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Fatalf("phase = %s, want Completed", got)
	}

	resp, err := e.srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "t1"})
	if err != nil {
		t.Fatalf("ResumeTask on a Completed task: %v", err)
	}
	if resp.GetStatus().GetPhase() != "Running" || resp.GetStatus().GetWorkerIp() != runner.ip() {
		t.Fatalf("resume response phase/ip = %s/%s", resp.GetStatus().GetPhase(), resp.GetStatus().GetWorkerIp())
	}
	stored := e.stored(t, "default", "t1")
	requireCondition(t, stored, server.ConditionCompleted, budget.StatusFalse, server.ReasonResumed)
	if s, r := rec.counts(); s != 1 || r != 1 {
		t.Errorf("suspends/resumes = %d/%d, want 1/1", s, r)
	}

	// Same session still reports commandExited: stays Running (Idle).
	sweep(e, p)
	sweep(e, p)
	stored = e.stored(t, "default", "t1")
	if got := stored.GetStatus().GetPhase(); got != "Running" {
		t.Fatalf("phase after resume + same-session pulls = %s, want Running", got)
	}
	requireCondition(t, stored, v1alpha1.ConditionIdle, budget.StatusTrue, budget.ReasonCommandExited)

	// A new runner session (fresh container) resets the flag ...
	runner.set("s2", false, 0)
	sweep(e, p)
	stored = e.stored(t, "default", "t1")
	if got := stored.GetStatus().GetPhase(); got != "Running" {
		t.Fatalf("phase with a running new session = %s, want Running", got)
	}
	requireCondition(t, stored, v1alpha1.ConditionIdle, budget.StatusFalse, budget.ReasonActive)

	// ... and completes again once its command exits.
	runner.set("s2", true, 1)
	sweep(e, p)
	stored = e.stored(t, "default", "t1")
	if got := stored.GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Fatalf("phase after new session exited = %s, want Completed", got)
	}
	requireCondition(t, stored, server.ConditionCompleted, budget.StatusTrue, server.ReasonSuspendedOnExit)
	if msg := condition(stored, server.ConditionCompleted).GetMessage(); !strings.Contains(msg, "code 1") || !strings.Contains(msg, "s2") {
		t.Errorf("Completed message = %q", msg)
	}
	if s, _ := rec.counts(); s != 2 {
		t.Errorf("suspends = %d, want 2", s)
	}
	if n := len(e.logs.summaries(t, metrics.ReasonCompleted, "t1")); n != 2 {
		t.Errorf("completed summaries = %d, want 2", n)
	}
}

// Many concurrent writes reporting the exit, periodic sweeps and a second
// server replica sharing the stores and the lock: exactly one suspend, one
// summary; every Flush ran without the task lock.
func TestCompletion_ConcurrentReportsSuspendOnce(t *testing.T) {
	rec := &completionReconciler{delay: 20 * time.Millisecond}
	e := newFinopsEnv(t, envOptions{reconciler: rec})
	fl := &recordingFlusher{locker: e.locker}
	e.srv.SetUsageFlusher(fl)
	replica := server.NewServer(e.tasks, server.Options{
		Locker:     e.locker,
		Reconciler: rec,
		Usage:      &server.UsageOptions{Store: e.usage, Flusher: fl},
	})
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"), withOnCompletion(v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT))
	key := usage.Key{Atespace: "default", Name: "t1"}

	var wg sync.WaitGroup
	start := time.Now()
	for i := range 16 {
		srv := e.srv
		if i%2 == 1 {
			srv = replica
		}
		wg.Go(func() {
			now := start.Add(time.Duration(i) * time.Second)
			if err := srv.UsageStore().Update(context.Background(), key, func(u *usage.TaskUsage) error {
				u.ApplyRunnerReport(usage.RunnerReport{Session: "s1", ReportTime: now, CommandExited: true, ExitCode: 0}, now)
				return nil
			}); err != nil {
				t.Error(err)
			}
		})
		if i%4 == 0 {
			wg.Go(func() { _ = srv.UsageSweep(context.Background()) })
		}
	}
	wg.Wait()
	e.srv.WaitCompletions()
	replica.WaitCompletions()

	if got := e.stored(t, "default", "t1").GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Fatalf("phase = %s, want Completed", got)
	}
	if s, _ := rec.counts(); s != 1 {
		t.Errorf("suspends = %d, want exactly 1", s)
	}
	if n := len(e.logs.summaries(t, metrics.ReasonCompleted, "t1")); n > 1 {
		t.Errorf("completed summaries on the primary = %d, want <= 1", n)
	}
	for _, c := range fl.get() {
		if !c.lockFree {
			t.Errorf("Flush ran with the task lock held: %+v", c)
		}
	}
	if len(fl.get()) == 0 {
		t.Error("no Flush before the completion suspend")
	}
}

// A failed suspend persists nothing (phase stays Running, no Completed
// condition, no summary); the task completes on a later attempt.
func TestCompletion_SuspendFailureRetries(t *testing.T) {
	rec := &completionReconciler{failSuspends: 1}
	clock := &fakeClock{now: time.Now()}
	e := newFinopsEnvWithClock(t, rec, clock.Now)
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"), withOnCompletion(v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT))
	key := usage.Key{Atespace: "default", Name: "t1"}
	report := func() {
		now := clock.Now()
		if err := e.srv.UsageStore().Update(context.Background(), key, func(u *usage.TaskUsage) error {
			u.ApplyRunnerReport(usage.RunnerReport{Session: "s1", ReportTime: now, CommandExited: true}, now)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		e.srv.WaitCompletions()
	}

	report()
	stored := e.stored(t, "default", "t1")
	if stored.GetStatus().GetPhase() != "Running" || condition(stored, server.ConditionCompleted) != nil {
		t.Fatalf("after failed suspend: phase %s, conditions %v", stored.GetStatus().GetPhase(), stored.GetStatus().GetConditions())
	}
	if n := len(e.logs.summaries(t, metrics.ReasonCompleted, "t1")); n != 0 {
		t.Errorf("completed summaries = %d, want 0", n)
	}

	// Within the backoff: no new attempt.
	clock.Add(5 * time.Second)
	report()
	if got := e.stored(t, "default", "t1").GetStatus().GetPhase(); got != "Running" {
		t.Fatalf("phase within backoff = %s, want Running", got)
	}
	// After the backoff the next write completes the task.
	clock.Add(time.Minute)
	report()
	if got := e.stored(t, "default", "t1").GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Fatalf("phase after retry = %s, want Completed", got)
	}
	if s, _ := rec.counts(); s != 1 {
		t.Errorf("suspends = %d, want 1", s)
	}
}

// completeViaReport drives a SUSPEND_ON_EXIT task t1 to Completed with a
// commandExited runner report written straight into the usage store.
func completeViaReport(t *testing.T, e *finopsEnv, now time.Time) {
	t.Helper()
	key := usage.Key{Atespace: "default", Name: "t1"}
	if err := e.srv.UsageStore().Update(context.Background(), key, func(u *usage.TaskUsage) error {
		u.ApplyRunnerReport(usage.RunnerReport{Session: "s1", ReportTime: now, CommandExited: true}, now)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	e.srv.WaitCompletions()
	if got := e.stored(t, "default", "t1").GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Fatalf("phase = %s, want Completed", got)
	}
}

// SuspendTask on a Completed task is a no-op: no reconcile, the stored task
// is unchanged and the response keeps phase Completed and Completed=True.
func TestCompletion_SuspendCompletedIsNoop(t *testing.T) {
	rec := &completionReconciler{}
	clock := &fakeClock{now: time.Now()}
	e := newFinopsEnvWithClock(t, rec, clock.Now)
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"), withOnCompletion(v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT))
	completeViaReport(t, e, clock.Now())
	before := e.stored(t, "default", "t1")
	if s, _ := rec.counts(); s != 1 {
		t.Fatalf("suspends after completion = %d, want 1", s)
	}

	clock.Add(time.Minute)
	resp, err := e.srv.SuspendTask(context.Background(), &v1alpha1.SuspendTaskRequest{Name: "t1"})
	if err != nil {
		t.Fatalf("SuspendTask on a Completed task: %v", err)
	}
	if got := resp.GetStatus().GetPhase(); got != v1alpha1.PhaseCompleted {
		t.Errorf("response phase = %s, want Completed", got)
	}
	requireCondition(t, resp, server.ConditionCompleted, budget.StatusTrue, server.ReasonSuspendedOnExit)
	if s, r := rec.counts(); s != 1 || r != 0 {
		t.Errorf("suspends/resumes = %d/%d, want 1/0 (no reconcile)", s, r)
	}
	if after := e.stored(t, "default", "t1"); !proto.Equal(before, after) {
		t.Errorf("stored task changed:\nbefore %v\nafter  %v", before, after)
	}
}

// Resuming a Completed task stamps Completed=False/Resumed with the FinOps
// clock, not the wall clock.
func TestCompletion_ResumeUsesClock(t *testing.T) {
	rec := &completionReconciler{workerIP: "10.0.0.5"}
	start := time.Date(2021, 3, 4, 5, 6, 7, 0, time.UTC)
	clock := &fakeClock{now: start}
	e := newFinopsEnvWithClock(t, rec, clock.Now)
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.5"), withOnCompletion(v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT))
	completeViaReport(t, e, start)

	clock.Add(time.Hour)
	if _, err := e.srv.ResumeTask(context.Background(), &v1alpha1.ResumeTaskRequest{Name: "t1"}); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	stored := e.stored(t, "default", "t1")
	requireCondition(t, stored, server.ConditionCompleted, budget.StatusFalse, server.ReasonResumed)
	if got, want := condition(stored, server.ConditionCompleted).GetLastTransitionTime().AsTime(), start.Add(time.Hour); !got.Equal(want) {
		t.Errorf("Completed lastTransitionTime = %v, want %v (FinOps clock)", got, want)
	}
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// newFinopsEnvWithClock is a minimal finops env with a fake clock.
func newFinopsEnvWithClock(t *testing.T, rec server.Reconciler, now func() time.Time) *finopsEnv {
	t.Helper()
	base := newFinopsEnv(t, envOptions{reconciler: rec})
	base.srv = server.NewServer(base.tasks, server.Options{
		Locker:     base.locker,
		Reconciler: rec,
		Usage: &server.UsageOptions{
			Store:   base.usage,
			Summary: metrics.NewSummaryLogger(loggerTo(base.logs)),
			Logger:  loggerTo(base.logs),
			Clock:   now,
		},
	})
	return base
}

func loggerTo(b *syncBuffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
