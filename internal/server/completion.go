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

// spec.onCompletion SUSPEND_ON_EXIT.
//
// Trigger: the commit observer (every committed usage write) and the periodic
// UsageSweep look at the committed record. When it says the runner's command
// exited, an unlocked pre-check reads the task (phase Running, policy
// SUSPEND_ON_EXIT, session not already completed) and starts one background
// completion per task (in-flight dedup). The writer (puller, ingest request)
// never waits for the suspend.
//
// Completion follows SuspendTask and T08's lock order: Flush before the lock,
// then the task lock, then a re-check on fresh reads of the task and the usage
// record, the suspend reconcile, phase Completed with condition
// Completed=True/SuspendedOnExit, budget/Idle re-evaluation, one persist and
// a "completed" summary line. The locked re-check makes concurrent triggers
// (two pulls, several replicas) suspend at most once.
//
// Runner sessions: the usage record keeps CommandExited per runner session
// (T03). Agent Substrate v0.5.0 restores only the filesystem on resume
// (gVisor --fs-restore-image-path; processes are not checkpointed), so a
// resume starts a fresh process tree: the runner gets a new session and
// re-runs the task command, and commandExited resets until that command exits
// again. Until the first pull of the new session, the usage record still holds
// the old session's commandExited=true. The Completed condition therefore
// records the completed runner session, ResumeTask keeps it (Completed=False,
// Resumed), and a session that already completed is never completed again.
// When the re-run command of the new session exits, the task completes again.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/internal/usage/metrics"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// ConditionCompleted is the condition set when a task is suspended on
	// completion (spec.onCompletion SUSPEND_ON_EXIT).
	ConditionCompleted = "Completed"
	// ReasonSuspendedOnExit: the command exited and the actor was suspended.
	ReasonSuspendedOnExit = "SuspendedOnExit"
	// ReasonResumed: a completed task was resumed (Completed=False).
	ReasonResumed = "Resumed"

	// sessionMarker precedes the completed runner session in the Completed
	// condition message ("... (runner session <id>)").
	sessionMarker = "(runner session "

	// completionTimeout bounds one background completion (flush, lock wait,
	// suspend reconcile, persist).
	completionTimeout = 3 * time.Minute
	// completionLockTimeout bounds the wait for the task lock; on timeout the
	// next usage write or sweep retries.
	completionLockTimeout = 30 * time.Second
	// completionRetryBackoff delays new attempts after a failed suspend.
	completionRetryBackoff = 30 * time.Second
)

// completions tracks background completions per task.
type completions struct {
	wg       sync.WaitGroup
	mu       sync.Mutex
	inFlight map[usage.Key]bool
	retryAt  map[usage.Key]time.Time
}

// start reserves key; it returns false when a completion for key is already
// running or a failed one is backing off.
func (c *completions) start(key usage.Key, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inFlight == nil {
		c.inFlight = map[usage.Key]bool{}
		c.retryAt = map[usage.Key]time.Time{}
	}
	if c.inFlight[key] || now.Before(c.retryAt[key]) {
		return false
	}
	c.inFlight[key] = true
	c.wg.Add(1)
	return true
}

func (c *completions) done(key usage.Key, failedAt time.Time) {
	c.mu.Lock()
	delete(c.inFlight, key)
	if failedAt.IsZero() {
		delete(c.retryAt, key)
	} else {
		c.retryAt[key] = failedAt.Add(completionRetryBackoff)
	}
	c.mu.Unlock()
	c.wg.Done()
}

// completedSession returns the runner session recorded by the task's
// Completed condition (ok=false when the task never completed).
func completedSession(task *v1alpha1.Task) (session string, ok bool) {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() != ConditionCompleted {
			continue
		}
		msg := c.GetMessage()
		i := strings.LastIndex(msg, sessionMarker)
		if i < 0 {
			return "", true
		}
		return strings.TrimSuffix(msg[i+len(sessionMarker):], ")"), true
	}
	return "", false
}

// shouldComplete reports whether task must be suspended on completion given
// its usage record u.
func shouldComplete(task *v1alpha1.Task, u *usage.TaskUsage) bool {
	if task == nil || u == nil || !u.State.CommandExited {
		return false
	}
	if task.GetSpec().GetOnCompletion() != v1alpha1.CompletionPolicy_SUSPEND_ON_EXIT {
		return false
	}
	if task.GetStatus().GetPhase() != "Running" {
		return false
	}
	if prev, ok := completedSession(task); ok && prev == u.State.RunnerSession {
		return false // this session already completed; stale record right after a resume
	}
	return true
}

// setCompletedCondition sets the Completed condition (replacing the slice
// element, never mutating a possibly shared message).
func setCompletedCondition(task *v1alpha1.Task, status, reason, message string, now time.Time) {
	c := &v1alpha1.Condition{Type: ConditionCompleted, Status: status, Reason: reason, Message: message, LastTransitionTime: timestamppb.New(now)}
	for i, old := range task.Status.Conditions {
		if old.GetType() == ConditionCompleted {
			conds := append([]*v1alpha1.Condition(nil), task.Status.Conditions...)
			conds[i] = c
			task.Status.Conditions = conds
			return
		}
	}
	task.Status.Conditions = append(task.Status.Conditions, c)
}

// markResumed flips a True Completed condition of a task being resumed to
// False/Resumed. The completed runner session stays in the message, so that
// session is not completed again. The caller holds the task lock.
func (s *Server) markResumed(task *v1alpha1.Task) {
	if task == nil || task.Status == nil {
		return
	}
	for _, c := range task.Status.Conditions {
		if c.GetType() != ConditionCompleted || c.GetStatus() != budget.StatusTrue {
			continue
		}
		msg := c.GetMessage()
		if i := strings.LastIndex(msg, sessionMarker); i >= 0 {
			msg = "resumed after completion " + msg[i:]
		} else {
			msg = "resumed after completion"
		}
		now := time.Now
		if s.fin != nil && s.fin.clock != nil {
			now = s.fin.clock
		}
		setCompletedCondition(task, budget.StatusFalse, ReasonResumed, msg, now())
		return
	}
}

// maybeComplete starts a background completion of key when the committed
// usage record u says the command exited and task (nil = read it) is eligible.
// It never blocks on the task lock.
func (s *Server) maybeComplete(ctx context.Context, key usage.Key, task *v1alpha1.Task, u *usage.TaskUsage) {
	f := s.fin
	if f == nil || u == nil || !u.State.CommandExited {
		return
	}
	key = normKey(key)
	if task == nil {
		var err error
		if task, err = s.store.GetTask(ctx, key.Atespace, key.Name); err != nil {
			return
		}
	}
	if !shouldComplete(task, u) {
		return
	}
	if !f.completions.start(key, f.clock()) {
		return
	}
	go func() {
		var failedAt time.Time
		defer func() { f.completions.done(key, failedAt) }()
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), completionTimeout)
		defer cancel()
		if err := s.completeTask(cctx, key); err != nil {
			failedAt = f.clock()
			f.logger.WarnContext(cctx, "suspend on completion failed; retrying on a later usage write",
				"atespace", key.Atespace, "task", key.Name, "error", err)
		}
	}()
}

// completeTask suspends key on completion (same path as SuspendTask) when the
// fresh, locked re-check still says so. Not completing (no longer eligible,
// lock busy) is not an error.
func (s *Server) completeTask(ctx context.Context, key usage.Key) error {
	f := s.fin
	// Flush before the lock (T08 lock order): the hooks it triggers take it.
	s.flushBeforeLock(ctx, key.Atespace, key.Name)

	lockCtx, lockCancel := context.WithTimeout(ctx, completionLockTimeout)
	unlock, err := s.locker.Lock(lockCtx, "task", key.Atespace, key.Name)
	lockCancel()
	if err != nil {
		f.logger.DebugContext(ctx, "suspend on completion deferred: task lock busy", "atespace", key.Atespace, "task", key.Name, "error", err)
		return nil
	}
	defer unlock()

	task, err := s.store.GetTask(ctx, key.Atespace, key.Name)
	if err != nil {
		return nil // deleted meanwhile
	}
	u, err := f.get(ctx, key)
	if err != nil {
		return fmt.Errorf("reading usage: %w", err)
	}
	if !shouldComplete(task, u) {
		return nil
	}
	exitCode, session := u.State.ExitCode, u.State.RunnerSession

	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = "Suspended"
	if s.reconciler != nil {
		reconciled, err := s.reconciler.Reconcile(ctx, task, s.fetchWorkspaces(ctx, key.Atespace, task)...)
		if err != nil {
			if substrate.IsTransientError(err) {
				return fmt.Errorf("suspending task on substrate (transient): %w", err)
			}
			return fmt.Errorf("suspending task on substrate: %w", err)
		}
		task.Status = reconciled.Status
	}
	now := f.clock()
	task.Status.Phase = v1alpha1.PhaseCompleted
	setCompletedCondition(task, budget.StatusTrue, ReasonSuspendedOnExit,
		fmt.Sprintf("command exited with code %d; actor suspended on completion %s%s)", exitCode, sessionMarker, session), now)
	ev := s.evaluateLocked(ctx, task)
	if err := s.store.UpdateTaskStatus(ctx, key.Atespace, key.Name, task.Status); err != nil {
		return fmt.Errorf("updating task status: %w", err)
	}
	s.emitCrossings(ctx, task, ev)
	if !ev.ok {
		ev.usage = u
	}
	s.logSummary(ctx, metrics.ReasonCompleted, task, ev.usage)
	f.logger.InfoContext(ctx, "task completed: suspended on exit", "atespace", key.Atespace, "task", key.Name,
		"exit_code", exitCode, "runner_session", session)
	return nil
}
