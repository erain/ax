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

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const schedulingInterval = 5 * time.Second

type schedulingRetry struct {
	next  time.Time
	delay time.Duration
}

// RetryPendingTasks retries capacity-blocked tasks until ctx is canceled. The
// store is the source of truth, so pending resumes survive server restarts.
// Run once per server, using the same resource locks as the task RPCs.
func (s *Server) RetryPendingTasks(ctx context.Context) {
	if s.reconciler == nil {
		return
	}
	retries := make(map[string]schedulingRetry)
	ticker := time.NewTicker(schedulingInterval)
	defer ticker.Stop()
	for {
		s.retryPendingTasks(ctx, time.Now(), retries)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func waitingForWorker(task *v1alpha1.Task) bool {
	if task.Status == nil || task.Status.Phase != "Pending" {
		return false
	}
	for _, c := range task.Status.Conditions {
		if c.Type == "Ready" && c.Status == "False" && c.Reason == "WaitingForWorker" {
			return true
		}
	}
	return false
}

func (s *Server) retryPendingTasks(ctx context.Context, now time.Time, retries map[string]schedulingRetry) {
	tasks, err := s.store.ListTasks(ctx, "", math.MaxInt64, 0)
	if err != nil {
		slog.Error("listing pending tasks", "error", err)
		return
	}
	pending := make(map[string]bool)
	for _, task := range tasks {
		if ctx.Err() != nil {
			return
		}
		if !waitingForWorker(task) {
			continue
		}
		key := task.Metadata.Atespace + "/" + task.Metadata.Name
		pending[key] = true
		retry := retries[key]
		if now.Before(retry.next) {
			continue
		}
		// Bound each attempt below the default resource-lock lease (30 seconds).
		attemptCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := s.retryPendingTask(attemptCtx, task.Metadata.Atespace, task.Metadata.Name)
		cancel()
		if err != nil {
			slog.Error("retrying pending task", "task", key, "error", err)
		}
		if retry.delay == 0 {
			retry.delay = schedulingInterval
		} else {
			retry.delay = min(2*retry.delay, 30*time.Second)
		}
		retry.next = now.Add(retry.delay)
		retries[key] = retry
	}
	for key := range retries {
		if !pending[key] {
			delete(retries, key)
		}
	}
}

func (s *Server) retryPendingTask(ctx context.Context, atespace, name string) error {
	unlock, err := s.locker.Lock(ctx, "task", atespace, name)
	if err != nil {
		return err
	}
	defer unlock()
	// A suspend, delete, or another server's retry may have won the lock.
	task, err := s.store.GetTask(ctx, atespace, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil || !waitingForWorker(task) {
		return err
	}
	reconciled, reconcileErr := s.reconciler.Reconcile(ctx, task, s.fetchWorkspaces(ctx, atespace, task)...)
	// Shutdown or an attempt timeout must not turn a queued resume into a failure.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if reconciled != nil {
		task.Status = reconciled.Status
	}
	if err := s.store.UpdateTaskStatus(ctx, atespace, name, task.Status); err != nil {
		return err
	}
	return reconcileErr
}
