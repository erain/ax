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
	"testing"
	"time"

	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

type capacityReconciler struct {
	calls       int
	available   bool
	err         error
	onReconcile func()
}

func (r *capacityReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, _ ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	r.calls++
	if r.onReconcile != nil {
		r.onReconcile()
	}
	if r.err != nil {
		task.Status.Phase = "Failed"
		return task, r.err
	}
	if task.Status.Phase == "Suspended" {
		return task, nil
	}
	if r.available {
		task.Status.Phase = "Running"
	} else {
		task.Status.Phase = "Pending"
		task.Status.Conditions = []*v1alpha1.Condition{{Type: "Ready", Status: "False", Reason: "WaitingForWorker"}}
	}
	return task, nil
}
func (*capacityReconciler) ReconcileDelete(context.Context, string, string) error { return nil }

func TestSchedulingLifecycle(t *testing.T) {
	for _, action := range []string{"capacity", "suspend", "delete", "permanent-error", "restart"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			st := memory.NewStore()
			t.Cleanup(func() { st.Close() })
			r := &capacityReconciler{}
			s := NewServer(st, Options{Reconciler: r})
			_, err := s.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "queued"}, Spec: &v1alpha1.TaskSpec{Image: "alpine"}}})
			if err != nil {
				t.Fatal(err)
			}
			task, err := s.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Name: "queued"})
			if err != nil || !waitingForWorker(task) {
				t.Fatalf("resume must queue: task=%v error=%v", task, err)
			}
			now := time.Now()
			retries := make(map[string]schedulingRetry)
			s.retryPendingTasks(ctx, now, retries)
			if r.calls != 3 {
				t.Fatalf("calls = %d, want create, resume, retry", r.calls)
			}
			s.retryPendingTasks(ctx, now.Add(time.Second), retries)
			if r.calls != 3 {
				t.Fatal("retried before backoff expired")
			}
			want := "Running"
			switch action {
			case "suspend":
				if _, err := s.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "queued"}); err != nil {
					t.Fatal(err)
				}
				want = "Suspended"
			case "delete":
				if _, err := s.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Name: "queued"}); err != nil {
					t.Fatal(err)
				}
			case "permanent-error":
				r.err = errors.New("permission denied")
				want = "Failed"
			case "restart":
				s = NewServer(st, Options{Reconciler: r})
				retries = make(map[string]schedulingRetry)
			}
			r.available = true
			s.retryPendingTasks(ctx, now.Add(time.Minute), retries)
			task, err = s.GetTask(ctx, &v1alpha1.GetTaskRequest{Name: "queued"})
			if action == "delete" {
				if err == nil {
					t.Fatal("deleted task was recreated")
				}
			} else if err != nil || task.Status.Phase != want {
				t.Fatalf("task=%v err=%v, want %s", task, err, want)
			}
			calls := r.calls
			s.retryPendingTasks(ctx, now.Add(2*time.Minute), retries)
			if r.calls != calls {
				t.Fatal("retried task after it left Pending")
			}
			if len(retries) != 0 {
				t.Fatal("retained obsolete backoff entry")
			}
		})
	}
}

func TestSchedulingBackoff(t *testing.T) {
	ctx := context.Background()
	st := memory.NewStore()
	t.Cleanup(func() { st.Close() })
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "queued"}, Status: &v1alpha1.TaskStatus{Phase: "Pending", Conditions: []*v1alpha1.Condition{{Type: "Ready", Status: "False", Reason: "WaitingForWorker"}}}}
	if err := st.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	r := &capacityReconciler{}
	s := NewServer(st, Options{Reconciler: r})
	retries := make(map[string]schedulingRetry)
	now := time.Now()
	for _, delay := range []time.Duration{5, 10, 20, 30, 30} {
		s.retryPendingTasks(ctx, now, retries)
		next := retries["default/queued"].next
		if next.Sub(now) != delay*time.Second {
			t.Fatalf("backoff = %s, want %ds", next.Sub(now), delay)
		}
		calls := r.calls
		s.retryPendingTasks(ctx, next.Add(-time.Nanosecond), retries)
		if r.calls != calls {
			t.Fatal("early retry")
		}
		now = next
	}
}

// changingLocker simulates suspend/delete winning the resource lock after listing.
type changingLocker struct{ beforeLock func() }

func (l changingLocker) Lock(context.Context, string, string, string) (func(), error) {
	l.beforeLock()
	return func() {}, nil
}

func TestSchedulingRechecksUnderLock(t *testing.T) {
	for _, action := range []string{"suspend", "delete", "cancel"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st := memory.NewStore()
			t.Cleanup(func() { st.Close() })
			task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "queued"}, Status: &v1alpha1.TaskStatus{Phase: "Pending", Conditions: []*v1alpha1.Condition{{Type: "Ready", Status: "False", Reason: "WaitingForWorker"}}}}
			if err := st.SaveTask(ctx, task); err != nil {
				t.Fatal(err)
			}
			r := &capacityReconciler{available: true}
			var locker lock.Locker = changingLocker{beforeLock: func() {
				if action == "delete" {
					if err := st.DeleteTask(ctx, "default", "queued"); err != nil {
						t.Fatal(err)
					}
				} else {
					task.Status.Phase = "Suspended"
					if err := st.UpdateTaskStatus(ctx, "default", "queued", task.Status); err != nil {
						t.Fatal(err)
					}
				}
			}}
			if action == "cancel" {
				cancel()
				locker = lock.NewMemoryLocker()
			}
			s := NewServer(st, Options{Reconciler: r, Locker: locker})
			s.retryPendingTasks(ctx, time.Now(), make(map[string]schedulingRetry))
			if r.calls != 0 {
				t.Fatal("stale or canceled retry resumed a task")
			}
		})
	}
}

func TestSchedulingCancellationPreservesPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := memory.NewStore()
	t.Cleanup(func() { st.Close() })
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "queued"},
		Status: &v1alpha1.TaskStatus{Phase: "Pending", Conditions: []*v1alpha1.Condition{
			{Type: "Ready", Status: "False", Reason: "WaitingForWorker"},
		}},
	}
	if err := st.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	r := &capacityReconciler{err: context.Canceled, onReconcile: cancel}
	s := NewServer(st, Options{Reconciler: r})
	s.retryPendingTasks(ctx, time.Now(), make(map[string]schedulingRetry))
	got, err := st.GetTask(context.Background(), "default", "queued")
	if err != nil || !waitingForWorker(got) {
		t.Fatalf("canceled retry lost pending task: task=%v error=%v", got, err)
	}
	if r.calls != 1 {
		t.Fatalf("reconcile calls = %d, want 1", r.calls)
	}
}
