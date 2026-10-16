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

package redis_test

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisstore "github.com/google/ax/internal/store/redis"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) *redisstore.Store {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return redisstore.NewStore(client, redisstore.Options{})
}

func mustSelector(t *testing.T, s string) v1alpha1.LabelSelector {
	t.Helper()
	sel, err := v1alpha1.ParseLabelSelector(s)
	if err != nil {
		t.Fatal(err)
	}
	return sel
}

func names(tasks []*v1alpha1.Task) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		out[i] = t.GetMetadata().GetName()
	}
	return out
}

func TestStore_TaskLabelsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	labels := map[string]string{"agent": "coder", "customer": "acme", "ax.io/team": "infra"}

	if err := s.SaveTask(ctx, &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "t1", Labels: maps.Clone(labels)},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTask(ctx, "default", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(got.GetMetadata().GetLabels(), labels) {
		t.Fatalf("GetTask labels = %v, want %v", got.GetMetadata().GetLabels(), labels)
	}

	// Status updates rewrite the record and must keep the labels.
	if err := s.UpdateTaskStatus(ctx, "default", "t1", &v1alpha1.TaskStatus{Phase: "Running"}); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetTask(ctx, "default", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Running" || !maps.Equal(got.GetMetadata().GetLabels(), labels) {
		t.Fatalf("after UpdateTaskStatus: phase %q labels %v", got.GetStatus().GetPhase(), got.GetMetadata().GetLabels())
	}

	list, err := s.ListTasks(ctx, "default", v1alpha1.LabelSelector{}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || !maps.Equal(list[0].GetMetadata().GetLabels(), labels) {
		t.Fatalf("ListTasks = %v", list)
	}
}

func TestStore_ListTasksSelector(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Enough tasks to span several index scan batches. Saved oldest first, so
	// ListTasks (newest first) returns them in descending index order.
	const n = 600
	for i := range n {
		agent := "reviewer"
		if i%3 == 0 {
			agent = "coder"
		}
		atespace := "default"
		if i%2 == 1 {
			atespace = "other"
		}
		if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{
			Name:     fmt.Sprintf("t%03d", i),
			Atespace: atespace,
			Labels:   map[string]string{"agent": agent, "customer": fmt.Sprintf("c%d", i%5)},
		}}); err != nil {
			t.Fatal(err)
		}
		// Index scores are save times in nanoseconds; keep them distinct.
		time.Sleep(time.Microsecond)
	}

	// Count matches across all pages.
	listAll := func(atespace, selector string) []*v1alpha1.Task {
		t.Helper()
		var all []*v1alpha1.Task
		for offset := int64(0); ; offset += 100 {
			page, err := s.ListTasks(ctx, atespace, mustSelector(t, selector), 100, offset)
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, page...)
			if len(page) < 100 {
				return all
			}
		}
	}

	tests := []struct {
		atespace, selector string
		want               int
	}{
		{"", "", n},
		{"*", "agent=coder", n / 3},
		{"", "agent=reviewer", n - n/3},
		{"default", "agent=coder", n / 6},
		{"other", "agent=coder", n / 6},
		{"", "agent=coder,customer=c0", n / 15},
		{"", "agent=nobody", 0},
		{"", "missing=x", 0},
	}
	for _, tt := range tests {
		got := listAll(tt.atespace, tt.selector)
		if len(got) != tt.want {
			t.Errorf("ListTasks(%q, %q) returned %d tasks, want %d", tt.atespace, tt.selector, len(got), tt.want)
		}
		sel := mustSelector(t, tt.selector)
		seen := make(map[string]bool)
		for _, task := range got {
			if !sel.Matches(task.GetMetadata().GetLabels()) {
				t.Errorf("ListTasks(%q, %q) returned non-matching task %s %v", tt.atespace, tt.selector, task.GetMetadata().GetName(), task.GetMetadata().GetLabels())
			}
			if tt.atespace != "" && tt.atespace != "*" && task.GetMetadata().GetAtespace() != tt.atespace {
				t.Errorf("ListTasks(%q, %q) returned task from atespace %q", tt.atespace, tt.selector, task.GetMetadata().GetAtespace())
			}
			key := task.GetMetadata().GetAtespace() + "/" + task.GetMetadata().GetName()
			if seen[key] {
				t.Errorf("ListTasks(%q, %q) returned %s twice across pages", tt.atespace, tt.selector, key)
			}
			seen[key] = true
		}
	}

	// Offset and limit apply to matching tasks, newest first.
	page, err := s.ListTasks(ctx, "", mustSelector(t, "agent=coder"), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Matching indexes (multiples of 3) newest first: 597, 594, 591, ...
	if got, want := names(page), []string{"t594", "t591"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("paged ListTasks = %v, want %v", got, want)
	}

	// Without a selector, pagination is unchanged.
	page, err = s.ListTasks(ctx, "", v1alpha1.LabelSelector{}, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := names(page), []string{"t598", "t597"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("unfiltered ListTasks = %v, want %v", got, want)
	}
}
