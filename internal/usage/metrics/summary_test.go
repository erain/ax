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

package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/usage"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines returns the JSON log lines written so far.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if l == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("bad JSON log line %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func captureLogger() (*slog.Logger, *syncBuffer) {
	b := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug})), b
}

func testUsage() *usage.TaskUsage {
	u := usage.NewTaskUsage()
	u.ActiveSeconds = 120
	u.CPUSeconds = 30.5
	u.MemoryGiBSeconds = 60
	u.IdleSeconds = 15
	u.StorageGiBSeconds = 4
	u.WorkspaceBytes = 2048
	u.Tokens = map[string]usage.TokenCounts{
		"gemini-2.5-flash": {Input: 1000, Output: 500, CacheRead: 200, Reasoning: 100},
		"gemini-2.5-pro":   {Input: 10, Output: 5},
		"zero":             {},
	}
	u.Cost = usage.Cost{Currency: "USD", Total: 0.75, Compute: 0.25, Tokens: 0.5, Idle: 0.125, TokensPartiallyUnpriced: true}
	u.State.ActorUID = "uid-123"
	u.UpdateTime = time.Date(2026, 10, 16, 7, 0, 0, 0, time.UTC)
	return u
}

// Acceptance 3: the summary line is emitted with the documented fields.
func TestSummaryLogger_LogEmitsUsageSummary(t *testing.T) {
	logger, buf := captureLogger()
	s := NewSummaryLogger(logger)
	s.Log(context.Background(), ReasonSuspend, TaskSummary{
		Key:    usage.Key{Atespace: "default", Name: "t1"},
		Labels: map[string]string{"agent": "triage", "customer": "acme"},
		Usage:  testUsage(),
	})
	lines := buf.lines(t)
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	l := lines[0]
	want := map[string]any{
		"level":                          "INFO",
		"msg":                            "ax.task.usage_summary",
		"reason":                         "suspend",
		"atespace":                       "default",
		"task":                           "t1",
		"actor_uid":                      "uid-123",
		"active_seconds":                 120.0,
		"cpu_seconds":                    30.5,
		"memory_gib_seconds":             60.0,
		"idle_seconds":                   15.0,
		"storage_gib_seconds":            4.0,
		"workspace_bytes":                2048.0,
		"tokens_total":                   1515.0,
		"cost_currency":                  "USD",
		"cost_total":                     0.75,
		"cost_compute":                   0.25,
		"cost_tokens":                    0.5,
		"cost_storage":                   0.0,
		"cost_idle":                      0.125,
		"cost_tokens_partially_unpriced": true,
		"update_time":                    "2026-10-16T07:00:00Z",
	}
	for k, v := range want {
		if l[k] != v {
			t.Errorf("%s = %#v, want %#v", k, l[k], v)
		}
	}
	labels, _ := l["labels"].(map[string]any)
	if labels["agent"] != "triage" || labels["customer"] != "acme" || len(labels) != 2 {
		t.Errorf("labels = %v", l["labels"])
	}
	tokens, _ := l["tokens"].(map[string]any)
	if len(tokens) != 2 {
		t.Fatalf("tokens = %v, want 2 models (zero entries dropped)", l["tokens"])
	}
	flash, _ := tokens["gemini-2.5-flash"].(map[string]any)
	if flash["input"] != 1000.0 || flash["output"] != 500.0 || flash["cache_read"] != 200.0 ||
		flash["cache_write"] != 0.0 || flash["reasoning"] != 100.0 {
		t.Errorf("tokens[gemini-2.5-flash] = %v", flash)
	}
}

func TestSummaryLogger_NilUsageAndDefaults(t *testing.T) {
	logger, buf := captureLogger()
	NewSummaryLogger(logger).Log(context.Background(), ReasonDelete, TaskSummary{Key: usage.Key{Name: "t2"}})
	l := buf.lines(t)[0]
	if l["atespace"] != "default" || l["task"] != "t2" || l["reason"] != "delete" || l["cpu_seconds"] != 0.0 || l["actor_uid"] != "" {
		t.Errorf("line = %v", l)
	}
	if labels, ok := l["labels"].(map[string]any); !ok || len(labels) != 0 {
		t.Errorf("labels = %#v, want {}", l["labels"])
	}
	if _, ok := l["update_time"]; ok {
		t.Error("update_time must be omitted when unset")
	}
	if NewSummaryLogger(nil).logger == nil {
		t.Error("nil logger must default")
	}
}

func TestSummaryLogger_Sweep(t *testing.T) {
	logger, buf := captureLogger()
	s := NewSummaryLogger(logger)
	ctx := context.Background()
	list := func(context.Context) ([]TaskSummary, error) {
		return []TaskSummary{
			{Key: usage.Key{Atespace: "default", Name: "a"}, Usage: testUsage()},
			{Key: usage.Key{Atespace: "default", Name: "b"}},
		}, nil
	}
	if err := s.Sweep(ctx, list); err != nil {
		t.Fatal(err)
	}
	lines := buf.lines(t)
	if len(lines) != 2 || lines[0]["task"] != "a" || lines[1]["task"] != "b" ||
		lines[0]["reason"] != ReasonPeriodic || lines[1]["reason"] != ReasonPeriodic {
		t.Errorf("lines = %v", lines)
	}
	boom := errors.New("boom")
	if err := s.Sweep(ctx, func(context.Context) ([]TaskSummary, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Errorf("Sweep error = %v, want boom", err)
	}
}

func TestSummaryLogger_RunPeriodic(t *testing.T) {
	logger, buf := captureLogger()
	s := NewSummaryLogger(logger)
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	calls := 0
	list := func(context.Context) ([]TaskSummary, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 2 {
			return nil, errors.New("store down")
		}
		return []TaskSummary{{Key: usage.Key{Atespace: "default", Name: "run"}, Usage: testUsage()}}, nil
	}
	done := make(chan struct{})
	go func() {
		s.RunPeriodic(ctx, 5*time.Millisecond, list)
		close(done)
	}()
	deadline := time.After(5 * time.Second)
	for {
		var summaries, warnings int
		for _, l := range buf.lines(t) {
			switch l["msg"] {
			case SummaryMessage:
				summaries++
			case "usage summary sweep failed":
				warnings++
			}
		}
		if summaries >= 2 && warnings >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: %d summaries, %d warnings", summaries, warnings)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RunPeriodic did not return after cancel")
	}
}

func TestSummaryLogger_RunPeriodicDefaultInterval(t *testing.T) {
	// interval <= 0 uses the 10 min default: nothing is logged quickly, and
	// the loop still stops on cancel.
	logger, buf := captureLogger()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		NewSummaryLogger(logger).RunPeriodic(ctx, 0, func(context.Context) ([]TaskSummary, error) {
			t.Error("list called before the default interval")
			return nil, nil
		})
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	if n := len(buf.lines(t)); n != 0 {
		t.Errorf("%d lines logged", n)
	}
	if DefaultSummaryInterval != 10*time.Minute {
		t.Errorf("DefaultSummaryInterval = %v", DefaultSummaryInterval)
	}
}
