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

package runner

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func newTestTracker(t *testing.T) *usageTracker {
	t.Helper()
	dir := t.TempDir()
	return newUsageTracker(filepath.Join(dir, "ws", usageFileRel), filepath.Join(dir, "ws"))
}

func postRecord(t *testing.T, tr *usageTracker, method, body string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	tr.handleRecord(rec, httptest.NewRequest(method, UsageRecordPath, strings.NewReader(body)))
	return rec.Code, strings.TrimSpace(rec.Body.String())
}

func TestUsageRecord(t *testing.T) {
	tr := newTestTracker(t)
	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
		wantBody string
	}{
		{
			name:     "first record",
			method:   http.MethodPost,
			body:     `{"model":"gemini-2.5-flash","inputTokens":100,"outputTokens":20,"cacheReadTokens":5,"cacheWriteTokens":1,"reasoningTokens":7,"requestId":"r1"}`,
			wantCode: http.StatusOK,
			wantBody: `{"recorded":true}`,
		},
		{
			name:     "duplicate request id ignored",
			method:   http.MethodPost,
			body:     `{"model":"gemini-2.5-flash","inputTokens":100,"outputTokens":20,"requestId":"r1"}`,
			wantCode: http.StatusOK,
			wantBody: `{"recorded":false}`,
		},
		{
			name:     "records without request id always count",
			method:   http.MethodPost,
			body:     `{"model":"gemini-2.5-flash","inputTokens":10,"outputTokens":2}`,
			wantCode: http.StatusOK,
			wantBody: `{"recorded":true}`,
		},
		{
			name:     "provider cost",
			method:   http.MethodPost,
			body:     `{"model":"gemini-2.5-pro","inputTokens":1000,"outputTokens":100,"cost":{"amount":0.0125,"currency":"USD"},"requestId":"r2"}`,
			wantCode: http.StatusOK,
			wantBody: `{"recorded":true}`,
		},
		{
			name:     "missing model counts as unknown",
			method:   http.MethodPost,
			body:     `{"inputTokens":3}`,
			wantCode: http.StatusOK,
			wantBody: `{"recorded":true}`,
		},
		{
			name:     "negative tokens rejected",
			method:   http.MethodPost,
			body:     `{"model":"m","inputTokens":-1}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "malformed json rejected",
			method:   http.MethodPost,
			body:     `{"model":`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "GET not allowed",
			method:   http.MethodGet,
			wantCode: http.StatusMethodNotAllowed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := postRecord(t, tr, tt.method, tt.body)
			if code != tt.wantCode {
				t.Fatalf("status = %d, want %d (body %q)", code, tt.wantCode, body)
			}
			if tt.wantBody != "" && body != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
		})
	}

	want := map[string]*TokenUsage{
		"gemini-2.5-flash": {Input: 110, Output: 22, CacheRead: 5, CacheWrite: 1, Reasoning: 7},
		"gemini-2.5-pro":   {Input: 1000, Output: 100, CostAmount: 0.0125, CostCurrency: "USD"},
		"unknown":          {Input: 3},
	}
	if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
		t.Errorf("tokens = %s, want %s", jsonString(got), jsonString(want))
	}
}

func TestUsageReport_JSONShape(t *testing.T) {
	tr := newTestTracker(t)
	fixed := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	tr.now = func() time.Time { return fixed }
	tr.session = "abc123"
	tr.record(UsageRecord{Model: "gemini-2.5-flash", InputTokens: 1200, OutputTokens: 340, CacheReadTokens: 100, ReasoningTokens: 50})
	tr.commandDone(3)

	rec := httptest.NewRecorder()
	tr.handleReport(rec, httptest.NewRequest(http.MethodGet, UsagePath, nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("GET /usage = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	// The usage file itself lives in the workspace and is counted.
	info, err := os.Stat(tr.path)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf(`{"session":"abc123","reportTime":"2026-10-16T12:00:00Z","commandExited":true,"exitCode":3,"workspaceBytes":%d,`+
		`"tokens":{"gemini-2.5-flash":{"input":1200,"output":340,"cacheRead":100,"cacheWrite":0,"reasoning":50}}}`, info.Size())
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Errorf("GET /usage body:\n got %s\nwant %s", got, want)
	}

	rec = httptest.NewRecorder()
	tr.handleReport(rec, httptest.NewRequest(http.MethodPost, UsagePath, nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /usage = %d, want 405", rec.Code)
	}
}

func TestUsageTracker_PersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".ax", "usage.json")

	first := newUsageTracker(path, dir)
	first.record(UsageRecord{Model: "gemini-2.5-flash", InputTokens: 10, OutputTokens: 5, RequestID: "a"})
	first.applyOTLP([]spanUsage{{id: "t1s1", model: "gemini-2.5-flash", delta: tokenDelta{input: 1, output: 1}}}, nil)
	first.commandDone(0)

	second := newUsageTracker(path, dir)
	got, want := second.report(), first.report()
	if got.Session == want.Session || got.Session == "" {
		t.Errorf("session = %q after restart, want a new non-empty id (was %q)", got.Session, want.Session)
	}
	if !reflect.DeepEqual(got.Tokens, want.Tokens) {
		t.Errorf("tokens after restart = %s, want %s", jsonString(got.Tokens), jsonString(want.Tokens))
	}
	if got.CommandExited {
		t.Error("commandExited carried over a restart; it belongs to the old session")
	}

	// No temp files are left behind by the atomic writes.
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 || entries[0].Name() != "usage.json" {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("counter dir contains %v, want only usage.json", names)
	}
}

func TestUsageTracker_CorruptFileMovedAside(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "usage.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	tr := newUsageTracker(path, "")
	if n := len(tr.report().Tokens); n != 0 {
		t.Errorf("tokens = %d models, want fresh counters", n)
	}
	if _, err := os.Stat(path + ".corrupt"); err != nil {
		t.Errorf("corrupt file not moved aside: %v", err)
	}
}

func TestUsageTracker_WorkspaceBytesCached(t *testing.T) {
	ws := t.TempDir()
	tr := newUsageTracker(filepath.Join(t.TempDir(), "usage.json"), ws)
	now := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	tr.now = func() time.Time { return now }

	if err := os.MkdirAll(filepath.Join(ws, "out"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ws, "out", "a.txt"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := tr.report().WorkspaceBytes; got != 100 {
		t.Fatalf("workspaceBytes = %d, want 100", got)
	}

	if err := os.WriteFile(filepath.Join(ws, "b.txt"), make([]byte, 50), 0o644); err != nil {
		t.Fatal(err)
	}
	now = now.Add(30 * time.Second)
	if got := tr.report().WorkspaceBytes; got != 100 {
		t.Errorf("workspaceBytes within TTL = %d, want cached 100", got)
	}
	now = now.Add(31 * time.Second)
	if got := tr.report().WorkspaceBytes; got != 150 {
		t.Errorf("workspaceBytes after TTL = %d, want 150", got)
	}
}

func TestLRU(t *testing.T) {
	l := newLRU(2)
	steps := []struct {
		key  string
		want bool
	}{
		{"a", true},
		{"b", true},
		{"a", false}, // refreshes a
		{"c", true},  // evicts b
		{"a", false},
		{"b", true},
	}
	for i, s := range steps {
		if got := l.add(s.key); got != s.want {
			t.Errorf("step %d add(%q) = %v, want %v", i, s.key, got, s.want)
		}
	}
}

func jsonString(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
