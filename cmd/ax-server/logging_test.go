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

package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogHandler_JSONCloudLogging(t *testing.T) {
	var buf bytes.Buffer
	h, err := newLogHandler(logFormatJSON, &buf, slog.LevelInfo)
	if err != nil {
		t.Fatal(err)
	}
	l := slog.New(h)
	l.Info("ax.task.usage_summary", "task", "t1", "labels", map[string]string{"agent": "a"}, "cost_total", 0.5)
	l.Warn("w", slog.Group("g", slog.String("level", "x")))
	l.Info("budget threshold crossed", "task", "t1", "level", "warning")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines: %q", len(lines), buf.String())
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatal(err)
	}
	if m["msg"] != "ax.task.usage_summary" || m["severity"] != "INFO" || m["task"] != "t1" || m["cost_total"] != 0.5 {
		t.Errorf("unexpected line: %v", m)
	}
	if _, ok := m["level"]; ok {
		t.Errorf("level key not renamed: %v", m)
	}
	if lbl, _ := m["labels"].(map[string]any); lbl["agent"] != "a" {
		t.Errorf("labels not a JSON object: %v", m["labels"])
	}
	m = nil
	if err := json.Unmarshal([]byte(lines[1]), &m); err != nil {
		t.Fatal(err)
	}
	if m["severity"] != "WARNING" {
		t.Errorf("severity = %v, want WARNING", m["severity"])
	}
	if g, _ := m["g"].(map[string]any); g["level"] != "x" {
		t.Errorf("grouped level attr changed: %v", m["g"])
	}
	// A top-level user attr named "level" stays "level" (no second "severity").
	if n := strings.Count(lines[2], `"severity"`); n != 1 {
		t.Errorf("line has %d severity keys: %s", n, lines[2])
	}
	m = nil
	if err := json.Unmarshal([]byte(lines[2]), &m); err != nil {
		t.Fatal(err)
	}
	if m["severity"] != "INFO" || m["level"] != "warning" {
		t.Errorf("severity = %v, level = %v; want INFO, warning", m["severity"], m["level"])
	}
}

func TestNewLogHandler_TextAndInvalid(t *testing.T) {
	var buf bytes.Buffer
	for _, f := range []string{"", logFormatText} {
		buf.Reset()
		h, err := newLogHandler(f, &buf, slog.LevelInfo)
		if err != nil {
			t.Fatal(err)
		}
		slog.New(h).Info("hello", "k", "v")
		if got := buf.String(); !strings.Contains(got, "level=INFO msg=hello k=v") {
			t.Errorf("format %q: got %q", f, got)
		}
	}
	if _, err := newLogHandler("yaml", &buf, slog.LevelInfo); err == nil {
		t.Error("want error for invalid format")
	}
}
