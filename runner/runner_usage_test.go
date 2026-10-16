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

package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
)

// getUsage fetches GET /usage from the probe port.
func getUsage(t *testing.T, port int) runner.UsageReport {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, runner.UsagePath))
	if err != nil {
		t.Fatalf("GET /usage: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /usage = %d", resp.StatusCode)
	}
	var r runner.UsageReport
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decoding /usage: %v", err)
	}
	return r
}

// postUsage sends a JSON usage record to the loopback metadata port.
func postUsage(t *testing.T, port int, body string) {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d%s", port, runner.UsageRecordPath), "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST usage: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST usage = %d", resp.StatusCode)
	}
}

func TestRun_UsageReportsCommandExit(t *testing.T) {
	h := newHarness(t)
	h.task.Spec.Command = []string{"sh", "-c", "exit 3"}
	h.start(t)
	waitHealthy(t, h.cfg.Port)

	postUsage(t, h.cfg.MetadataPort, `{"model":"gemini-2.5-flash","inputTokens":120,"outputTokens":30,"requestId":"call-1"}`)
	postUsage(t, h.cfg.MetadataPort, `{"model":"gemini-2.5-flash","inputTokens":120,"outputTokens":30,"requestId":"call-1"}`)
	h.waitExit(t)

	got := getUsage(t, h.cfg.Port)
	if !got.CommandExited || got.ExitCode != 3 {
		t.Errorf("commandExited=%v exitCode=%d, want true 3", got.CommandExited, got.ExitCode)
	}
	if got.Session == "" || got.ReportTime.IsZero() {
		t.Errorf("missing session or reportTime: %+v", got)
	}
	want := map[string]*runner.TokenUsage{"gemini-2.5-flash": {Input: 120, Output: 30}}
	if !reflect.DeepEqual(got.Tokens, want) {
		t.Errorf("tokens = %+v, want %+v", got.Tokens, want)
	}
	// The counters file lives in the workspace root and is part of workspaceBytes.
	if got.WorkspaceBytes <= 0 {
		t.Errorf("workspaceBytes = %d, want > 0", got.WorkspaceBytes)
	}
	if _, err := os.Stat(filepath.Join(h.cfg.WorkspaceRoot, ".ax", "usage.json")); err != nil {
		t.Errorf("counters not persisted: %v", err)
	}
}

func TestRun_UsageSurvivesRestart(t *testing.T) {
	first := newHarness(t)
	first.start(t)
	waitHealthy(t, first.cfg.Port)
	postUsage(t, first.cfg.MetadataPort, `{"model":"gemini-2.5-pro","inputTokens":7,"outputTokens":3,"reasoningTokens":2}`)
	before := getUsage(t, first.cfg.Port)
	first.cancel()
	first.waitFinished(t, 5*time.Second)

	second := newHarness(t)
	second.cfg.WorkspaceRoot = first.cfg.WorkspaceRoot
	second.start(t)
	waitHealthy(t, second.cfg.Port)
	after := getUsage(t, second.cfg.Port)

	if after.Session == before.Session {
		t.Errorf("session %q unchanged across runner restart", after.Session)
	}
	if !reflect.DeepEqual(after.Tokens, before.Tokens) {
		t.Errorf("tokens after restart = %+v, want %+v", after.Tokens, before.Tokens)
	}
}

func TestRun_InjectsOTLPEnv(t *testing.T) {
	tests := []struct {
		name         string
		otlpAddr     string
		taskEnv      []*v1alpha1.EnvVar
		wantEndpoint string // "loopback" means the receiver's own address
		wantProtocol string
	}{
		{name: "receiver enabled", otlpAddr: "127.0.0.1:0", wantEndpoint: "loopback", wantProtocol: "http/protobuf"},
		{
			name:         "task env wins",
			otlpAddr:     "127.0.0.1:0",
			taskEnv:      []*v1alpha1.EnvVar{{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://collector:4318"}},
			wantEndpoint: "http://collector:4318",
			wantProtocol: "http/protobuf",
		},
		{name: "receiver disabled", otlpAddr: "", wantEndpoint: "", wantProtocol: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run copies spec.env into its own process environment; isolate each
			// case from the developer's environment and from earlier cases.
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
			t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "")
			h := newHarness(t)
			outFile := filepath.Join(t.TempDir(), "env.txt")
			h.cfg.OTLPAddr = tt.otlpAddr
			h.task.Spec.Env = tt.taskEnv
			h.task.Spec.Command = []string{"sh", "-c", `printf '%s\n%s\n' "$OTEL_EXPORTER_OTLP_ENDPOINT" "$OTEL_EXPORTER_OTLP_PROTOCOL" > ` + outFile}
			if exit := h.runUntilCommandExits(t); exit.Err != nil {
				t.Fatalf("command failed: %+v", exit)
			}
			out, err := os.ReadFile(outFile)
			if err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(string(out), "\n")
			endpoint, protocol := lines[0], lines[1]
			if tt.wantEndpoint == "loopback" {
				if !strings.HasPrefix(endpoint, "http://127.0.0.1:") || strings.HasSuffix(endpoint, ":0") {
					t.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT = %q, want the loopback receiver", endpoint)
				}
				// The injected endpoint is the live receiver.
				resp, err := http.Post(endpoint+"/v1/traces", "application/json", strings.NewReader(`{"resourceSpans":[]}`))
				if err != nil {
					t.Fatalf("receiver at %s not reachable: %v", endpoint, err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Errorf("POST %s/v1/traces = %d", endpoint, resp.StatusCode)
				}
			} else if endpoint != tt.wantEndpoint {
				t.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT = %q, want %q", endpoint, tt.wantEndpoint)
			}
			if protocol != tt.wantProtocol {
				t.Errorf("OTEL_EXPORTER_OTLP_PROTOCOL = %q, want %q", protocol, tt.wantProtocol)
			}
		})
	}
}

func TestRun_RejectsNonLoopbackOTLPAddr(t *testing.T) {
	h := newHarness(t)
	h.cfg.OTLPAddr = fmt.Sprintf(":%d", freePort(t))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runner.Run(ctx, h.cfg); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("Run with OTLPAddr %q = %v, want a loopback error", h.cfg.OTLPAddr, err)
	}
}
