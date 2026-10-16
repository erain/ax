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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

var updateGolden = flag.Bool("update", false, "rewrite cmd/ax/testdata/*.golden")

// fakeTaskServer is an in-memory AX server that serves canned tasks (with
// status.usage and conditions already populated, as ax-server returns them).
type fakeTaskServer struct {
	v1alpha1.UnimplementedAXServer

	mu        sync.Mutex
	tasks     []*v1alpha1.Task
	listCalls []*v1alpha1.ListTasksRequest
}

func (f *fakeTaskServer) GetTask(_ context.Context, req *v1alpha1.GetTaskRequest) (*v1alpha1.Task, error) {
	for _, t := range f.tasks {
		if t.GetMetadata().GetAtespace() == req.GetAtespace() && t.GetMetadata().GetName() == req.GetName() {
			return t, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "task %q not found", req.GetName())
}

func (f *fakeTaskServer) ListTasks(_ context.Context, req *v1alpha1.ListTasksRequest) (*v1alpha1.ListTasksResponse, error) {
	f.mu.Lock()
	f.listCalls = append(f.listCalls, proto.Clone(req).(*v1alpha1.ListTasksRequest))
	f.mu.Unlock()
	sel, err := v1alpha1.ParseLabelSelector(req.GetLabelSelector())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var matched []*v1alpha1.Task
	for _, t := range f.tasks {
		if req.GetAtespace() != "" && t.GetMetadata().GetAtespace() != req.GetAtespace() {
			continue
		}
		if sel.Matches(t.GetMetadata().GetLabels()) {
			matched = append(matched, t)
		}
	}
	limit, offset := req.GetLimit(), req.GetOffset()
	if limit <= 0 {
		limit = 50
	}
	if offset > int64(len(matched)) {
		offset = int64(len(matched))
	}
	end := min(offset+limit, int64(len(matched)))
	return &v1alpha1.ListTasksResponse{Tasks: matched[offset:end]}, nil
}

func (f *fakeTaskServer) calls() []*v1alpha1.ListTasksRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*v1alpha1.ListTasksRequest(nil), f.listCalls...)
}

func startFakeTaskServer(t *testing.T, tasks []*v1alpha1.Task) (*fakeTaskServer, string) {
	t.Helper()
	f := &fakeTaskServer{tasks: tasks}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	v1alpha1.RegisterAXServer(srv, f)
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(listener) }()
	return f, listener.Addr().String()
}

// goldenNow is the fixed "current time" of the golden tests.
var goldenNow = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func finopsFixtureTasks() []*v1alpha1.Task {
	created := func(ago time.Duration) *timestamppb.Timestamp { return timestamppb.New(goldenNow.Add(-ago)) }
	ltt := timestamppb.New(goldenNow.Add(-2 * time.Minute))
	return []*v1alpha1.Task{
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "coder-acme", Atespace: "default", CreationTimestamp: created(40 * time.Minute),
				Labels: map[string]string{"agent": "coder", "customer": "acme"}},
			Spec: &v1alpha1.TaskSpec{
				Image:   "us-docker.pkg.dev/demo/ax-task-runner:finops",
				Command: []string{"python3", "/opt/finops-agent/main.py"},
				Budget: &v1alpha1.BudgetSpec{
					ActiveDeadline: durationpb.New(30 * time.Minute),
					MaxTokens:      100000,
					MaxCost:        &v1alpha1.Money{Currency: "USD", Amount: 1},
					WarnAtPercent:  80,
					OnExceeded:     v1alpha1.ExceededAction_WARN,
				},
			},
			Status: &v1alpha1.TaskStatus{
				Phase: "Running", Actor: "coder-acme", WorkerIp: "10.20.3.67", ActorUid: "a1b2c3",
				Usage: &v1alpha1.UsageStatus{
					ActiveDuration:   durationpb.New(25*time.Minute + 12*time.Second),
					IdleDuration:     durationpb.New(6 * time.Minute),
					CpuSeconds:       812.345,
					MemoryGibSeconds: 1520.5,
					WorkspaceBytes:   12_934_144,
					Tokens: []*v1alpha1.ModelTokenUsage{
						{Model: "some-unpriced-model", Input: 500, Output: 100},
						{Model: "gemini-3-flash-preview", Input: 85000, Output: 3000, CacheRead: 20000, CacheWrite: 1000, Reasoning: 1200},
					},
					Cost: &v1alpha1.CostBreakdown{Currency: "USD", Compute: 0.0187, Idle: 0.0031, Tokens: 0.0912,
						Storage: 0.0001, Total: 0.11, TokensPartiallyUnpriced: true},
					UpdateTime: timestamppb.New(goldenNow.Add(-30 * time.Second)),
				},
				Conditions: []*v1alpha1.Condition{
					{Type: "Ready", Status: "True", Reason: "ActorRunning", LastTransitionTime: ltt},
					{Type: v1alpha1.ConditionBudgetWarning, Status: "True", Reason: "ActiveDeadlineWarning", LastTransitionTime: ltt,
						Message: "active 25m12s of 30m0s (84%); tokens 88,600 of 100,000 (88%)"},
					{Type: v1alpha1.ConditionIdle, Status: "True", Reason: "LowCPU", LastTransitionTime: ltt,
						Message: "cpu below 0.02 vCPU and no new tokens for 6m0s; idle 6m0s total, idle cost $0.0031"},
				},
			},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "coder-globex", Atespace: "default", CreationTimestamp: created(3 * time.Hour),
				Labels: map[string]string{"agent": "coder", "customer": "globex"}},
			Spec: &v1alpha1.TaskSpec{Budget: &v1alpha1.BudgetSpec{MaxCost: &v1alpha1.Money{Amount: 1}}},
			Status: &v1alpha1.TaskStatus{
				Phase: "Suspended", Actor: "coder-globex",
				Usage: &v1alpha1.UsageStatus{
					ActiveDuration: durationpb.New(2*time.Hour + 5*time.Minute),
					IdleDuration:   durationpb.New(10 * time.Minute),
					CpuSeconds:     5400, MemoryGibSeconds: 9000, WorkspaceBytes: 3 << 30,
					Tokens: []*v1alpha1.ModelTokenUsage{{Model: "gemini-2.5-pro", Input: 400000, Output: 60000, CacheRead: 100000}},
					Cost:   &v1alpha1.CostBreakdown{Currency: "USD", Compute: 0.21, Idle: 0.012, Tokens: 1.02, Storage: 0.0045, Total: 1.2345},
				},
				Conditions: []*v1alpha1.Condition{
					{Type: v1alpha1.ConditionBudgetWarning, Status: "False", Reason: "BudgetExceeded", LastTransitionTime: ltt,
						Message: "cost $1.23 of $1.00 (123%)"},
					{Type: v1alpha1.ConditionBudgetExceeded, Status: "True", Reason: "MaxCostExceeded", LastTransitionTime: ltt,
						Message: "cost $1.23 of $1.00 (123%)"},
				},
			},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "reviewer-acme", Atespace: "default", CreationTimestamp: created(90 * time.Second),
				Labels: map[string]string{"agent": "reviewer", "customer": "acme"}},
			Status: &v1alpha1.TaskStatus{
				Phase: "Running", Actor: "reviewer-acme", WorkerIp: "10.20.3.68",
				Usage: &v1alpha1.UsageStatus{
					ActiveDuration: durationpb.New(80 * time.Second),
					CpuSeconds:     12.5, MemoryGibSeconds: 20, WorkspaceBytes: 2048,
					Tokens: []*v1alpha1.ModelTokenUsage{{Model: "gemini-3-flash-preview", Input: 9000, Output: 400}},
					Cost:   &v1alpha1.CostBreakdown{Currency: "USD", Compute: 0.0003, Tokens: 0.0039, Total: 0.0042},
				},
			},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "unlabeled", Atespace: "default", CreationTimestamp: created(26 * time.Hour)},
			Spec:     &v1alpha1.TaskSpec{Image: "python:3.12"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "coder-team", Atespace: "team", CreationTimestamp: created(5 * time.Minute),
				Labels: map[string]string{"agent": "coder"}},
			Status: &v1alpha1.TaskStatus{
				Phase: "Running",
				Usage: &v1alpha1.UsageStatus{
					ActiveDuration: durationpb.New(5 * time.Minute),
					Tokens:         []*v1alpha1.ModelTokenUsage{{Model: "gemini-3-flash-preview", Input: 1000, Output: 200}},
					Cost:           &v1alpha1.CostBreakdown{Currency: "USD", Compute: 0.002, Tokens: 0.0008, Total: 0.0028},
				},
			},
		},
	}
}

// captureStdout runs fn with os.Stdout redirected and returns what it wrote.
// Callers must not run in parallel.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stdout := os.Stdout
	os.Stdout = f
	runErr := fn()
	os.Stdout = stdout
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return string(data), runErr
}

func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading golden file (run 'go test ./cmd/ax -run Golden -update' to create it): %v", err)
	}
	if got != string(want) {
		t.Errorf("output mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", path, got, want)
	}
}

func TestGolden(t *testing.T) {
	_, addr := startFakeTaskServer(t, finopsFixtureTasks())
	prevNow := nowFunc
	nowFunc = func() time.Time { return goldenNow }
	t.Cleanup(func() { nowFunc = prevNow })

	for _, tc := range []struct {
		golden   string
		atespace string
		run      func(addr, atespace string) error
	}{
		{"get_tasks", "default", func(a, s string) error { return runGet(a, s, []string{"tasks"}) }},
		{"get_tasks_show_labels", "default", func(a, s string) error { return runGet(a, s, []string{"tasks", "--show-labels"}) }},
		{"get_tasks_selector", "default", func(a, s string) error { return runGet(a, s, []string{"tasks", "-l", "customer=acme"}) }},
		{"get_task_yaml", "default", func(a, s string) error { return runGet(a, s, []string{"task", "coder-acme"}) }},
		{"get_task_json", "default", func(a, s string) error { return runGet(a, s, []string{"task", "coder-acme", "-o", "json"}) }},
		{"get_tasks_json", "default", func(a, s string) error {
			return runGet(a, s, []string{"tasks", "-l", "customer=globex", "--output=json"})
		}},
		{"describe_task", "default", func(a, s string) error { return runDescribe(a, s, []string{"task", "coder-acme"}) }},
		{"describe_task_exceeded", "default", func(a, s string) error { return runDescribe(a, s, []string{"task", "coder-globex"}) }},
		{"describe_task_no_usage", "default", func(a, s string) error { return runDescribe(a, s, []string{"task", "unlabeled"}) }},
		{"cost_summary", "default", func(a, s string) error { return runCost(a, s, []string{"summary"}, os.Stdout) }},
		{"cost_summary_by_customer", "default", func(a, s string) error {
			return runCost(a, s, []string{"summary", "--by", "customer"}, os.Stdout)
		}},
		{"cost_summary_selector", "default", func(a, s string) error {
			return runCost(a, s, []string{"summary", "--by=customer", "-l", "agent=coder"}, os.Stdout)
		}},
		{"cost_summary_atespace", "team", func(a, s string) error { return runCost(a, s, []string{"summary"}, os.Stdout) }},
		{"cost_summary_json", "default", func(a, s string) error {
			return runCost(a, s, []string{"summary", "-o", "json"}, os.Stdout)
		}},
	} {
		t.Run(tc.golden, func(t *testing.T) {
			got, err := captureStdout(t, func() error { return tc.run(addr, tc.atespace) })
			if err != nil {
				t.Fatalf("command failed: %v\noutput:\n%s", err, got)
			}
			checkGolden(t, tc.golden, got)
		})
	}
}

// T19: `ax get task -o json` prints protojson; -o yaml is the unchanged
// default YAML.
func TestGetTaskOutputFormats(t *testing.T) {
	fixtures := finopsFixtureTasks()
	_, addr := startFakeTaskServer(t, fixtures)
	get := func(args ...string) string {
		t.Helper()
		out, err := captureStdout(t, func() error { return runGet(addr, "default", args) })
		if err != nil {
			t.Fatalf("runGet(%q): %v\n%s", args, err, out)
		}
		return out
	}
	def := get("task", "coder-acme")
	for _, args := range [][]string{{"task", "coder-acme", "-o", "yaml"}, {"task", "coder-acme", "--output=yaml"}, {"-o=yaml", "task", "coder-acme"}} {
		if got := get(args...); got != def {
			t.Errorf("runGet(%q) differs from the default YAML:\n%s\n--- default ---\n%s", args, got, def)
		}
	}

	out := get("task", "coder-acme", "-o", "json")
	if !json.Valid([]byte(out)) {
		t.Fatalf("-o json output is not valid JSON:\n%s", out)
	}
	got := &v1alpha1.Task{}
	if err := protojson.Unmarshal([]byte(out), got); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	if !proto.Equal(got, fixtures[0]) {
		t.Errorf("JSON round trip = %v, want %v", got, fixtures[0])
	}
	// Same field names as the YAML.
	var asJSON map[string]any
	var asYAML map[string]any
	if err := json.Unmarshal([]byte(out), &asJSON); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal([]byte(def), &asYAML); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"metadata", "spec", "status"} {
		if _, ok := asJSON[k]; !ok {
			t.Errorf("JSON lacks %q", k)
		}
		jm, _ := asJSON[k].(map[string]any)
		ym, _ := asYAML[k].(map[string]any)
		if len(jm) != len(ym) {
			t.Errorf("%s: JSON keys %v, YAML keys %v", k, keys(jm), keys(ym))
		}
		for kk := range ym {
			if _, ok := jm[kk]; !ok {
				t.Errorf("%s.%s in YAML but not in JSON", k, kk)
			}
		}
	}

	// A list in JSON is the ListTasksResponse.
	want := 0
	for _, f := range fixtures {
		if f.GetMetadata().GetAtespace() == "default" {
			want++
		}
	}
	var list v1alpha1.ListTasksResponse
	if err := protojson.Unmarshal([]byte(get("tasks", "-o", "json")), &list); err != nil || len(list.Tasks) != want {
		t.Errorf("tasks -o json: %d tasks, want %d, err %v", len(list.Tasks), want, err)
	}

	for _, tc := range []struct {
		args    []string
		wantErr string
	}{
		{[]string{"task", "coder-acme", "-o", "xml"}, "unsupported output format"},
		{[]string{"task", "coder-acme", "-o", "table"}, "only supported when listing"},
		{[]string{"tasks", "--show-labels", "-o", "json"}, "--show-labels is only supported with table output"},
		{[]string{"task", "coder-acme", "-o"}, "requires a value"},
	} {
		if err := runGet(addr, "default", tc.args); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("runGet(%q) = %v, want error containing %q", tc.args, err, tc.wantErr)
		}
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestCostSummaryJSONMatchesTable(t *testing.T) {
	_, addr := startFakeTaskServer(t, finopsFixtureTasks())
	var buf bytes.Buffer
	if err := runCost(addr, "default", []string{"summary", "-o", "json"}, &buf); err != nil {
		t.Fatal(err)
	}
	var s costSummary
	if err := json.Unmarshal(buf.Bytes(), &s); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	var sum float64
	tasks := 0
	for _, g := range s.Groups {
		sum += g.Cost.Total
		tasks += g.Tasks
	}
	if fmt.Sprintf("%.6f", sum) != fmt.Sprintf("%.6f", s.Total.Cost.Total) || tasks != s.Total.Tasks || s.Total.Tasks != 4 {
		t.Errorf("groups do not add up to total: sum=%v tasks=%d total=%+v", sum, tasks, s.Total)
	}
	if s.By != "agent" || s.Atespace != "default" || s.Source != "tasks" || s.Currency != "USD" {
		t.Errorf("unexpected summary header: %+v", s)
	}
}

func TestCostSummaryPagesThroughAllTasks(t *testing.T) {
	var tasks []*v1alpha1.Task
	for i := range 1201 {
		tasks = append(tasks, &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: fmt.Sprintf("t%04d", i), Atespace: "default",
				Labels: map[string]string{"agent": []string{"a", "b", "c"}[i%3]}},
			Status: &v1alpha1.TaskStatus{Usage: &v1alpha1.UsageStatus{Cost: &v1alpha1.CostBreakdown{Total: 0.01}}},
		})
	}
	f, addr := startFakeTaskServer(t, tasks)
	var buf bytes.Buffer
	if err := runCost(addr, "default", []string{"summary", "-o", "json", "-l", "agent!=x"}, &buf); err == nil {
		t.Fatal("expected invalid selector error")
	}
	if err := runCost(addr, "default", []string{"summary", "-o", "json"}, &buf); err != nil {
		t.Fatal(err)
	}
	var s costSummary
	if err := json.Unmarshal(buf.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	if s.Total.Tasks != 1201 || len(s.Groups) != 3 {
		t.Errorf("got %d tasks in %d groups, want 1201 in 3", s.Total.Tasks, len(s.Groups))
	}
	calls := f.calls()
	if len(calls) != 3 {
		t.Errorf("got %d ListTasks calls, want 3 pages", len(calls))
	}
	for i, c := range calls {
		if c.GetLimit() != taskPageSize || c.GetOffset() != int64(i*taskPageSize) || c.GetAtespace() != "default" {
			t.Errorf("call %d = %v", i, c)
		}
	}
}

func TestSummarizeCountsTasksOncePerGroup(t *testing.T) {
	// A ListUsage source (T16) yields one row per task and day.
	day := func(task, agent string, total float64) usageRow {
		return usageRow{Atespace: "default", Task: task, Labels: map[string]string{"agent": agent},
			ActiveSeconds: 60, Tokens: 10, Cost: &v1alpha1.CostBreakdown{Total: total, Compute: total}}
	}
	s := summarize([]usageRow{day("t1", "a", 1), day("t1", "a", 2), day("t2", "a", 0.5), day("t3", "b", 5), day("t3", "", 0)}, "agent")
	if len(s.Groups) != 3 {
		t.Fatalf("groups = %d", len(s.Groups))
	}
	want := []struct {
		group string
		tasks int
		total float64
	}{{"b", 1, 5}, {"a", 2, 3.5}, {noLabelGroup, 1, 0}}
	for i, w := range want {
		g := s.Groups[i]
		if g.Group != w.group || g.Tasks != w.tasks || g.Cost.Total != w.total {
			t.Errorf("group %d = %s/%d/%v, want %s/%d/%v", i, g.Group, g.Tasks, g.Cost.Total, w.group, w.tasks, w.total)
		}
	}
	if s.Total.Tasks != 3 || s.Total.Cost.Total != 8.5 || s.Total.ActiveSeconds != 300 || s.Total.Tokens != 50 {
		t.Errorf("total = %+v", s.Total)
	}
}

func TestParseCostSummaryArgs(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		want    costSummaryOptions
		wantErr string
	}{
		{args: nil, want: costSummaryOptions{by: "agent", output: "table"}},
		{args: []string{"--by", "customer", "-l", "agent=coder", "-o", "json"}, want: costSummaryOptions{by: "customer", selector: "agent=coder", output: "json"}},
		{args: []string{"--by=team", "--selector=agent=coder", "--output=json"}, want: costSummaryOptions{by: "team", selector: "agent=coder", output: "json"}},
		{args: []string{"-l=agent=coder"}, want: costSummaryOptions{by: "agent", selector: "agent=coder", output: "table"}},
		{args: []string{"--by"}, wantErr: "requires a value"},
		{args: []string{"--by", "bad key"}, wantErr: "invalid --by label"},
		{args: []string{"-l", "agent"}, wantErr: "invalid label selector"},
		{args: []string{"-o", "yaml"}, wantErr: "unsupported output format"},
		{args: []string{"extra"}, wantErr: "unexpected argument"},
	} {
		got, err := parseCostSummaryArgs(tc.args)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("parseCostSummaryArgs(%q) error = %v, want %q", tc.args, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseCostSummaryArgs(%q) = %+v, %v; want %+v", tc.args, got, err, tc.want)
		}
	}

	// Usage errors from the command itself.
	for _, args := range [][]string{nil, {"bogus"}} {
		var buf bytes.Buffer
		if err := runCost("127.0.0.1:1", "default", args, &buf); err == nil {
			t.Errorf("runCost(%q) succeeded, want error", args)
		}
	}
	var buf bytes.Buffer
	if err := runCost("127.0.0.1:1", "default", []string{"summary", "--help"}, &buf); err != nil || !strings.Contains(buf.String(), "ax cost summary") {
		t.Errorf("runCost --help = %v, %q", err, buf.String())
	}
}

func TestRunGetShowLabelsOnlyForTaskList(t *testing.T) {
	for _, args := range [][]string{{"task", "x", "--show-labels"}, {"workspaces", "--show-labels"}} {
		if err := runGet("127.0.0.1:1", "default", args); err == nil || !strings.Contains(err.Error(), "--show-labels") {
			t.Errorf("runGet(%q) = %v, want --show-labels error", args, err)
		}
	}
}

func TestFormatMoney(t *testing.T) {
	for _, tc := range []struct {
		amount   float64
		currency string
		want     string
	}{
		{0, "USD", "$0.0000"},
		{0, "", "$0.0000"},
		{0.0123, "USD", "$0.0123"},
		{0.00004, "USD", "$0.0000"},
		{0.12345, "USD", "$0.1235"},
		{0.9999, "USD", "$0.9999"},
		{0.99996, "USD", "$1.00"},
		{1, "USD", "$1.00"},
		{1.2345, "USD", "$1.23"},
		{1234.5, "USD", "$1234.50"},
		{-0.5, "USD", "-$0.5000"},
		{-0.00001, "USD", "$0.0000"},
		{2.5, "EUR", "2.50 EUR"},
	} {
		if got := formatMoney(tc.amount, tc.currency); got != tc.want {
			t.Errorf("formatMoney(%v, %q) = %q, want %q", tc.amount, tc.currency, got, tc.want)
		}
	}
}

func TestFormatBytesAndCount(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 12_934_144: "12.3 MiB", 3 << 30: "3.0 GiB", 5 << 40: "5.0 TiB"} {
		if got := formatBytes(n); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", n, got, want)
		}
	}
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 88600: "88,600", 1234567: "1,234,567", -1234: "-1,234"} {
		if got := formatCount(n); got != want {
			t.Errorf("formatCount(%d) = %q, want %q", n, got, want)
		}
	}
}
