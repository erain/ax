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
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

func TestRunApplyEmptyDocuments(t *testing.T) {
	const first = "apiVersion: ax.io/v1alpha1\nkind: Workspace\nmetadata:\n  name: first\nspec: {}\n"
	second := strings.Replace(first, "name: first", "name: second", 1)
	for _, tc := range []struct {
		name      string
		manifest  string
		wantNames []string
		wantErr   string
	}{
		{name: "empty file"},
		{name: "empty documents only", manifest: "---\n# empty\n---\n"},
		{name: "leading empty document", manifest: "---\n---\n" + first, wantNames: []string{"first"}},
		{name: "trailing separator", manifest: first + "---\n", wantNames: []string{"first"}},
		{name: "middle empty documents", manifest: first + "---\n# empty\n---\n---\n" + second, wantNames: []string{"first", "second"}},
		{name: "missing kind", manifest: first + "---\n---\n{}\n---\n" + second, wantNames: []string{"first"}, wantErr: "applying document 3: missing kind"},
		{name: "explicit null", manifest: "null\n", wantErr: "applying document 1: missing kind"},
		{name: "tagged null", manifest: "!!null\n", wantErr: "applying document 1: missing kind"},
		{name: "tagged quoted null", manifest: "!!null \"\"\n", wantErr: "applying document 1: missing kind"},
		{name: "empty string", manifest: "\"\"\n", wantErr: "applying document 1: reading kind"},
		{name: "invalid YAML", manifest: first + "---\n[\n", wantNames: []string{"first"}, wantErr: "decoding document 2:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := memory.NewStore()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			srv := server.NewServer(s).GRPCServer()
			t.Cleanup(srv.Stop)
			go func() { _ = srv.Serve(listener) }()

			path := filepath.Join(t.TempDir(), "manifest.yaml")
			if err := os.WriteFile(path, []byte(tc.manifest), 0600); err != nil {
				t.Fatal(err)
			}
			// runApply writes to os.Stdout, so these subtests must remain sequential.
			output, err := os.CreateTemp(t.TempDir(), "stdout")
			if err != nil {
				t.Fatal(err)
			}
			stdout := os.Stdout
			os.Stdout = output
			t.Cleanup(func() {
				os.Stdout = stdout
				_ = output.Close()
			})

			runErr := runApply(listener.Addr().String(), []string{"-f", path})
			if tc.wantErr == "" {
				if runErr != nil {
					t.Fatal(runErr)
				}
			} else if runErr == nil || !strings.Contains(runErr.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, runErr)
			}
			var wantOutput strings.Builder
			for _, name := range tc.wantNames {
				if _, err := s.GetWorkspace(context.Background(), "default", name); err != nil {
					t.Fatalf("expected workspace %q to be created: %v", name, err)
				}
				wantOutput.WriteString("workspace.ax.io/" + name + " created\n")
			}
			data, err := os.ReadFile(output.Name())
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != wantOutput.String() {
				t.Fatalf("expected output %q, got %q", wantOutput.String(), data)
			}
		})
	}
}

func TestRunGetResourceAliases(t *testing.T) {
	const atespace = "test-space"
	s := memory.NewStore()
	for _, name := range []string{"target", "other"} {
		meta := &v1alpha1.ObjectMeta{Name: name, Atespace: atespace}
		if err := s.SaveTask(context.Background(), &v1alpha1.Task{Metadata: meta}); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveWorkspace(context.Background(), &v1alpha1.Workspace{Metadata: meta}); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveModel(context.Background(), &v1alpha1.Model{Metadata: meta}); err != nil {
			t.Fatal(err)
		}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewServer(s).GRPCServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(listener) }()

	for _, resource := range []string{"task", "workspace", "model"} {
		for _, alias := range []string{resource, resource + "s"} {
			for _, tc := range []struct {
				name         string
				resourceName string
			}{
				{name: "list"},
				{name: "get", resourceName: "target"},
				{name: "missing", resourceName: "missing"},
			} {
				t.Run(alias+"/"+tc.name, func(t *testing.T) {
					// runGet writes to os.Stdout, so these subtests must remain sequential.
					output, err := os.CreateTemp(t.TempDir(), "stdout")
					if err != nil {
						t.Fatal(err)
					}
					stdout := os.Stdout
					os.Stdout = output
					t.Cleanup(func() {
						os.Stdout = stdout
						_ = output.Close()
					})

					args := []string{alias}
					if tc.resourceName != "" {
						args = append(args, tc.resourceName)
					}
					runErr := runGet(listener.Addr().String(), atespace, args)
					data, err := os.ReadFile(output.Name())
					if err != nil {
						t.Fatal(err)
					}
					if tc.name == "missing" {
						if status.Code(runErr) != codes.NotFound {
							t.Fatalf("expected NotFound, got %v; output: %s", runErr, data)
						}
						if len(data) != 0 {
							t.Fatalf("unexpected output for missing resource: %s", data)
						}
						return
					}
					if runErr != nil {
						t.Fatal(runErr)
					}
					if tc.name == "get" {
						var got struct {
							Metadata struct {
								Name     string `yaml:"name"`
								Atespace string `yaml:"atespace"`
							} `yaml:"metadata"`
						}
						if err := yaml.Unmarshal(data, &got); err != nil {
							t.Fatalf("expected resource YAML, got %q: %v", data, err)
						}
						if got.Metadata.Name != tc.resourceName || got.Metadata.Atespace != atespace {
							t.Fatalf("unexpected resource metadata: %+v", got.Metadata)
						}
						return
					}
					rows := strings.Split(strings.TrimSpace(string(data)), "\n")
					if len(rows) != 3 {
						t.Fatalf("expected header and two resources, got %q", data)
					}
					// Task tables have an AGENT column between NAME and ATESPACE.
					atespaceCol := 1
					if resource == "task" {
						atespaceCol = 2
					}
					seen := make(map[string]bool)
					for _, row := range rows[1:] {
						fields := strings.Fields(row)
						if len(fields) <= atespaceCol || fields[atespaceCol] != atespace {
							t.Fatalf("unexpected resource row: %q", row)
						}
						seen[fields[0]] = true
					}
					if !seen["target"] || !seen["other"] {
						t.Fatalf("expected both resources, got %q", data)
					}
				})
			}
		}
	}
}

type fakeMainReconciler struct {
	deleted chan struct{}
}

func (f *fakeMainReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	return task, nil
}

func (f *fakeMainReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string) error {
	time.Sleep(100 * time.Millisecond)
	close(f.deleted)
	return nil
}

func TestRunDeleteTask(t *testing.T) {
	s := memory.NewStore()
	meta := &v1alpha1.ObjectMeta{Name: "task-to-delete", Atespace: "default"}
	if err := s.SaveTask(context.Background(), &v1alpha1.Task{Metadata: meta}); err != nil {
		t.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &fakeMainReconciler{deleted: make(chan struct{})}
	srv := server.NewServer(s, server.Options{Reconciler: rec}).GRPCServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(listener) }()

	output, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = output
	t.Cleanup(func() {
		os.Stdout = stdout
		_ = output.Close()
	})

	runErr := runDelete(listener.Addr().String(), "default", []string{"task", "task-to-delete"})
	if runErr != nil {
		t.Fatalf("runDelete failed: %v", runErr)
	}

	// Verify actor deletion finished before runDelete returned
	select {
	case <-rec.deleted:
	default:
		t.Fatal("expected actor deletion to complete before runDelete returned")
	}

	data, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	wantOutput := "task.ax.io/task-to-delete deleted\n"
	if string(data) != wantOutput {
		t.Fatalf("expected output %q, got %q", wantOutput, string(data))
	}

	// Verify task is deleted from store
	_, err = s.GetTask(context.Background(), "default", "task-to-delete")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected task to be NotFound after deletion, got %v", err)
	}
}

func TestParseGlobalArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want globalArgs
	}{
		{
			name: "flags before and after the command",
			args: []string{"-a", "team", "get", "tasks", "--namespace=ax"},
			want: globalArgs{cmd: "get", args: []string{"tasks"}, atespace: "team", namespace: "ax"},
		},
		{
			name: "flags after -- belong to the remote command",
			args: []string{"ssh", "task123", "--", "grep", "-n", "foo", "-a", "file"},
			want: globalArgs{
				cmd:       "ssh",
				args:      []string{"task123", "--", "grep", "-n", "foo", "-a", "file"},
				atespace:  "default",
				namespace: "ax-system",
			},
		},
		{
			name: "global flags before -- still apply",
			args: []string{"ssh", "-a", "team", "task123", "--", "ls", "--context=x"},
			want: globalArgs{
				cmd:       "ssh",
				args:      []string{"task123", "--", "ls", "--context=x"},
				atespace:  "team",
				namespace: "ax-system",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseGlobalArgs(tt.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseGlobalArgs(%q) = %+v, want %+v", tt.args, got, tt.want)
			}
		})
	}
}

func TestParseSSHArgs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantTask    string
		wantCommand []string
		wantHelp    bool
		wantErr     bool
	}{
		{name: "long help", args: []string{"--help"}, wantHelp: true},
		{name: "short help", args: []string{"-h"}, wantHelp: true},
		{name: "no args", args: nil, wantErr: true},
		{name: "no task before --", args: []string{"--", "ls"}, wantErr: true},
		{name: "unknown flag", args: []string{"--verbose", "task123"}, wantErr: true},
		{name: "default shell", args: []string{"task123"}, wantTask: "task123", wantCommand: []string{"/bin/sh"}},
		{
			name:        "command after --",
			args:        []string{"task123", "--", "ls", "-la", "/workspace"},
			wantTask:    "task123",
			wantCommand: []string{"ls", "-la", "/workspace"},
		},
		{
			name:        "command without --",
			args:        []string{"task123", "python3", "main.py"},
			wantTask:    "task123",
			wantCommand: []string{"python3", "main.py"},
		},
		{
			name:        "later -- is kept with leading --",
			args:        []string{"task123", "--", "git", "log", "--", "file"},
			wantTask:    "task123",
			wantCommand: []string{"git", "log", "--", "file"},
		},
		{
			name:        "later -- is kept without leading --",
			args:        []string{"task123", "git", "log", "--", "file"},
			wantTask:    "task123",
			wantCommand: []string{"git", "log", "--", "file"},
		},
		{
			name:        "help after the task name goes to the remote command",
			args:        []string{"task123", "--", "git", "--help"},
			wantTask:    "task123",
			wantCommand: []string{"git", "--help"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, command, help, err := parseSSHArgs(tt.args)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSSHArgs(%q) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			}
			if task != tt.wantTask || help != tt.wantHelp || !reflect.DeepEqual(command, tt.wantCommand) {
				t.Errorf("parseSSHArgs(%q) = (%q, %q, %v), want (%q, %q, %v)",
					tt.args, task, command, help, tt.wantTask, tt.wantCommand, tt.wantHelp)
			}
		})
	}
}

func TestParseGetArgs(t *testing.T) {
	tests := []struct {
		args         []string
		wantPos      []string
		wantSelector string
		wantErr      bool
	}{
		{args: []string{"tasks"}, wantPos: []string{"tasks"}},
		{args: []string{"tasks", "-l", "agent=foo,customer=bar"}, wantPos: []string{"tasks"}, wantSelector: "agent=foo,customer=bar"},
		{args: []string{"-l", "agent=foo", "tasks"}, wantPos: []string{"tasks"}, wantSelector: "agent=foo"},
		{args: []string{"tasks", "--selector", "agent=foo"}, wantPos: []string{"tasks"}, wantSelector: "agent=foo"},
		{args: []string{"tasks", "--selector=agent=foo"}, wantPos: []string{"tasks"}, wantSelector: "agent=foo"},
		{args: []string{"tasks", "-l=agent=foo"}, wantPos: []string{"tasks"}, wantSelector: "agent=foo"},
		{args: []string{"task", "name"}, wantPos: []string{"task", "name"}},
		{args: []string{"tasks", "-l"}, wantErr: true},
	}
	for _, tt := range tests {
		pos, sel, err := parseGetArgs(tt.args)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseGetArgs(%q) error = %v, wantErr %v", tt.args, err, tt.wantErr)
			continue
		}
		if tt.wantErr {
			continue
		}
		if !reflect.DeepEqual(pos, tt.wantPos) || sel != tt.wantSelector {
			t.Errorf("parseGetArgs(%q) = %q, %q; want %q, %q", tt.args, pos, sel, tt.wantPos, tt.wantSelector)
		}
	}

	// The global parser leaves -l and its value for the command.
	g := parseGlobalArgs([]string{"get", "tasks", "-a", "team", "-l", "agent=foo"})
	if g.cmd != "get" || g.atespace != "team" || !reflect.DeepEqual(g.args, []string{"tasks", "-l", "agent=foo"}) {
		t.Errorf("parseGlobalArgs = %+v", g)
	}
}

func TestRunGetTasksLabelSelector(t *testing.T) {
	s := memory.NewStore()
	for name, labels := range map[string]map[string]string{
		"coder-acme":    {"agent": "coder", "customer": "acme"},
		"coder-globex":  {"agent": "coder", "customer": "globex"},
		"reviewer-acme": {"agent": "reviewer", "customer": "acme"},
		"unlabeled":     nil,
	} {
		if err := s.SaveTask(context.Background(), &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: name, Labels: labels}}); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewServer(s).GRPCServer()
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(listener) }()

	for _, tc := range []struct {
		args []string
		want map[string]string // task name -> AGENT column
	}{
		{args: []string{"tasks"}, want: map[string]string{"coder-acme": "coder", "coder-globex": "coder", "reviewer-acme": "reviewer", "unlabeled": "-"}},
		{args: []string{"tasks", "-l", "agent=coder"}, want: map[string]string{"coder-acme": "coder", "coder-globex": "coder"}},
		{args: []string{"tasks", "-l", "agent=coder,customer=acme"}, want: map[string]string{"coder-acme": "coder"}},
		{args: []string{"tasks", "--selector=customer=acme"}, want: map[string]string{"coder-acme": "coder", "reviewer-acme": "reviewer"}},
		{args: []string{"tasks", "-l", "agent=nobody"}, want: map[string]string{}},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			// runGet writes to os.Stdout, so these subtests must remain sequential.
			output, err := os.CreateTemp(t.TempDir(), "stdout")
			if err != nil {
				t.Fatal(err)
			}
			stdout := os.Stdout
			os.Stdout = output
			t.Cleanup(func() {
				os.Stdout = stdout
				_ = output.Close()
			})
			if err := runGet(listener.Addr().String(), "default", tc.args); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(output.Name())
			if err != nil {
				t.Fatal(err)
			}
			rows := strings.Split(strings.TrimSpace(string(data)), "\n")
			if header := strings.Fields(rows[0]); len(header) < 3 || header[0] != "NAME" || header[1] != "AGENT" || header[2] != "ATESPACE" {
				t.Fatalf("unexpected header %q", rows[0])
			}
			got := make(map[string]string)
			for _, row := range rows[1:] {
				fields := strings.Fields(row)
				got[fields[0]] = fields[1]
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("rows = %v, want %v; output:\n%s", got, tc.want, data)
			}
		})
	}

	// Invalid selectors and selectors on other resources fail before any output.
	for _, args := range [][]string{
		{"tasks", "-l", "agent"},
		{"tasks", "-l", "agent!=coder"},
		{"task", "coder-acme", "-l", "agent=coder"},
		{"workspaces", "-l", "agent=coder"},
	} {
		if err := runGet(listener.Addr().String(), "default", args); err == nil {
			t.Errorf("runGet(%q) succeeded, want error", args)
		}
	}
}
