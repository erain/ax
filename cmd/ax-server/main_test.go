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
	"errors"
	"flag"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestRun_StartsWithOTLPListener starts ax-server in-process (in-memory
// stores, no Substrate reachable) with --otlp-addr=127.0.0.1:0, posts the T04
// golden fixture to /v1/logs and shuts it down gracefully.
func TestRun_StartsWithOTLPListener(t *testing.T) {
	t.Setenv("ADDR", "")
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	type addrs struct{ api, otlp net.Addr }
	ready := make(chan addrs, 1)
	onListening = func(api, otlp net.Addr) { ready <- addrs{api, otlp} }
	t.Cleanup(func() { onListening = nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"--addr=127.0.0.1:0",
			"--otlp-addr=127.0.0.1:0",
			"--redis-addr=",
			"--substrate-endpoint=127.0.0.1:1",
			"--substrate-plaintext",
			"--rate-card=../../deploy/ratecard.yaml",
			"--usage-pull-interval=1h",
			"--runner-port=8080",
			"--summary-interval=1h",
		})
	}()

	var a addrs
	select {
	case a = <-ready:
	case err := <-done:
		t.Fatalf("run exited early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("ax-server did not start")
	}
	if a.otlp == nil {
		t.Fatal("OTLP listener not started")
	}

	body, err := os.ReadFile("../../internal/usage/ingest/testdata/substrate_events.json")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+a.otlp.String()+"/v1/logs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /v1/logs = %d, want 200", resp.StatusCode)
	}
	for _, u := range []string{"http://" + a.api.String() + "/healthz", "http://" + a.otlp.String() + "/healthz"} {
		resp, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s = %d", u, resp.StatusCode)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run returned %v after shutdown, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ax-server did not shut down")
	}
}

func TestRun_FlagErrors(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	if err := run(context.Background(), []string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("--help: err = %v, want flag.ErrHelp", err)
	}
	if err := run(context.Background(), []string{"--no-such-flag"}); err == nil {
		t.Error("unknown flag accepted")
	}
	err := run(context.Background(), []string{"--redis-addr=", "--rate-card=/does/not/exist.yaml", "--addr=127.0.0.1:0", "--otlp-addr="})
	if err == nil || !strings.Contains(err.Error(), "rate card") {
		t.Errorf("missing rate card: err = %v, want a rate card error", err)
	}
}

// T12c N1: --usage-router-addr is validated at startup.
func TestRun_RejectsBadRouterAddr(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("ATENET_ROUTER_ADDR", "")
	for _, addr := range []string{
		"http://atenet-router:80", "atenet-router", "atenet-router:80/usage", ":80",
		"atenet-router:http", "atenet-router:0", "user@atenet-router:80",
	} {
		err := run(context.Background(), []string{"--redis-addr=", "--addr=127.0.0.1:0", "--otlp-addr=", "--usage-router-addr=" + addr})
		if err == nil || !strings.Contains(err.Error(), "--usage-router-addr") {
			t.Errorf("--usage-router-addr=%s: err = %v, want a validation error", addr, err)
		}
	}
	for _, addr := range []string{"", "atenet-router.ate-system.svc.cluster.local:80", "10.0.0.1:8080", "[::1]:80"} {
		if err := checkRouterAddr(addr); err != nil {
			t.Errorf("checkRouterAddr(%q) = %v, want nil", addr, err)
		}
	}
}
