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

package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/usage"
	"github.com/google/ax/internal/usage/puller"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

type snapshotLister []*v1alpha1.Task

func (l snapshotLister) ListRunningTasks(context.Context) ([]*v1alpha1.Task, error) { return l, nil }

// T12c R1: router-mode pulls re-read the task through the server
// (LookupTask) right before the request. SuspendTask's Flush (task still
// Running) goes through; a sweep or Flush holding a Running snapshot taken
// before the suspend sends no router request (which would resume the actor).
func TestFinOps_RouterPullRechecksTask(t *testing.T) {
	var hits atomic.Int64
	router := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(puller.TargetActorHeader) != "default/t1" {
			http.NotFound(w, r)
			return
		}
		hits.Add(1)
		fmt.Fprintf(w, `{"session":"s1","reportTime":%q,"tokens":{"gemini-2.5-flash":{"input":1000,"output":500}}}`,
			time.Now().UTC().Format(time.RFC3339Nano))
	}))
	defer router.Close()
	routerAddr := router.Listener.Addr().String()

	e := newFinopsEnv(t, envOptions{reconciler: &statusReconciler{}})
	wired := e.srv.NewUsagePuller(puller.Options{RouterAddr: routerAddr, Timeout: 2 * time.Second})
	e.saveTask(t, "default", "t1", "Running", withWorker("10.0.0.9"))
	snap := e.stored(t, "default", "t1") // a sweep's Running snapshot

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Name: "t1"}); err != nil {
		t.Fatal(err)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("router requests after SuspendTask = %d, want 1 (flush before suspend)", n)
	}
	u, err := e.srv.UsageStore().Get(ctx, usage.Key{Atespace: "default", Name: "t1"})
	if err != nil || u.Tokens["gemini-2.5-flash"].Input != 1000 {
		t.Fatalf("flushed usage = %+v, %v", u, err)
	}

	// The sweep listed t1 before the suspend and pulls it afterwards.
	stale := puller.New(puller.Options{
		Tasks: snapshotLister{snap}, Getter: e.srv, Store: e.srv.UsageStore(),
		RouterAddr: routerAddr, Timeout: 2 * time.Second,
	})
	stale.Sweep(ctx)
	if n := hits.Load(); n != 1 {
		t.Errorf("router requests after stale sweep = %d, want 1", n)
	}
	if st := stale.Stats(); st.Skipped != 1 || st.Pulls != 0 {
		t.Errorf("stale sweep stats = %+v, want 1 skipped, 0 pulls", st)
	}

	// The server-wired puller picks up the server as its TaskGetter.
	if err := wired.Flush(ctx, snap); err != nil {
		t.Errorf("Flush(stale snapshot) = %v, want nil", err)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("router requests after stale Flush = %d, want 1", n)
	}
	if st := wired.Stats(); st.Skipped != 1 || st.Pulls != 1 {
		t.Errorf("wired puller stats = %+v, want 1 pull (suspend flush) and 1 skipped", st)
	}

	// A deleted task is skipped too.
	if got, err := e.srv.LookupTask(ctx, "default", "missing"); got != nil || err != nil {
		t.Errorf("LookupTask(missing) = %v, %v, want nil, nil", got, err)
	}
}
