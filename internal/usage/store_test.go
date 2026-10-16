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

package usage

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// Acceptance 8: memory and Redis stores pass the same conformance test.
func TestStoreConformance(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		testStoreConformance(t, NewMemoryStore())
	})
	t.Run("redis", func(t *testing.T) {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { client.Close() })
		s := NewRedisStore(client, "")
		testStoreConformance(t, s)
		if !mr.Exists("ax:usage:default:t2") {
			t.Errorf("missing key ax:usage:default:t2; keys = %v", mr.Keys())
		}
	})
}

func testStoreConformance(t *testing.T, s Store) {
	ctx := context.Background()
	k1 := Key{Atespace: "default", Name: "t1"}
	k2 := Key{Atespace: "default", Name: "t2"}
	k3 := Key{Atespace: "other", Name: "t1"}

	if _, err := s.Get(ctx, k1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}

	// Update creates a record from an empty one.
	err := s.Update(ctx, k1, func(u *TaskUsage) error {
		if u == nil || u.Days == nil || u.CPUSeconds != 0 {
			t.Errorf("Update got non-empty record %+v", u)
		}
		u.ApplyStateChange(StateChange{Time: at(0), ActorUID: "uid", State: "running"})
		u.ApplyStateChange(StateChange{Time: at(60), ActorUID: "uid", State: "suspended"})
		u.AddCost("2026-10-16", Cost{Currency: "USD", Total: 1, Compute: 1})
		return nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	// Empty atespace means "default".
	u, err := s.Get(ctx, Key{Name: "t1"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	approx(t, "ActiveSeconds", u.ActiveSeconds, 60)
	approx(t, "day cost", u.Day("2026-10-16").Cost.Total, 1)
	if u.State.ActorUID != "uid" || !u.UpdateTime.Equal(at(60)) {
		t.Errorf("state not persisted: %+v %v", u.State, u.UpdateTime)
	}

	// Returned records are copies.
	u.CPUSeconds = 999
	if u2, _ := s.Get(ctx, k1); u2.CPUSeconds != 0 {
		t.Errorf("Get returned shared record")
	}

	// fn errors abort the write; ErrNoChange skips it without error.
	boom := errors.New("boom")
	if err := s.Update(ctx, k1, func(u *TaskUsage) error { u.CPUSeconds = 5; return boom }); !errors.Is(err, boom) {
		t.Errorf("Update error = %v, want boom", err)
	}
	if err := s.Update(ctx, k1, func(u *TaskUsage) error { u.CPUSeconds = 5; return ErrNoChange }); err != nil {
		t.Errorf("Update ErrNoChange = %v, want nil", err)
	}
	if err := s.Update(ctx, k2, func(u *TaskUsage) error { return ErrNoChange }); err != nil {
		t.Errorf("Update ErrNoChange (new key) = %v", err)
	}
	if _, err := s.Get(ctx, k2); !errors.Is(err, ErrNotFound) {
		t.Errorf("ErrNoChange created a record: err = %v", err)
	}
	if u2, _ := s.Get(ctx, k1); u2.CPUSeconds != 0 {
		t.Errorf("aborted Update was written: CPUSeconds = %v", u2.CPUSeconds)
	}

	// Concurrent updates are atomic.
	const workers, perWorker = 20, 5
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				if err := s.Update(ctx, k2, func(u *TaskUsage) error {
					u.CPUSeconds++
					u.AddCost("2026-10-16", Cost{Total: 1})
					return nil
				}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Update: %v", err)
	}
	u2, err := s.Get(ctx, k2)
	if err != nil {
		t.Fatal(err)
	}
	if u2.CPUSeconds != workers*perWorker || u2.Day("2026-10-16").Cost.Total != workers*perWorker {
		t.Errorf("concurrent total = %v / %v, want %d", u2.CPUSeconds, u2.Day("2026-10-16").Cost.Total, workers*perWorker)
	}

	if err := s.Update(ctx, k3, func(u *TaskUsage) error { u.CPUSeconds = 1; return nil }); err != nil {
		t.Fatal(err)
	}

	// List.
	got, err := s.List(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[k1] == nil || got[k2] == nil {
		t.Errorf("List(default) = %v", keysOf(got))
	}
	got, err = s.List(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[k3] == nil || got[k3].CPUSeconds != 1 {
		t.Errorf("List(all) = %v", keysOf(got))
	}

	// Delete.
	if err := s.Delete(ctx, k1); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, k1); err != nil {
		t.Errorf("Delete missing: %v", err)
	}
	if _, err := s.Get(ctx, k1); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: err = %v", err)
	}
	got, err = s.List(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[k2] == nil {
		t.Errorf("List after Delete = %v", keysOf(got))
	}
}

func keysOf(m map[Key]*TaskUsage) []Key {
	var ks []Key
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}
