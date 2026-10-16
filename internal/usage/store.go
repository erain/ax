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
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrNotFound is returned by Store.Get for a key without usage.
	ErrNotFound = errors.New("usage: not found")
	// ErrNoChange may be returned by an Update callback to skip the write;
	// Update then returns nil.
	ErrNoChange = errors.New("usage: no change")
)

// Store persists TaskUsage records.
type Store interface {
	// Get returns the usage for key, or ErrNotFound.
	Get(ctx context.Context, key Key) (*TaskUsage, error)
	// Update atomically reads the usage for key (an empty record from
	// NewTaskUsage if absent), calls fn, and writes the result. fn may be
	// called more than once (optimistic retries), so it must only mutate the
	// record it is given (capture results in variables it overwrites). If fn
	// returns an error nothing is written; ErrNoChange yields a nil result.
	Update(ctx context.Context, key Key, fn func(*TaskUsage) error) error
	// Delete removes the usage for key; deleting a missing key is not an
	// error.
	Delete(ctx context.Context, key Key) error
	// List returns all usage records in atespace ("" = all atespaces).
	List(ctx context.Context, atespace string) (map[Key]*TaskUsage, error)
}

func marshalUsage(u *TaskUsage) ([]byte, error) {
	b, err := json.Marshal(u)
	if err != nil {
		return nil, fmt.Errorf("marshaling usage: %w", err)
	}
	return b, nil
}

func unmarshalUsage(b []byte) (*TaskUsage, error) {
	u := NewTaskUsage()
	if err := json.Unmarshal(b, u); err != nil {
		return nil, fmt.Errorf("unmarshaling usage: %w", err)
	}
	if u.Days == nil {
		u.Days = map[string]*DayUsage{}
	}
	return u, nil
}
