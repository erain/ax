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
)

// MemoryStore is an in-process Store. Records are kept serialized so callers
// never share memory with the store.
type MemoryStore struct {
	mu   sync.Mutex
	data map[Key][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: map[Key][]byte{}}
}

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, key Key) (*TaskUsage, error) {
	key = key.normalize()
	s.mu.Lock()
	b, ok := s.data[key]
	s.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return unmarshalUsage(b)
}

// Update implements Store.
func (s *MemoryStore) Update(_ context.Context, key Key, fn func(*TaskUsage) error) error {
	key = key.normalize()
	s.mu.Lock()
	defer s.mu.Unlock()
	u := NewTaskUsage()
	if b, ok := s.data[key]; ok {
		var err error
		if u, err = unmarshalUsage(b); err != nil {
			return err
		}
	}
	if err := fn(u); err != nil {
		if errors.Is(err, ErrNoChange) {
			return nil
		}
		return err
	}
	b, err := marshalUsage(u)
	if err != nil {
		return err
	}
	s.data[key] = b
	return nil
}

// Delete implements Store.
func (s *MemoryStore) Delete(_ context.Context, key Key) error {
	key = key.normalize()
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
	return nil
}

// List implements Store.
func (s *MemoryStore) List(_ context.Context, atespace string) (map[Key]*TaskUsage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[Key]*TaskUsage{}
	for k, b := range s.data {
		if atespace != "" && k.Atespace != atespace {
			continue
		}
		u, err := unmarshalUsage(b)
		if err != nil {
			return nil, err
		}
		out[k] = u
	}
	return out, nil
}
