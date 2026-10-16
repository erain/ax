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
	"fmt"

	"github.com/redis/go-redis/v9"
)

// redisMaxRetries bounds optimistic-transaction retries in Update.
const redisMaxRetries = 200

// ErrConflict is returned by RedisStore.Update when the optimistic
// transaction kept conflicting.
var ErrConflict = errors.New("usage: too many concurrent update conflicts")

// RedisStore is a Redis-backed Store. Each record is a JSON value at
// "{prefix}:usage:{atespace}:{name}"; Update uses WATCH/MULTI with retry.
// Sets "{prefix}:usages:atespaces" and "{prefix}:usages:atespace:{atespace}"
// index the records for List.
type RedisStore struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisStore returns a RedisStore. An empty prefix means "ax" (the task
// store's default).
func NewRedisStore(client redis.UniversalClient, prefix string) *RedisStore {
	if prefix == "" {
		prefix = "ax"
	}
	return &RedisStore{client: client, prefix: prefix}
}

func (s *RedisStore) key(k Key) string {
	return fmt.Sprintf("%s:usage:%s:%s", s.prefix, k.Atespace, k.Name)
}

func (s *RedisStore) atespacesKey() string {
	return fmt.Sprintf("%s:usages:atespaces", s.prefix)
}

func (s *RedisStore) atespaceIndexKey(atespace string) string {
	return fmt.Sprintf("%s:usages:atespace:%s", s.prefix, atespace)
}

// Get implements Store.
func (s *RedisStore) Get(ctx context.Context, key Key) (*TaskUsage, error) {
	key = key.normalize()
	b, err := s.client.Get(ctx, s.key(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting usage from redis: %w", err)
	}
	return unmarshalUsage(b)
}

// Update implements Store.
func (s *RedisStore) Update(ctx context.Context, key Key, fn func(*TaskUsage) error) error {
	key = key.normalize()
	rkey := s.key(key)
	txf := func(tx *redis.Tx) error {
		u := NewTaskUsage()
		b, err := tx.Get(ctx, rkey).Bytes()
		switch {
		case errors.Is(err, redis.Nil):
		case err != nil:
			return fmt.Errorf("getting usage from redis: %w", err)
		default:
			if u, err = unmarshalUsage(b); err != nil {
				return err
			}
		}
		if err := fn(u); err != nil {
			return err
		}
		data, err := marshalUsage(u)
		if err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, rkey, data, 0)
			p.SAdd(ctx, s.atespaceIndexKey(key.Atespace), key.Name)
			p.SAdd(ctx, s.atespacesKey(), key.Atespace)
			return nil
		})
		return err
	}
	for range redisMaxRetries {
		err := s.client.Watch(ctx, txf, rkey)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, redis.TxFailedErr):
			continue
		case errors.Is(err, ErrNoChange):
			return nil
		default:
			return err
		}
	}
	return ErrConflict
}

// Delete implements Store.
func (s *RedisStore) Delete(ctx context.Context, key Key) error {
	key = key.normalize()
	_, err := s.client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, s.key(key))
		p.SRem(ctx, s.atespaceIndexKey(key.Atespace), key.Name)
		return nil
	})
	if err != nil {
		return fmt.Errorf("deleting usage from redis: %w", err)
	}
	return nil
}

// List implements Store.
func (s *RedisStore) List(ctx context.Context, atespace string) (map[Key]*TaskUsage, error) {
	atespaces := []string{atespace}
	if atespace == "" {
		var err error
		if atespaces, err = s.client.SMembers(ctx, s.atespacesKey()).Result(); err != nil {
			return nil, fmt.Errorf("listing usage atespaces: %w", err)
		}
	}
	out := map[Key]*TaskUsage{}
	for _, as := range atespaces {
		names, err := s.client.SMembers(ctx, s.atespaceIndexKey(as)).Result()
		if err != nil {
			return nil, fmt.Errorf("listing usage index: %w", err)
		}
		if len(names) == 0 {
			continue
		}
		keys := make([]string, len(names))
		for i, n := range names {
			keys[i] = s.key(Key{Atespace: as, Name: n})
		}
		vals, err := s.client.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("getting usage from redis: %w", err)
		}
		for i, v := range vals {
			str, ok := v.(string)
			if !ok {
				continue // deleted concurrently
			}
			u, err := unmarshalUsage([]byte(str))
			if err != nil {
				return nil, err
			}
			out[Key{Atespace: as, Name: names[i]}] = u
		}
	}
	return out, nil
}
