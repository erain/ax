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

package server

import (
	"context"
	"errors"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/usage/puller"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

var _ puller.TaskGetter = (*Server)(nil)

// LookupTask re-reads one task from the task store without taking its lock
// (puller.TaskGetter: the router-mode puller re-checks phase and worker IP
// right before each pull). A missing task is (nil, nil).
func (s *Server) LookupTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	t, err := s.store.GetTask(ctx, atespace, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return t, err
}
