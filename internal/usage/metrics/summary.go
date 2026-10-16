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

package metrics

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"github.com/google/ax/internal/usage"
)

// SummaryMessage is the msg of per-task usage summary log lines.
const SummaryMessage = "ax.task.usage_summary"

// DefaultSummaryInterval is how often RunPeriodic summarizes running tasks.
const DefaultSummaryInterval = 10 * time.Minute

// Summary reasons (the "reason" field of a summary line).
const (
	ReasonSuspend   = "suspend"
	ReasonDelete    = "delete"
	ReasonThreshold = "threshold"
	ReasonPeriodic  = "periodic"
)

// TaskSummary is the input of one summary line. Usage may be nil (no usage
// recorded yet); the line then carries zeros.
type TaskSummary struct {
	Key    usage.Key
	Labels map[string]string
	Usage  *usage.TaskUsage
}

// RunningTasksFunc lists the tasks to summarize periodically (the server
// returns its Running tasks joined with their usage records).
type RunningTasksFunc func(ctx context.Context) ([]TaskSummary, error)

// SummaryLogger writes ax.task.usage_summary lines. With a JSON handler a
// line looks like:
//
//	{"time":"...","level":"INFO","msg":"ax.task.usage_summary","reason":"suspend",
//	 "atespace":"default","task":"t1","labels":{"agent":"triage"},"actor_uid":"...",
//	 "active_seconds":120,"cpu_seconds":30.5,"memory_gib_seconds":60,"idle_seconds":0,
//	 "storage_gib_seconds":0,"workspace_bytes":0,"tokens_total":1500,
//	 "tokens":{"gemini-2.5-flash":{"input":1000,"output":500,...}},
//	 "cost_currency":"USD","cost_total":0.01,"cost_compute":0.002,"cost_tokens":0.008,
//	 "cost_storage":0,"cost_idle":0,"cost_tokens_partially_unpriced":false,"update_time":"..."}
//
// Labels are the task's full label set (logs are per task, so there is no
// cardinality concern). tokens_total = Σ input + output (GenAI semconv: input
// includes cache tokens, output includes reasoning).
type SummaryLogger struct {
	logger *slog.Logger
}

// NewSummaryLogger returns a SummaryLogger writing to l (slog.Default() when
// nil).
func NewSummaryLogger(l *slog.Logger) *SummaryLogger {
	if l == nil {
		l = slog.Default()
	}
	return &SummaryLogger{logger: l}
}

// TokenSummary is the per-model token entry of a summary line.
type TokenSummary struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cache_read"`
	CacheWrite int64 `json:"cache_write"`
	Reasoning  int64 `json:"reasoning"`
}

// Log writes one summary line for t. reason is one of the Reason* constants.
func (s *SummaryLogger) Log(ctx context.Context, reason string, t TaskSummary) {
	s.logger.LogAttrs(ctx, slog.LevelInfo, SummaryMessage, SummaryAttrs(reason, t)...)
}

// SummaryAttrs returns the attributes of a summary line (exported so callers
// with their own logger can emit the same line).
func SummaryAttrs(reason string, t TaskSummary) []slog.Attr {
	u := t.Usage
	if u == nil {
		u = usage.NewTaskUsage()
	}
	atespace := t.Key.Atespace
	if atespace == "" {
		atespace = defaultAtespace
	}
	labels := t.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	tokens := make(map[string]TokenSummary, len(u.Tokens))
	var total int64
	models := make([]string, 0, len(u.Tokens))
	for m := range u.Tokens {
		models = append(models, m)
	}
	sort.Strings(models)
	for _, m := range models {
		c := u.Tokens[m]
		if c.IsZero() {
			continue
		}
		tokens[m] = TokenSummary{Input: c.Input, Output: c.Output, CacheRead: c.CacheRead, CacheWrite: c.CacheWrite, Reasoning: c.Reasoning}
		total += c.Input + c.Output
	}
	attrs := []slog.Attr{
		slog.String("reason", reason),
		slog.String("atespace", atespace),
		slog.String("task", t.Key.Name),
		slog.Any("labels", labels),
		slog.String("actor_uid", u.State.ActorUID),
		slog.Float64("active_seconds", u.ActiveSeconds),
		slog.Float64("cpu_seconds", u.CPUSeconds),
		slog.Float64("memory_gib_seconds", u.MemoryGiBSeconds),
		slog.Float64("idle_seconds", u.IdleSeconds),
		slog.Float64("storage_gib_seconds", u.StorageGiBSeconds),
		slog.Int64("workspace_bytes", u.WorkspaceBytes),
		slog.Int64("tokens_total", total),
		slog.Any("tokens", tokens),
		slog.String("cost_currency", u.Cost.Currency),
		slog.Float64("cost_total", u.Cost.Total),
		slog.Float64("cost_compute", u.Cost.Compute),
		slog.Float64("cost_tokens", u.Cost.Tokens),
		slog.Float64("cost_storage", u.Cost.Storage),
		slog.Float64("cost_idle", u.Cost.Idle),
		slog.Bool("cost_tokens_partially_unpriced", u.Cost.TokensPartiallyUnpriced),
	}
	if !u.UpdateTime.IsZero() {
		attrs = append(attrs, slog.Time("update_time", u.UpdateTime))
	}
	return attrs
}

// Sweep writes a ReasonPeriodic line for every task list returns.
func (s *SummaryLogger) Sweep(ctx context.Context, list RunningTasksFunc) error {
	tasks, err := list(ctx)
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.Log(ctx, ReasonPeriodic, t)
	}
	return nil
}

// RunPeriodic calls Sweep every interval (DefaultSummaryInterval when <= 0)
// until ctx is done. The first sweep happens one interval after the start.
// List errors are logged and the loop continues. Start it in a goroutine.
func (s *SummaryLogger) RunPeriodic(ctx context.Context, interval time.Duration, list RunningTasksFunc) {
	if interval <= 0 {
		interval = DefaultSummaryInterval
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := s.Sweep(ctx, list); err != nil && ctx.Err() == nil {
				s.logger.WarnContext(ctx, "usage summary sweep failed", "err", err)
			}
		}
	}
}
