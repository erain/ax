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

// Package metrics turns priced usage deltas into OpenTelemetry metrics and
// writes per-task usage summary log lines.
//
// Metrics (all monotonic counters, cumulative temporality when exported by
// internal/telemetry):
//
//	ax.task.cost                 USD      ax.cost.kind = compute|idle|tokens|storage
//	ax.task.tokens               {token}  gen_ai.request.model, gen_ai.token.type
//	ax.task.cpu.time             s
//	ax.task.active.time          s
//	ax.task.idle.time            s
//	ax.budget.threshold_crossed  {event}  ax.budget.dimension, ax.budget.level
//
// Every series carries ax.atespace and ax.label.<key> for each configured
// metric label key (rate card metricLabels); a task without that label, or
// with an empty value, reports "none". There are no per-task attributes, so
// cardinality is bounded by atespaces x label values x kinds/models.
//
// The ax.cost.kind values are disjoint so that summing ax.task.cost over all
// kinds gives the total cost: "compute" is the non-idle compute cost
// (Cost.Compute - Cost.Idle) and "idle" is the idle compute cost (Cost.Idle).
//
// Token types follow the GenAI semantic conventions, as recorded by
// internal/usage: "input" includes cache_read and cache_write, and "output"
// includes reasoning. Do not add the types together for a total.
package metrics

import (
	"context"
	"sort"

	"github.com/google/ax/internal/usage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metric names, units and the instrumentation scope name.
const (
	MetricCost             = "ax.task.cost"
	MetricTokens           = "ax.task.tokens"
	MetricCPUTime          = "ax.task.cpu.time"
	MetricActiveTime       = "ax.task.active.time"
	MetricIdleTime         = "ax.task.idle.time"
	MetricThresholdCrossed = "ax.budget.threshold_crossed"

	UnitUSD     = "USD"
	UnitToken   = "{token}"
	UnitSeconds = "s"
	UnitEvent   = "{event}"

	// ScopeName is the meter name to use for the Recorder.
	ScopeName = "github.com/google/ax/internal/usage/metrics"
)

// Attribute keys and values.
const (
	AttrAtespace        = "ax.atespace"
	AttrLabelPrefix     = "ax.label."
	AttrCostKind        = "ax.cost.kind"
	AttrModel           = "gen_ai.request.model"
	AttrTokenType       = "gen_ai.token.type"
	AttrBudgetDimension = "ax.budget.dimension"
	AttrBudgetLevel     = "ax.budget.level"

	// NoneValue is the ax.label.<k> value of a task without label k.
	NoneValue = "none"

	CostKindCompute = "compute"
	CostKindIdle    = "idle"
	CostKindTokens  = "tokens"
	CostKindStorage = "storage"

	TokenTypeInput      = "input"
	TokenTypeOutput     = "output"
	TokenTypeCacheRead  = "cache_read"
	TokenTypeCacheWrite = "cache_write"
	TokenTypeReasoning  = "reasoning"
)

const (
	defaultAtespace         = "default"
	unknownValue            = "unknown"
	attributeValueMaxLength = 256
)

// Budget dimensions and levels for OnThreshold (ax.budget.dimension and
// ax.budget.level attribute values).
const (
	DimensionActiveDeadline = "activeDeadline"
	DimensionMaxTokens      = "maxTokens"
	DimensionMaxCost        = "maxCost"
	LevelWarning            = "warning"
	LevelExceeded           = "exceeded"
)

// LabelLookup returns the labels of the task identified by key. A nil map
// (unknown task, lookup failure) makes every metric label "none". The server
// backs it with the task store; task labels are immutable, so implementations
// may cache.
type LabelLookup interface {
	Labels(ctx context.Context, key usage.Key) map[string]string
}

// LabelLookupFunc adapts a function to LabelLookup.
type LabelLookupFunc func(ctx context.Context, key usage.Key) map[string]string

// Labels implements LabelLookup.
func (f LabelLookupFunc) Labels(ctx context.Context, key usage.Key) map[string]string {
	return f(ctx, key)
}

// Recorder records usage metrics. It implements usage.Hooks and is safe for
// concurrent use.
type Recorder struct {
	labelKeys func() []string
	labels    LabelLookup

	cost      metric.Float64Counter
	tokens    metric.Int64Counter
	cpu       metric.Float64Counter
	active    metric.Float64Counter
	idle      metric.Float64Counter
	threshold metric.Int64Counter
}

var _ usage.Hooks = (*Recorder)(nil)

// New creates the instruments on meter. labelKeys returns the task label keys
// promoted to metric attributes (e.g. the current rate card's MetricLabels;
// it is called on every record, so hot-reloaded rate cards take effect); nil
// means none. labels resolves a task's labels; nil means every label is
// "none".
func New(meter metric.Meter, labelKeys func() []string, labels LabelLookup) (*Recorder, error) {
	r := &Recorder{labelKeys: labelKeys, labels: labels}
	var err error
	if r.cost, err = meter.Float64Counter(MetricCost, metric.WithUnit(UnitUSD),
		metric.WithDescription("Priced cost of AX tasks, by cost kind (kinds are disjoint; sum = total cost).")); err != nil {
		return nil, err
	}
	if r.tokens, err = meter.Int64Counter(MetricTokens, metric.WithUnit(UnitToken),
		metric.WithDescription("Model tokens used by AX tasks (GenAI semconv: input includes cache, output includes reasoning).")); err != nil {
		return nil, err
	}
	if r.cpu, err = meter.Float64Counter(MetricCPUTime, metric.WithUnit(UnitSeconds),
		metric.WithDescription("CPU time consumed by AX task sandboxes.")); err != nil {
		return nil, err
	}
	if r.active, err = meter.Float64Counter(MetricActiveTime, metric.WithUnit(UnitSeconds),
		metric.WithDescription("Wall time AX tasks spent running.")); err != nil {
		return nil, err
	}
	if r.idle, err = meter.Float64Counter(MetricIdleTime, metric.WithUnit(UnitSeconds),
		metric.WithDescription("Running time classified as idle (subset of active time).")); err != nil {
		return nil, err
	}
	if r.threshold, err = meter.Int64Counter(MetricThresholdCrossed, metric.WithUnit(UnitEvent),
		metric.WithDescription("Soft budget threshold crossings of AX tasks.")); err != nil {
		return nil, err
	}
	return r, nil
}

// OnDelta implements usage.Hooks: it adds the delta's amounts and its cost to
// the counters. Zero amounts are not recorded.
func (r *Recorder) OnDelta(ctx context.Context, key usage.Key, d usage.Delta, c usage.Cost, _ *usage.TaskUsage) {
	sum := d.Sum()
	base := r.commonAttributes(ctx, key)

	idleCost := positive(c.Idle)
	r.addFloat(ctx, r.cost, positive(c.Compute-idleCost), base, attribute.String(AttrCostKind, CostKindCompute))
	r.addFloat(ctx, r.cost, idleCost, base, attribute.String(AttrCostKind, CostKindIdle))
	r.addFloat(ctx, r.cost, positive(c.Tokens), base, attribute.String(AttrCostKind, CostKindTokens))
	r.addFloat(ctx, r.cost, positive(c.Storage), base, attribute.String(AttrCostKind, CostKindStorage))

	r.addFloat(ctx, r.cpu, positive(sum.CPUSeconds), base)
	r.addFloat(ctx, r.active, positive(sum.ActiveSeconds), base)
	r.addFloat(ctx, r.idle, positive(sum.IdleSeconds), base)

	models := make([]string, 0, len(sum.Tokens))
	for m := range sum.Tokens {
		models = append(models, m)
	}
	sort.Strings(models)
	for _, m := range models {
		t := sum.Tokens[m]
		model := attribute.String(AttrModel, attrValue(m, unknownValue))
		for _, tt := range []struct {
			typ string
			n   int64
		}{
			{TokenTypeInput, t.Input},
			{TokenTypeOutput, t.Output},
			{TokenTypeCacheRead, t.CacheRead},
			{TokenTypeCacheWrite, t.CacheWrite},
			{TokenTypeReasoning, t.Reasoning},
		} {
			if tt.n > 0 {
				r.tokens.Add(ctx, tt.n, metric.WithAttributes(with(base, model, attribute.String(AttrTokenType, tt.typ))...))
			}
		}
	}
}

// OnThreshold records one soft budget threshold crossing of the task key.
// dimension is one of the Dimension* constants and level one of the Level*
// constants (other non-empty values are recorded as given). Callers (budget
// evaluation) call it once per transition, not on every delta.
func (r *Recorder) OnThreshold(ctx context.Context, key usage.Key, dimension, level string) {
	base := r.commonAttributes(ctx, key)
	r.threshold.Add(ctx, 1, metric.WithAttributes(with(base,
		attribute.String(AttrBudgetDimension, attrValue(dimension, unknownValue)),
		attribute.String(AttrBudgetLevel, attrValue(level, unknownValue)))...))
}

// commonAttributes returns ax.atespace and ax.label.<k> for the configured
// label keys. Only configured keys appear, whatever labels the task has.
func (r *Recorder) commonAttributes(ctx context.Context, key usage.Key) []attribute.KeyValue {
	atespace := key.Atespace
	if atespace == "" {
		atespace = defaultAtespace
	}
	var keys []string
	if r.labelKeys != nil {
		keys = r.labelKeys()
	}
	attrs := make([]attribute.KeyValue, 0, 1+len(keys))
	attrs = append(attrs, attribute.String(AttrAtespace, atespace))
	if len(keys) == 0 {
		return attrs
	}
	var labels map[string]string
	if r.labels != nil {
		labels = r.labels.Labels(ctx, key)
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		attrs = append(attrs, attribute.String(AttrLabelPrefix+k, attrValue(labels[k], NoneValue)))
	}
	return attrs
}

func (r *Recorder) addFloat(ctx context.Context, c metric.Float64Counter, v float64, base []attribute.KeyValue, extra ...attribute.KeyValue) {
	if v <= 0 {
		return
	}
	c.Add(ctx, v, metric.WithAttributes(with(base, extra...)...))
}

// with returns base + extra in a new slice (base is shared between calls).
func with(base []attribute.KeyValue, extra ...attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(base)+len(extra))
	out = append(out, base...)
	return append(out, extra...)
}

// attrValue returns v, or def when v is empty; long values are truncated.
func attrValue(v, def string) string {
	if v == "" {
		return def
	}
	if len(v) > attributeValueMaxLength {
		return v[:attributeValueMaxLength]
	}
	return v
}

// positive returns v when v > 0 (and not NaN), else 0. Counters only accept
// non-negative increments.
func positive(v float64) float64 {
	if v > 0 {
		return v
	}
	return 0
}
