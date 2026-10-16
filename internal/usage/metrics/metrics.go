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
//	ax.task.cost                 {USD}    ax.cost.kind = compute|idle|tokens|storage
//	ax.task.tokens               {token}  gen_ai.request.model, gen_ai.token.type
//	ax.task.cpu.time             s
//	ax.task.active.time          s
//	ax.task.idle.time            s
//	ax.budget.threshold_crossed  {event}  ax.budget.dimension, ax.budget.level
//
// Every series carries ax.atespace and ax.label.<key> for each configured
// metric label key (rate card metricLabels); a task without that label, or
// with an empty value, reports "none". Label keys are sanitized to the Cloud
// Monitoring attribute key syntax [a-zA-Z_][a-zA-Z0-9_.]* (other characters
// become "_", e.g. "cost-center" -> ax.label.cost_center and
// "example.com/team" -> ax.label.example.com_team; colliding keys get a
// "_2", "_3", ... suffix in sorted order of the original keys). There are no
// per-task attributes, so cardinality is bounded by atespaces x label values
// x kinds/models; with WithKnownModels, models missing from the rate card are
// grouped as gen_ai.request.model="other".
//
// Attribute values are made valid UTF-8 and truncated (on a rune boundary)
// to 256 bytes: one invalid string would make the OTLP protobuf marshal, and
// so the whole export, fail.
//
// The cost unit is {USD}: the PoC rate card only accepts USD (see
// internal/cost).
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
	"strconv"
	"strings"
	"unicode/utf8"

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

	UnitUSD     = "{USD}"
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

	// OtherModel is the gen_ai.request.model value of models that are not
	// known (see WithKnownModels).
	OtherModel = "other"

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
	labelKeys  func() []string
	labels     LabelLookup
	knownModel func(string) bool

	cost      metric.Float64Counter
	tokens    metric.Int64Counter
	cpu       metric.Float64Counter
	active    metric.Float64Counter
	idle      metric.Float64Counter
	threshold metric.Int64Counter
}

var _ usage.Hooks = (*Recorder)(nil)

// Option configures a Recorder.
type Option func(*Recorder)

// WithKnownModels bounds the gen_ai.request.model cardinality: models for
// which known returns false are recorded as OtherModel. The server passes a
// rate card lookup, e.g.
//
//	func(m string) bool { _, ok := pricer.RateCard().ModelRates(m); return ok }
//
// Without this option every model name is recorded as reported.
func WithKnownModels(known func(model string) bool) Option {
	return func(r *Recorder) { r.knownModel = known }
}

// New creates the instruments on meter. labelKeys returns the task label keys
// promoted to metric attributes (e.g. the current rate card's MetricLabels;
// it is called on every record, so hot-reloaded rate cards take effect); nil
// means none. labels resolves a task's labels; nil means every label is
// "none".
func New(meter metric.Meter, labelKeys func() []string, labels LabelLookup, opts ...Option) (*Recorder, error) {
	r := &Recorder{labelKeys: labelKeys, labels: labels}
	for _, o := range opts {
		o(r)
	}
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
		model := attribute.String(AttrModel, r.modelValue(m))
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
	attrs = append(attrs, attribute.String(AttrAtespace, attrValue(atespace, defaultAtespace)))
	if len(keys) == 0 {
		return attrs
	}
	var labels map[string]string
	if r.labels != nil {
		labels = r.labels.Labels(ctx, key)
	}
	for _, lk := range labelAttributeKeys(keys) {
		attrs = append(attrs, attribute.String(lk.attr, attrValue(labels[lk.label], NoneValue)))
	}
	return attrs
}

// modelValue returns the gen_ai.request.model value for model.
func (r *Recorder) modelValue(model string) string {
	if model == "" {
		return unknownValue
	}
	if r.knownModel != nil && !r.knownModel(model) {
		return OtherModel
	}
	return attrValue(model, unknownValue)
}

type labelAttributeKey struct {
	label string // task label key
	attr  string // metric attribute key (AttrLabelPrefix + sanitized key)
}

// labelAttributeKeys maps the configured label keys to metric attribute keys.
// Empty and duplicate keys are dropped. Keys are sanitized to
// [a-zA-Z0-9_.] (the prefix makes the first character valid). When several
// keys sanitize to the same attribute key, the keys are taken in sorted order:
// the first keeps the name and later ones get "_2", "_3", ... (skipping names
// already in use), so the mapping depends only on the set of keys.
func labelAttributeKeys(keys []string) []labelAttributeKey {
	uniq := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k != "" && !seen[k] {
			seen[k] = true
			uniq = append(uniq, k)
		}
	}
	sort.Strings(uniq)
	// Plain sanitized names are reserved first so a suffixed name never takes
	// the plain name of another key.
	taken := make(map[string]bool, len(uniq))
	plain := make([]string, len(uniq))
	for i, k := range uniq {
		plain[i] = AttrLabelPrefix + sanitizeKey(k)
	}
	out := make([]labelAttributeKey, 0, len(uniq))
	var collided []int
	for i, k := range uniq {
		if taken[plain[i]] {
			collided = append(collided, i)
			continue
		}
		taken[plain[i]] = true
		out = append(out, labelAttributeKey{label: k, attr: plain[i]})
	}
	for _, i := range collided {
		for n := 2; ; n++ {
			name := plain[i] + "_" + strconv.Itoa(n)
			if !taken[name] {
				taken[name] = true
				out = append(out, labelAttributeKey{label: uniq[i], attr: name})
				break
			}
		}
	}
	return out
}

// sanitizeKey replaces every character outside [A-Za-z0-9_.] with "_".
func sanitizeKey(k string) string {
	var b strings.Builder
	b.Grow(len(k))
	for _, c := range k {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '.':
			b.WriteRune(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
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

// attrValue returns v, or def when v is empty. Invalid UTF-8 is replaced
// and long values are truncated on a rune boundary, so the result is always
// valid UTF-8 (an invalid string fails the OTLP marshal of the whole batch).
func attrValue(v, def string) string {
	if v == "" {
		return def
	}
	v = strings.ToValidUTF8(v, "\uFFFD")
	if len(v) > attributeValueMaxLength {
		i := attributeValueMaxLength
		for i > 0 && !utf8.RuneStart(v[i]) {
			i--
		}
		v = v[:i]
	}
	if v == "" {
		return def
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
