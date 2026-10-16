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

// Package cost prices usage deltas with a rate card loaded from a YAML file
// (ax-server --rate-card).
//
// Rate card format (all prices in Currency; model prices per 1M tokens):
//
//	currency: USD
//	compute: { default: { vcpuHour: 0.0445, gibHour: 0.0049 }, pools: { gpu: { vcpuHour: 0.09 } } }
//	storage: { gibMonth: 0.020 }   # GiB-month = 30 days
//	models:
//	  gemini-2.5-flash: { input: 0.30, output: 2.50, cacheRead: 0.03 }
//	idle: { cpuThreshold: 0.02, after: 5m }
//	budgets: { defaultWarnAtPercent: 80 }
//	metricLabels: [agent, customer]
//
// Unknown keys are rejected so that typos in prices are not silently ignored.
package cost

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"gopkg.in/yaml.v3"
)

// DefaultCurrency is the only currency accepted in the PoC.
const DefaultCurrency = "USD"

// DefaultWarnAtPercent is used when budgets.defaultWarnAtPercent is unset.
const DefaultWarnAtPercent = 80

// tokensPerPrice is the token count model prices are quoted for.
const tokensPerPrice = 1e6

// storageMonthSeconds is the length of a GiB-month: 30 days.
const storageMonthSeconds = 30 * 86400

// RateCard is the parsed rate card. Treat a RateCard returned by this package
// as immutable; it may be shared between goroutines.
type RateCard struct {
	Currency     string                `yaml:"currency"`
	Compute      ComputeRates          `yaml:"compute"`
	Storage      StorageRates          `yaml:"storage"`
	Models       map[string]ModelRates `yaml:"models"`
	Idle         IdleConfig            `yaml:"idle"`
	Budgets      BudgetDefaults        `yaml:"budgets"`
	MetricLabels []string              `yaml:"metricLabels"`
}

// ComputeRates are the per-hour compute prices: a default and optional
// per-pool overrides (keyed by the Substrate worker pool name).
type ComputeRates struct {
	Default ComputeRate         `yaml:"default"`
	Pools   map[string]PoolRate `yaml:"pools"`
}

// ComputeRate is a price per vCPU-hour and per GiB-hour of memory.
type ComputeRate struct {
	VCPUHour float64 `yaml:"vcpuHour"`
	GiBHour  float64 `yaml:"gibHour"`
}

// PoolRate overrides the default compute rate for one pool; unset fields
// fall back to the default.
type PoolRate struct {
	VCPUHour *float64 `yaml:"vcpuHour"`
	GiBHour  *float64 `yaml:"gibHour"`
}

// StorageRates is the workspace storage price per GiB-month (30 days).
type StorageRates struct {
	GiBMonth float64 `yaml:"gibMonth"`
}

// ModelRates are token prices per 1M tokens. Unset CacheRead and CacheWrite
// fall back to Input; unset Reasoning falls back to Output.
type ModelRates struct {
	Input      float64  `yaml:"input"`
	Output     float64  `yaml:"output"`
	CacheRead  *float64 `yaml:"cacheRead"`
	CacheWrite *float64 `yaml:"cacheWrite"`
	Reasoning  *float64 `yaml:"reasoning"`
}

// IdleConfig configures idle detection. Zero values mean the usage package
// defaults (usage.DefaultIdleCPUThreshold, usage.DefaultIdleAfter).
type IdleConfig struct {
	CPUThreshold float64       `yaml:"cpuThreshold"`
	After        time.Duration `yaml:"after"`
}

// BudgetDefaults are defaults for task budgets.
type BudgetDefaults struct {
	DefaultWarnAtPercent int32 `yaml:"defaultWarnAtPercent"`
}

// Parse parses and validates a rate card, applying defaults (currency USD,
// defaultWarnAtPercent 80).
func Parse(data []byte) (*RateCard, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c RateCard
	if err := dec.Decode(&c); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("rate card is empty")
		}
		return nil, fmt.Errorf("parse rate card: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// LoadFile reads and parses a rate card file.
func LoadFile(path string) (*RateCard, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rate card: %w", err)
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

func (c *RateCard) applyDefaults() {
	if c.Currency == "" {
		c.Currency = DefaultCurrency
	}
	if c.Budgets.DefaultWarnAtPercent == 0 {
		c.Budgets.DefaultWarnAtPercent = DefaultWarnAtPercent
	}
}

// Validate checks the rate card: currency must be USD (PoC), every price
// must be a finite non-negative number, idle settings non-negative,
// defaultWarnAtPercent in 1..100, metric labels valid unique label keys.
func (c *RateCard) Validate() error {
	var errs []error
	if c.Currency != DefaultCurrency {
		errs = append(errs, fmt.Errorf("currency %q not supported (PoC supports only %s)", c.Currency, DefaultCurrency))
	}
	checkPrice := func(field string, v float64) {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			errs = append(errs, fmt.Errorf("%s: price must be a finite non-negative number, got %v", field, v))
		}
	}
	checkOptPrice := func(field string, v *float64) {
		if v != nil {
			checkPrice(field, *v)
		}
	}
	checkPrice("compute.default.vcpuHour", c.Compute.Default.VCPUHour)
	checkPrice("compute.default.gibHour", c.Compute.Default.GiBHour)
	for _, name := range sortedKeys(c.Compute.Pools) {
		if name == "" {
			errs = append(errs, errors.New("compute.pools: empty pool name"))
		}
		p := c.Compute.Pools[name]
		checkOptPrice("compute.pools."+name+".vcpuHour", p.VCPUHour)
		checkOptPrice("compute.pools."+name+".gibHour", p.GiBHour)
	}
	checkPrice("storage.gibMonth", c.Storage.GiBMonth)
	seen := map[string]string{}
	for _, name := range sortedKeys(c.Models) {
		norm := normalizeModel(name)
		if norm == "" {
			errs = append(errs, fmt.Errorf("models: invalid model name %q", name))
			continue
		}
		if prev, ok := seen[norm]; ok {
			errs = append(errs, fmt.Errorf("models: %q and %q name the same model", prev, name))
		}
		seen[norm] = name
		m := c.Models[name]
		f := "models." + name
		checkPrice(f+".input", m.Input)
		checkPrice(f+".output", m.Output)
		checkOptPrice(f+".cacheRead", m.CacheRead)
		checkOptPrice(f+".cacheWrite", m.CacheWrite)
		checkOptPrice(f+".reasoning", m.Reasoning)
	}
	if math.IsNaN(c.Idle.CPUThreshold) || math.IsInf(c.Idle.CPUThreshold, 0) || c.Idle.CPUThreshold < 0 {
		errs = append(errs, fmt.Errorf("idle.cpuThreshold must be a finite non-negative number, got %v", c.Idle.CPUThreshold))
	}
	if c.Idle.After < 0 {
		errs = append(errs, fmt.Errorf("idle.after must not be negative, got %v", c.Idle.After))
	}
	if p := c.Budgets.DefaultWarnAtPercent; p < 1 || p > 100 {
		errs = append(errs, fmt.Errorf("budgets.defaultWarnAtPercent must be in 1..100, got %d", p))
	}
	labels := map[string]bool{}
	for _, k := range c.MetricLabels {
		if err := v1alpha1.ValidateLabelKey(k); err != nil {
			errs = append(errs, fmt.Errorf("metricLabels: %w", err))
		}
		if labels[k] {
			errs = append(errs, fmt.Errorf("metricLabels: duplicate key %q", k))
		}
		labels[k] = true
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid rate card: %w", errors.Join(errs...))
	}
	return nil
}

// AccountingConfig returns the idle settings as a usage.AccountingConfig
// (zero fields mean the usage package defaults).
func (c *RateCard) AccountingConfig() usage.AccountingConfig {
	return usage.AccountingConfig{IdleCPUThreshold: c.Idle.CPUThreshold, IdleAfter: c.Idle.After}
}

// ComputeRate returns the effective compute rate for pool: the pool override
// where set, the default otherwise (also for an empty or unknown pool).
func (c *RateCard) ComputeRate(pool string) ComputeRate {
	r := c.Compute.Default
	if p, ok := c.Compute.Pools[pool]; ok && pool != "" {
		if p.VCPUHour != nil {
			r.VCPUHour = *p.VCPUHour
		}
		if p.GiBHour != nil {
			r.GiBHour = *p.GiBHour
		}
	}
	return r
}

// versionSuffix matches a trailing numeric model version such as "-001".
var versionSuffix = regexp.MustCompile(`-[0-9]+$`)

// ModelRates returns the rates for model. Names are matched case-
// insensitively after stripping any resource path prefix (for example
// "models/" or "publishers/google/models/"); if there is no exact match, a
// trailing numeric version ("-001") is stripped and the lookup retried.
func (c *RateCard) ModelRates(model string) (ModelRates, bool) {
	if r, ok := c.Models[model]; ok {
		return r, true
	}
	want := normalizeModel(model)
	if want == "" {
		return ModelRates{}, false
	}
	for _, cand := range []string{want, versionSuffix.ReplaceAllString(want, "")} {
		for name, r := range c.Models {
			if normalizeModel(name) == cand {
				return r, true
			}
		}
	}
	return ModelRates{}, false
}

func normalizeModel(m string) string {
	m = strings.ToLower(strings.TrimSpace(m))
	if i := strings.LastIndex(m, "/"); i >= 0 {
		m = m[i+1:]
	}
	return m
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
