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

package cost

import (
	"github.com/google/ax/internal/usage"
)

// Charge kinds of a ChargeLine.
const (
	ChargeCPU     = "cpu"
	ChargeMemory  = "memory"
	ChargeTokens  = "tokens"
	ChargeStorage = "storage"
)

// Token types of a tokens ChargeLine. The quantities do not overlap: input is
// uncached input (OTel GenAI input minus cache reads and writes) and output
// excludes reasoning.
const (
	TokenInput      = "input"
	TokenCacheRead  = "cache_read"
	TokenCacheWrite = "cache_write"
	TokenOutput     = "output"
	TokenReasoning  = "reasoning"
)

// Units of a ChargeLine (FOCUS ConsumedUnit / PricingUnit values).
const (
	UnitVCPUHours  = "vCPU-Hours"
	UnitGiBHours   = "GiB-Hours"
	UnitTokens     = "Tokens"
	UnitMTokens    = "1M Tokens"
	UnitGiBMonths  = "GiB-Months"
	secondsPerHour = 3600
)

// ChargeLine is one priced component of a day's usage.
type ChargeLine struct {
	Kind      string // ChargeCPU, ChargeMemory, ChargeTokens or ChargeStorage
	Model     string // tokens only
	TokenType string // tokens only

	Quantity        float64 // in Unit
	Unit            string
	PricingQuantity float64 // in PricingUnit
	PricingUnit     string
	// ListUnitPrice is the rate card price per PricingUnit; HasListPrice is
	// false when there is none (no rate card, unknown model).
	ListUnitPrice float64
	HasListPrice  bool

	Cost     float64 // share of the frozen day cost
	IdleCost float64 // share of the frozen idle cost (cpu and memory only)
}

// Breakdown splits a day's frozen cost into charge lines: compute into vCPU
// and memory, tokens per model and token type, and storage. Each frozen
// component (Cost.Compute, Cost.Tokens, Cost.Storage, and Cost.Idle within
// compute) is split over its lines in proportion to their list-price cost
// under c (nil = no rate card), with the last line taking the rounding
// remainder, so the line costs sum to day.Cost.Total. The split is exact when
// the day was priced with the same rate card; after a rate card change or
// with provider-reported token costs it is an allocation. Without list-price
// weights, compute goes to the cpu line and tokens are split by count. Lines
// with zero quantity and zero cost are omitted; the order is cpu, memory,
// tokens (by model, then token type), storage.
func (c *RateCard) Breakdown(day *usage.DayUsage, pool string) []ChargeLine {
	if day == nil {
		return nil
	}
	var rate ComputeRate
	var storageRate float64
	if c != nil {
		rate = c.ComputeRate(pool)
		storageRate = c.Storage.GiBMonth
	}

	// Compute: vCPU and memory.
	cpu := ChargeLine{Kind: ChargeCPU, Quantity: day.CPUSeconds / secondsPerHour, Unit: UnitVCPUHours,
		PricingUnit: UnitVCPUHours, ListUnitPrice: rate.VCPUHour, HasListPrice: c != nil}
	mem := ChargeLine{Kind: ChargeMemory, Quantity: day.MemoryGiBSeconds / secondsPerHour, Unit: UnitGiBHours,
		PricingUnit: UnitGiBHours, ListUnitPrice: rate.GiBHour, HasListPrice: c != nil}
	compute := allocate(day.Cost.Compute,
		[]float64{cpu.Quantity * cpu.ListUnitPrice, mem.Quantity * mem.ListUnitPrice}, []float64{1, 0})
	idle := allocate(day.Cost.Idle,
		[]float64{day.IdleCPUSeconds / secondsPerHour * rate.VCPUHour, day.IdleMemoryGiBSeconds / secondsPerHour * rate.GiBHour},
		[]float64{1, 0})
	cpu.Cost, mem.Cost = compute[0], compute[1]
	cpu.IdleCost, mem.IdleCost = idle[0], idle[1]
	lines := []ChargeLine{cpu, mem}

	// Tokens: per model and token type.
	var tokens []ChargeLine
	var weights, counts []float64
	for _, model := range sortedKeys(day.Tokens) {
		t := day.Tokens[model]
		var rates ModelRates
		priced := false
		if c != nil {
			rates, priced = c.ModelRates(model)
		}
		reasoning := min(max(0, t.Reasoning), max(0, t.Output))
		for _, p := range []struct {
			typ   string
			n     int64
			price float64
		}{
			{TokenInput, max(0, t.Input-t.CacheRead-t.CacheWrite), rates.Input},
			{TokenCacheRead, t.CacheRead, opt(rates.CacheRead, rates.Input)},
			{TokenCacheWrite, t.CacheWrite, opt(rates.CacheWrite, rates.Input)},
			{TokenOutput, t.Output - reasoning, rates.Output},
			{TokenReasoning, reasoning, opt(rates.Reasoning, rates.Output)},
		} {
			if p.n <= 0 {
				continue
			}
			l := ChargeLine{Kind: ChargeTokens, Model: model, TokenType: p.typ,
				Quantity: float64(p.n), Unit: UnitTokens, PricingQuantity: float64(p.n) / tokensPerPrice, PricingUnit: UnitMTokens}
			if priced {
				l.ListUnitPrice, l.HasListPrice = p.price, true
			}
			tokens = append(tokens, l)
			weights = append(weights, l.PricingQuantity*l.ListUnitPrice)
			counts = append(counts, l.Quantity)
		}
	}
	if len(tokens) == 0 && day.Cost.Tokens != 0 {
		// A cost without token counts (e.g. provider-reported only).
		tokens = append(tokens, ChargeLine{Kind: ChargeTokens, Unit: UnitTokens, PricingUnit: UnitMTokens})
		weights, counts = []float64{0}, []float64{1}
	}
	for i, share := range allocate(day.Cost.Tokens, weights, counts) {
		tokens[i].Cost = share
	}
	lines = append(lines, tokens...)

	// Storage.
	lines = append(lines, ChargeLine{Kind: ChargeStorage, Quantity: day.StorageGiBSeconds / storageMonthSeconds,
		Unit: UnitGiBMonths, PricingUnit: UnitGiBMonths, ListUnitPrice: storageRate, HasListPrice: c != nil,
		Cost: day.Cost.Storage})

	out := lines[:0]
	for _, l := range lines {
		if l.Quantity == 0 && l.Cost == 0 && l.IdleCost == 0 {
			continue
		}
		if l.Kind != ChargeTokens {
			l.PricingQuantity = l.Quantity
		}
		out = append(out, l)
	}
	return out
}

// allocate splits total over len(weights) shares in proportion to weights,
// or to fallback when the weights sum to zero (equal shares when both do).
// The last share with a non-zero weight takes the remainder, so the shares
// sum to total up to one rounding step.
func allocate(total float64, weights, fallback []float64) []float64 {
	out := make([]float64, len(weights))
	if total == 0 || len(weights) == 0 {
		return out
	}
	w := weights
	if sum(w) <= 0 {
		w = fallback
	}
	if sum(w) <= 0 {
		w = make([]float64, len(weights))
		for i := range w {
			w[i] = 1
		}
	}
	s := sum(w)
	last := -1
	for i, x := range w {
		if x > 0 {
			last = i
		}
	}
	allocated := 0.0
	for i, x := range w {
		if x <= 0 || i == last {
			continue
		}
		out[i] = total * x / s
		allocated += out[i]
	}
	out[last] = total - allocated
	return out
}

func sum(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s
}
