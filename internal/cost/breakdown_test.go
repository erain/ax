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
	"math"
	"math/rand/v2"
	"testing"

	"github.com/google/ax/internal/usage"
)

// pricedDay prices amounts with card (as the accounting path does) and
// returns the resulting DayUsage.
func pricedDay(card *RateCard, a usage.Amounts, pool string) *usage.DayUsage {
	c := card.Price(oneDay(usage.DayDelta{Amounts: a}), pool)
	return &usage.DayUsage{Amounts: a, Cost: c}
}

func lineSum(lines []ChargeLine) (cost, idle float64) {
	for _, l := range lines {
		cost += l.Cost
		idle += l.IdleCost
	}
	return cost, idle
}

func TestBreakdown_SameCardIsExact(t *testing.T) {
	card := testCard(t)
	a := usage.Amounts{
		CPUSeconds: 7200, MemoryGiBSeconds: 3600, IdleCPUSeconds: 1800, IdleMemoryGiBSeconds: 720,
		StorageGiBSeconds: 2_592_000, // 1 GiB-month
		Tokens: map[string]usage.TokenCounts{
			"m-full":  {Input: 1_000_000, Output: 500_000, CacheRead: 200_000, CacheWrite: 100_000, Reasoning: 100_000},
			"m-basic": {Input: 1_000_000, Output: 500_000, Reasoning: 200_000},
			"unknown": {Input: 10, Output: 5},
		},
	}
	day := pricedDay(card, a, "gpu")
	lines := card.Breakdown(day, "gpu")

	type key struct{ kind, model, typ string }
	want := map[key]struct{ qty, unit, cost float64 }{
		{ChargeCPU, "", ""}:                       {2, 0.10, 0.2},
		{ChargeMemory, "", ""}:                    {1, 0.01, 0.01},
		{ChargeTokens, "m-basic", TokenInput}:     {1_000_000, 1.0, 1.0},
		{ChargeTokens, "m-basic", TokenOutput}:    {300_000, 4.0, 1.2},
		{ChargeTokens, "m-basic", TokenReasoning}: {200_000, 4.0, 0.8}, // falls back to output rate
		{ChargeTokens, "m-full", TokenInput}:      {700_000, 2.0, 1.4},
		{ChargeTokens, "m-full", TokenCacheRead}:  {200_000, 0.5, 0.1},
		{ChargeTokens, "m-full", TokenCacheWrite}: {100_000, 2.5, 0.25},
		{ChargeTokens, "m-full", TokenOutput}:     {400_000, 8.0, 3.2},
		{ChargeTokens, "m-full", TokenReasoning}:  {100_000, 10.0, 1.0},
		{ChargeTokens, "unknown", TokenInput}:     {10, 0, 0},
		{ChargeTokens, "unknown", TokenOutput}:    {5, 0, 0},
		{ChargeStorage, "", ""}:                   {1, 0.03, 0.03},
	}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d: %+v", len(lines), len(want), lines)
	}
	for _, l := range lines {
		w, ok := want[key{l.Kind, l.Model, l.TokenType}]
		if !ok {
			t.Errorf("unexpected line %+v", l)
			continue
		}
		if !approx(l.Quantity, w.qty) || !approx(l.ListUnitPrice, w.unit) || !approx(l.Cost, w.cost) {
			t.Errorf("%s/%s/%s: qty=%v unit=%v cost=%v, want %v %v %v", l.Kind, l.Model, l.TokenType, l.Quantity, l.ListUnitPrice, l.Cost, w.qty, w.unit, w.cost)
		}
		if l.Kind == ChargeTokens {
			if l.HasListPrice != (l.Model != "unknown") || l.Unit != UnitTokens || l.PricingUnit != UnitMTokens || !approx(l.PricingQuantity, l.Quantity/1e6) {
				t.Errorf("tokens line %+v", l)
			}
		} else if l.PricingQuantity != l.Quantity || !l.HasListPrice {
			t.Errorf("line %+v", l)
		}
	}
	// Idle: cpu 0.5h × 0.10 = 0.05, memory 0.2 GiB-h × 0.01 = 0.002.
	if !approx(lines[0].IdleCost, 0.05) || !approx(lines[1].IdleCost, 0.002) {
		t.Errorf("idle = %v / %v", lines[0].IdleCost, lines[1].IdleCost)
	}
	c, idle := lineSum(lines)
	if math.Abs(c-day.Cost.Total) > 1e-12 || math.Abs(idle-day.Cost.Idle) > 1e-12 {
		t.Errorf("lines sum to %v (idle %v), day cost %+v", c, idle, day.Cost)
	}
}

func TestBreakdown_ChangedOrMissingCard(t *testing.T) {
	card := testCard(t)
	a := usage.Amounts{CPUSeconds: 3600, MemoryGiBSeconds: 3600, IdleCPUSeconds: 360,
		Tokens: map[string]usage.TokenCounts{"m-basic": {Input: 1000, Output: 3000}, "zzz": {Input: 1000}}}
	day := pricedDay(card, a, "")
	day.Cost.Tokens += 0.5 // e.g. a provider-reported cost
	day.Cost.Total += 0.5

	// The day is priced with a card that later doubled compute prices: the
	// frozen cost is kept and split in the new proportions.
	changed := testCard(t)
	changed.Compute.Default.VCPUHour *= 2
	for _, c := range []*RateCard{changed, nil} {
		lines := c.Breakdown(day, "")
		got, idle := lineSum(lines)
		if math.Abs(got-day.Cost.Total) > 1e-12 || math.Abs(idle-day.Cost.Idle) > 1e-12 {
			t.Errorf("card %v: lines sum to %v (idle %v), want %v (%v)", c != nil, got, idle, day.Cost.Total, day.Cost.Idle)
		}
		if c == nil {
			// No list prices: compute goes to cpu, tokens split by count.
			if lines[0].Kind != ChargeCPU || !approx(lines[0].Cost, day.Cost.Compute) || lines[1].Cost != 0 || lines[0].HasListPrice {
				t.Errorf("no card compute lines: %+v %+v", lines[0], lines[1])
			}
			for _, l := range lines[2:] {
				if want := day.Cost.Tokens * l.Quantity / 5000; !approx(l.Cost, want) {
					t.Errorf("no card %s/%s cost = %v, want %v", l.Model, l.TokenType, l.Cost, want)
				}
			}
		}
	}

	// A cost without any quantity still gets a line.
	lines := card.Breakdown(&usage.DayUsage{Cost: usage.Cost{Tokens: 0.3, Compute: 0.2, Total: 0.5}}, "")
	if len(lines) != 2 || lines[0].Kind != ChargeCPU || lines[1].Kind != ChargeTokens || lines[1].Cost != 0.3 {
		t.Errorf("cost-only lines = %+v", lines)
	}
	if card.Breakdown(nil, "") != nil || len(card.Breakdown(&usage.DayUsage{}, "")) != 0 {
		t.Error("empty day should have no lines")
	}
}

func TestBreakdown_RandomSumsToTotal(t *testing.T) {
	card := testCard(t)
	changed := testCard(t)
	changed.Compute.Default.GiBHour = 0
	changed.Models = map[string]ModelRates{"m-basic": {Input: 3, Output: 1}}
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 2000 {
		a := usage.Amounts{
			CPUSeconds: r.Float64() * 1e5, MemoryGiBSeconds: r.Float64() * 1e5, StorageGiBSeconds: r.Float64() * 1e7,
			Tokens: map[string]usage.TokenCounts{},
		}
		a.IdleCPUSeconds, a.IdleMemoryGiBSeconds = a.CPUSeconds*r.Float64(), a.MemoryGiBSeconds*r.Float64()
		for _, m := range []string{"m-basic", "m-full", "other"} {
			if r.IntN(2) == 0 {
				in := r.Int64N(1e7)
				a.Tokens[m] = usage.TokenCounts{Input: in, CacheRead: in / 3, Output: r.Int64N(1e6), Reasoning: r.Int64N(1e5)}
			}
		}
		day := pricedDay(card, a, []string{"", "gpu", "cheapcpu"}[i%3])
		for _, c := range []*RateCard{card, changed, nil} {
			got, idle := lineSum(c.Breakdown(day, "gpu"))
			if math.Abs(got-day.Cost.Total) > 1e-9 || math.Abs(idle-day.Cost.Idle) > 1e-9 {
				t.Fatalf("case %d: lines sum to %v (idle %v), want %v (%v)", i, got, idle, day.Cost.Total, day.Cost.Idle)
			}
		}
	}
}
