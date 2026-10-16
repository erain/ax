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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/usage"
)

const day1, day2 = "2026-10-15", "2026-10-16"

func testCard(t *testing.T) *RateCard {
	t.Helper()
	c, err := LoadFile("testdata/ratecard.yaml")
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	return c
}

func oneDay(dd usage.DayDelta) usage.Delta {
	return usage.Delta{Days: map[string]*usage.DayDelta{day1: &dd}}
}

func approx(a, b float64) bool { return math.Abs(a-b) <= 1e-12*math.Max(1, math.Abs(b)) }

func checkCost(t *testing.T, got, want usage.Cost) {
	t.Helper()
	if got.Currency != want.Currency || got.TokensPartiallyUnpriced != want.TokensPartiallyUnpriced ||
		!approx(got.Total, want.Total) || !approx(got.Compute, want.Compute) || !approx(got.Tokens, want.Tokens) ||
		!approx(got.Storage, want.Storage) || !approx(got.Idle, want.Idle) {
		t.Errorf("cost mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

// Counts used by several token cases (semconv: input includes cache read +
// write, output includes reasoning).
var mixed = usage.TokenCounts{Input: 1_000_000, CacheRead: 200_000, CacheWrite: 100_000, Output: 500_000, Reasoning: 100_000}

func TestPrice(t *testing.T) {
	card := testCard(t)
	tests := []struct {
		name string
		pool string
		dd   usage.DayDelta
		want usage.Cost
	}{
		{
			name: "empty",
			want: usage.Cost{Currency: "USD"},
		},
		{
			// cpu 7200s = 2 vCPU-h × 0.04 = 0.08; mem 3600 GiB·s = 1 GiB-h × 0.005 = 0.005 → 0.085
			// idle cpu 1800s = 0.5h × 0.04 = 0.02; idle mem 720 GiB·s = 0.2 GiB-h × 0.005 = 0.001 → 0.021
			name: "compute default with idle subset",
			dd: usage.DayDelta{Amounts: usage.Amounts{
				ActiveSeconds: 9999, IdleSeconds: 99, // not priced
				CPUSeconds: 7200, MemoryGiBSeconds: 3600, IdleCPUSeconds: 1800, IdleMemoryGiBSeconds: 720,
			}},
			want: usage.Cost{Currency: "USD", Compute: 0.085, Idle: 0.021, Total: 0.085},
		},
		{
			// unknown pool → default rates: same as above without idle
			name: "unknown pool uses default",
			pool: "nope",
			dd:   usage.DayDelta{Amounts: usage.Amounts{CPUSeconds: 7200, MemoryGiBSeconds: 3600}},
			want: usage.Cost{Currency: "USD", Compute: 0.085, Total: 0.085},
		},
		{
			// gpu: 2 vCPU-h × 0.10 = 0.2; 1 GiB-h × 0.01 = 0.01 → 0.21; idle 0.5h × 0.10 + 0.2 × 0.01 = 0.052
			name: "pool override",
			pool: "gpu",
			dd: usage.DayDelta{Amounts: usage.Amounts{
				CPUSeconds: 7200, MemoryGiBSeconds: 3600, IdleCPUSeconds: 1800, IdleMemoryGiBSeconds: 720,
			}},
			want: usage.Cost{Currency: "USD", Compute: 0.21, Idle: 0.052, Total: 0.21},
		},
		{
			// cheapcpu: 2 vCPU-h × 0.01 = 0.02; gibHour falls back to default: 1 × 0.005 → 0.025
			name: "partial pool override",
			pool: "cheapcpu",
			dd:   usage.DayDelta{Amounts: usage.Amounts{CPUSeconds: 7200, MemoryGiBSeconds: 3600}},
			want: usage.Cost{Currency: "USD", Compute: 0.025, Total: 0.025},
		},
		{
			// 2 GiB for 30 days = 2 × 2,592,000 = 5,184,000 GiB·s = 2 GiB-month × 0.03 = 0.06
			name: "storage",
			dd:   usage.DayDelta{Amounts: usage.Amounts{StorageGiBSeconds: 5_184_000}},
			want: usage.Cost{Currency: "USD", Storage: 0.06, Total: 0.06},
		},
		{
			// m-basic (input 1, output 4; cache → input, reasoning → output):
			// uncached (1M−0.2M−0.1M)=0.7M × 1 = 0.7; cacheRead 0.2M × 1 = 0.2; cacheWrite 0.1M × 1 = 0.1
			// output: (0.5M−0.1M)=0.4M × 4 = 1.6; reasoning 0.1M × 4 = 0.4 → 3.0 (= 1M×1 + 0.5M×4)
			name: "tokens with fallbacks",
			dd:   usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{"m-basic": mixed}}},
			want: usage.Cost{Currency: "USD", Tokens: 3.0, Total: 3.0},
		},
		{
			// m-full (input 2, output 8, cacheRead 0.5, cacheWrite 2.5, reasoning 10):
			// uncached 0.7M × 2 = 1.4; cacheRead 0.2M × 0.5 = 0.1; cacheWrite 0.1M × 2.5 = 0.25
			// visible output 0.4M × 8 = 3.2; reasoning 0.1M × 10 = 1.0 → 5.95
			name: "tokens with all rates",
			dd:   usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{"m-full": mixed}}},
			want: usage.Cost{Currency: "USD", Tokens: 5.95, Total: 5.95},
		},
		{
			// semconv: reasoning is inside output, never added on top:
			// output 0.5M (all reasoning) × 4 = 2.0 (not 4.0); input 0.25M × 1 = 0.25 → 2.25
			name: "reasoning not added on top of output",
			dd: usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{
				"m-basic": {Input: 250_000, Output: 500_000, Reasoning: 500_000},
			}}},
			want: usage.Cost{Currency: "USD", Tokens: 2.25, Total: 2.25},
		},
		{
			// cache read is subtracted from input: m-full input 1M of which 1M cached:
			// uncached 0 × 2 = 0; cacheRead 1M × 0.5 = 0.5 → 0.5 (not 2.5)
			name: "fully cached input",
			dd: usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{
				"m-full": {Input: 1_000_000, CacheRead: 1_000_000},
			}}},
			want: usage.Cost{Currency: "USD", Tokens: 0.5, Total: 0.5},
		},
		{
			// malformed (cache > input): uncached clamps at 0; cacheRead 2M × 0.5 = 1.0;
			// reasoning > output clamps to output: 1M × 10 = 10 → 11.0
			name: "clamping of inconsistent counts",
			dd: usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{
				"m-full": {Input: 1_000_000, CacheRead: 2_000_000, Output: 1_000_000, Reasoning: 3_000_000},
			}}},
			want: usage.Cost{Currency: "USD", Tokens: 11.0, Total: 11.0},
		},
		{
			// provider cost replaces the rate card for that model: 0.42; m-full still from card: 5.95 → 6.37
			name: "provider cost",
			dd: usage.DayDelta{
				Amounts:      usage.Amounts{Tokens: map[string]usage.TokenCounts{"m-basic": mixed, "m-full": mixed}},
				ProviderCost: map[string]usage.Money{"m-basic": {Currency: "USD", Amount: 0.42}},
			},
			want: usage.Cost{Currency: "USD", Tokens: 6.37, Total: 6.37},
		},
		{
			// provider cost for a model not on the card (empty currency = card currency): priced, not flagged
			name: "provider cost for unknown model",
			dd: usage.DayDelta{
				Amounts:      usage.Amounts{Tokens: map[string]usage.TokenCounts{"vendor-x": mixed}},
				ProviderCost: map[string]usage.Money{"vendor-x": {Amount: 0.42}},
			},
			want: usage.Cost{Currency: "USD", Tokens: 0.42, Total: 0.42},
		},
		{
			// provider cost in another currency is ignored → rate card: 3.0
			name: "provider cost in other currency falls back to card",
			dd: usage.DayDelta{
				Amounts:      usage.Amounts{Tokens: map[string]usage.TokenCounts{"m-basic": mixed}},
				ProviderCost: map[string]usage.Money{"m-basic": {Currency: "EUR", Amount: 0.42}},
			},
			want: usage.Cost{Currency: "USD", Tokens: 3.0, Total: 3.0},
		},
		{
			// unknown model without provider cost: not priced, flagged; m-basic: 3.0
			name: "unknown model",
			dd: usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{
				"m-basic": mixed, "mystery-model": mixed,
			}}},
			want: usage.Cost{Currency: "USD", Tokens: 3.0, Total: 3.0, TokensPartiallyUnpriced: true},
		},
		{
			// path prefix, case and numeric version suffix are normalized: "models/M-Basic-001" → m-basic: 3.0
			name: "model name normalization",
			dd: usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{
				"models/M-Basic-001": mixed,
			}}},
			want: usage.Cost{Currency: "USD", Tokens: 3.0, Total: 3.0},
		},
		{
			// everything: compute 0.085 + tokens 3.0 + storage 0.06 = 3.145; idle 0.021
			name: "total",
			dd: usage.DayDelta{Amounts: usage.Amounts{
				CPUSeconds: 7200, MemoryGiBSeconds: 3600, IdleCPUSeconds: 1800, IdleMemoryGiBSeconds: 720,
				StorageGiBSeconds: 5_184_000, Tokens: map[string]usage.TokenCounts{"m-basic": mixed},
			}},
			want: usage.Cost{Currency: "USD", Compute: 0.085, Idle: 0.021, Storage: 0.06, Tokens: 3.0, Total: 3.145},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checkCost(t, card.Price(oneDay(tc.dd), tc.pool), tc.want)
			// The Pricer wrapper must agree with the rate card.
			checkCost(t, NewPricer(card).Price(oneDay(tc.dd), tc.pool), tc.want)
		})
	}
}

func TestPrice_MultiDayIsSumOfDays(t *testing.T) {
	card := testCard(t)
	a := usage.DayDelta{Amounts: usage.Amounts{
		CPUSeconds: 3600, MemoryGiBSeconds: 1800, IdleCPUSeconds: 600, StorageGiBSeconds: 86400,
		Tokens: map[string]usage.TokenCounts{"m-full": mixed},
	}}
	b := usage.DayDelta{Amounts: usage.Amounts{
		CPUSeconds: 1200, MemoryGiBSeconds: 7200, IdleMemoryGiBSeconds: 3600, StorageGiBSeconds: 43200,
		Tokens: map[string]usage.TokenCounts{"m-full": {Input: 10_000, Output: 2_000}, "m-basic": mixed},
	}}
	d := usage.Delta{Days: map[string]*usage.DayDelta{day1: &a, day2: &b}}

	var want usage.Cost
	want.Add(card.Price(usage.Delta{Days: map[string]*usage.DayDelta{day1: &a}}, "gpu"))
	want.Add(card.Price(usage.Delta{Days: map[string]*usage.DayDelta{day2: &b}}, "gpu"))
	got := card.Price(d, "gpu")
	checkCost(t, got, want)
	// Linear: pricing the summed day equals pricing the days separately.
	checkCost(t, card.Price(oneDay(d.Sum()), "gpu"), want)

	// ChargeDelta (one Price call per day) yields the same total and per-day costs.
	u := usage.NewTaskUsage()
	checkCost(t, u.ChargeDelta(d, NewPricer(card), "gpu"), want)
	checkCost(t, u.Cost, want)
	checkCost(t, u.Day(day1).Cost, card.Price(usage.Delta{Days: map[string]*usage.DayDelta{day1: &a}}, "gpu"))
}

func TestPrice_ProviderCostMixedAcrossDays(t *testing.T) {
	// Day 1 has a provider cost for m-basic, day 2 only token counts: each day
	// is priced on its own (0.42 + 3.0), so day 2's tokens are not lost.
	card := testCard(t)
	d := usage.Delta{Days: map[string]*usage.DayDelta{
		day1: {Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{"m-basic": mixed}},
			ProviderCost: map[string]usage.Money{"m-basic": {Currency: "USD", Amount: 0.42}}},
		day2: {Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{"m-basic": mixed}}},
	}}
	checkCost(t, card.Price(d, ""), usage.Cost{Currency: "USD", Tokens: 3.42, Total: 3.42})
}

func writeFile(t *testing.T, path, content string, mod time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// Force a distinct mtime regardless of filesystem timestamp resolution.
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
}

func cardYAML(inputPrice string) string {
	return "currency: USD\nmodels:\n  m: { input: " + inputPrice + ", output: 1 }\n"
}

func TestFilePricer_Reload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ratecard.yaml")
	base := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	writeFile(t, path, cardYAML("1"), base)

	p, err := NewFilePricer(path)
	if err != nil {
		t.Fatalf("NewFilePricer: %v", err)
	}
	clock := base
	p.now = func() time.Time { return clock }
	p.lastCheck = clock

	// 1M input tokens of model m cost exactly the input price.
	d := oneDay(usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{"m": {Input: 1_000_000}}}})
	price := func() float64 { return p.Price(d, "").Tokens }
	expect := func(step string, want float64) {
		t.Helper()
		if got := price(); !approx(got, want) {
			t.Fatalf("%s: tokens cost = %v, want %v", step, got, want)
		}
	}
	expect("initial", 1)

	writeFile(t, path, cardYAML("2"), base.Add(time.Minute))
	expect("before reload interval", 1)
	clock = clock.Add(ReloadInterval)
	expect("after reload interval", 2)

	// Invalid YAML keeps the previous card.
	writeFile(t, path, "currency: [USD\n", base.Add(2*time.Minute))
	clock = clock.Add(ReloadInterval)
	expect("invalid yaml", 2)

	// Valid YAML but an invalid card (negative price) also keeps it.
	writeFile(t, path, cardYAML("-3"), base.Add(3*time.Minute))
	clock = clock.Add(ReloadInterval)
	expect("negative price", 2)

	// Fixing the file reloads.
	writeFile(t, path, cardYAML("4"), base.Add(4*time.Minute))
	clock = clock.Add(ReloadInterval)
	expect("fixed", 4)

	// A deleted file keeps the card.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(ReloadInterval)
	expect("deleted", 4)
}

func TestFilePricer_InitialLoadMustSucceed(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewFilePricer(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Error("missing file: want error")
	}
	bad := filepath.Join(dir, "bad.yaml")
	writeFile(t, bad, "currency: EUR\n", time.Now())
	if _, err := NewFilePricer(bad); err == nil {
		t.Error("invalid card: want error")
	}
}

func TestFilePricer_Concurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ratecard.yaml")
	writeFile(t, path, cardYAML("1"), time.Now())
	p, err := NewFilePricer(path)
	if err != nil {
		t.Fatal(err)
	}
	p.lastCheck = time.Time{} // force file checks
	d := oneDay(usage.DayDelta{Amounts: usage.Amounts{Tokens: map[string]usage.TokenCounts{"m": {Input: 1_000_000}}}})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if c := p.Price(d, ""); c.Tokens != 1 {
					t.Errorf("tokens = %v", c.Tokens)
					return
				}
				_ = p.RateCard().MetricLabels
			}
		}()
	}
	wg.Wait()
}

func TestRateCardAccessor(t *testing.T) {
	p, err := NewFilePricer("testdata/ratecard.yaml")
	if err != nil {
		t.Fatal(err)
	}
	c := p.RateCard()
	if got := c.AccountingConfig(); got.IdleCPUThreshold != 0.05 || got.IdleAfter != 2*time.Minute {
		t.Errorf("AccountingConfig = %+v", got)
	}
	if c.Budgets.DefaultWarnAtPercent != 75 {
		t.Errorf("DefaultWarnAtPercent = %d", c.Budgets.DefaultWarnAtPercent)
	}
	if strings.Join(c.MetricLabels, ",") != "agent,customer" {
		t.Errorf("MetricLabels = %v", c.MetricLabels)
	}
}
