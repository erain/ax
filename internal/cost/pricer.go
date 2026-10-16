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
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/ax/internal/usage"
)

// ReloadInterval is the minimum time between rate card file checks.
const ReloadInterval = 10 * time.Second

var (
	_ usage.Pricer = (*Pricer)(nil)
	_ usage.Pricer = (*RateCard)(nil)
)

// Price prices d with the rate card. It is linear: a multi-day Delta is
// priced day by day and the day costs are summed, so Price(a+b) equals
// Price(a)+Price(b) and splitting a Delta at UTC midnight does not change
// the total. Per day:
//
//	compute = cpuSeconds/3600×vcpuHour + memoryGiBSeconds/3600×gibHour  (pool rate)
//	idle    = idleCPUSeconds/3600×vcpuHour + idleMemoryGiBSeconds/3600×gibHour  (subset of compute)
//	storage = storageGiBSeconds/(30×86400)×gibMonth
//	tokens  = per model: the provider-reported cost if present, else the
//	          rate card price (see tokenCost); a model with tokens but neither
//	          a provider cost nor rates sets TokensPartiallyUnpriced.
//	total   = compute + tokens + storage
func (c *RateCard) Price(d usage.Delta, pool string) usage.Cost {
	out := usage.Cost{Currency: c.Currency}
	for _, date := range d.Dates() {
		if day := d.Days[date]; day != nil {
			out.Add(c.priceDay(day, pool))
		}
	}
	return out
}

func (c *RateCard) priceDay(d *usage.DayDelta, pool string) usage.Cost {
	r := c.ComputeRate(pool)
	out := usage.Cost{Currency: c.Currency}
	out.Compute = d.CPUSeconds/3600*r.VCPUHour + d.MemoryGiBSeconds/3600*r.GiBHour
	out.Idle = d.IdleCPUSeconds/3600*r.VCPUHour + d.IdleMemoryGiBSeconds/3600*r.GiBHour
	out.Storage = d.StorageGiBSeconds / storageMonthSeconds * c.Storage.GiBMonth

	// Sorted iteration keeps float sums deterministic.
	for _, model := range sortedKeys(d.ProviderCost) {
		pc := d.ProviderCost[model]
		switch {
		case c.usableProviderCost(pc):
			out.Tokens += pc.Amount
		case pc.Amount != 0 && d.Tokens[model].IsZero():
			// A cost in another currency with no token counts to fall
			// back to cannot be priced.
			out.TokensPartiallyUnpriced = true
		}
	}
	for _, model := range sortedKeys(d.Tokens) {
		t := d.Tokens[model]
		if t.IsZero() {
			continue
		}
		if pc, ok := d.ProviderCost[model]; ok && c.usableProviderCost(pc) {
			continue // already counted above
		}
		rates, ok := c.ModelRates(model)
		if !ok {
			out.TokensPartiallyUnpriced = true
			continue
		}
		out.Tokens += tokenCost(t, rates)
	}
	out.Total = out.Compute + out.Tokens + out.Storage
	return out
}

// usableProviderCost reports whether a provider-reported cost can be used
// instead of rate card prices: non-zero and in the rate card currency (an
// empty currency is taken to be the rate card currency).
func (c *RateCard) usableProviderCost(m usage.Money) bool {
	return m.Amount != 0 && (m.Currency == "" || m.Currency == c.Currency)
}

// tokenCost prices token counts reported per the OTel GenAI semantic
// conventions: Input already includes CacheRead and CacheWrite, and Output
// already includes Reasoning. So:
//
//	uncached input = max(0, input − cacheRead − cacheWrite) × input rate
//	               + cacheRead × cacheRead rate (fallback input)
//	               + cacheWrite × cacheWrite rate (fallback input)
//	output         = (output − reasoning) × output rate
//	               + reasoning × reasoning rate (fallback output)
//
// with reasoning clamped to [0, output]. Reasoning is never added on top of
// output; with no separate reasoning rate the output term is output × output
// rate. All rates are per 1M tokens.
func tokenCost(t usage.TokenCounts, r ModelRates) float64 {
	cacheRead := opt(r.CacheRead, r.Input)
	cacheWrite := opt(r.CacheWrite, r.Input)
	reasoningRate := opt(r.Reasoning, r.Output)

	uncached := max(0, t.Input-t.CacheRead-t.CacheWrite)
	reasoning := min(max(0, t.Reasoning), max(0, t.Output))
	visible := t.Output - reasoning

	sum := float64(uncached)*r.Input +
		float64(t.CacheRead)*cacheRead +
		float64(t.CacheWrite)*cacheWrite +
		float64(visible)*r.Output +
		float64(reasoning)*reasoningRate
	return sum / tokensPerPrice
}

func opt(v *float64, fallback float64) float64 {
	if v != nil {
		return *v
	}
	return fallback
}

// Pricer is a usage.Pricer backed by a rate card. A Pricer created with
// NewFilePricer reloads its file when the file's modification time or size
// changes, checking at most once per ReloadInterval (on Price/RateCard
// calls). An invalid file keeps the previous rate card and logs an error.
type Pricer struct {
	card atomic.Pointer[RateCard]

	path string
	now  func() time.Time

	mu        sync.Mutex // guards the fields below
	lastCheck time.Time
	modTime   time.Time
	size      int64
	badMod    time.Time // modTime of the last file that failed to load
	badSize   int64
}

// NewPricer returns a Pricer with a fixed rate card.
func NewPricer(c *RateCard) *Pricer {
	p := &Pricer{now: time.Now}
	p.card.Store(c)
	return p
}

// NewFilePricer loads the rate card at path and returns a Pricer that
// reloads it when it changes. The initial load must succeed.
func NewFilePricer(path string) (*Pricer, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	c, err := LoadFile(path)
	if err != nil {
		return nil, err
	}
	p := &Pricer{path: path, now: time.Now, modTime: fi.ModTime(), size: fi.Size()}
	p.lastCheck = p.now()
	p.card.Store(c)
	return p, nil
}

// Price prices d with the current rate card (see RateCard.Price).
func (p *Pricer) Price(d usage.Delta, pool string) usage.Cost {
	return p.RateCard().Price(d, pool)
}

// RateCard returns the current rate card (for idle thresholds, budget
// defaults and metric labels). The returned value must not be modified.
func (p *Pricer) RateCard() *RateCard {
	p.maybeReload()
	return p.card.Load()
}

func (p *Pricer) maybeReload() {
	if p.path == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if now.Sub(p.lastCheck) < ReloadInterval {
		return
	}
	p.lastCheck = now
	fi, err := os.Stat(p.path)
	if err != nil {
		slog.Error("rate card stat failed; keeping previous rate card", "path", p.path, "error", err)
		return
	}
	mod, size := fi.ModTime(), fi.Size()
	if mod.Equal(p.modTime) && size == p.size {
		return
	}
	if mod.Equal(p.badMod) && size == p.badSize {
		return // already reported this broken version
	}
	c, err := LoadFile(p.path)
	if err != nil {
		p.badMod, p.badSize = mod, size
		slog.Error("rate card reload failed; keeping previous rate card", "path", p.path, "error", err)
		return
	}
	p.modTime, p.size = mod, size
	p.badMod, p.badSize = time.Time{}, 0
	p.card.Store(c)
	slog.Info("rate card reloaded", "path", p.path, "models", len(c.Models))
}
