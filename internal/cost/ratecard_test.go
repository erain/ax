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
	"strings"
	"testing"
	"time"
)

func TestParse_Defaults(t *testing.T) {
	c, err := Parse([]byte("models:\n  m: { input: 1, output: 2 }\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if c.Currency != "USD" {
		t.Errorf("Currency = %q, want USD", c.Currency)
	}
	if c.Budgets.DefaultWarnAtPercent != DefaultWarnAtPercent {
		t.Errorf("DefaultWarnAtPercent = %d", c.Budgets.DefaultWarnAtPercent)
	}
	if got := c.AccountingConfig(); got.IdleCPUThreshold != 0 || got.IdleAfter != 0 {
		t.Errorf("AccountingConfig = %+v, want zero (usage defaults)", got)
	}
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name, yaml, wantErr string
	}{
		{"empty", "", "empty"},
		{"syntax", "currency: [USD\n", "parse rate card"},
		{"unknown field", "currency: USD\nmodels:\n  m: { input: 1, cache_read: 0.1 }\n", "cache_read"},
		{"currency", "currency: EUR\n", "currency \"EUR\" not supported"},
		{"negative compute", "compute: { default: { vcpuHour: -0.1 } }\n", "compute.default.vcpuHour"},
		{"negative pool", "compute: { pools: { gpu: { gibHour: -1 } } }\n", "compute.pools.gpu.gibHour"},
		{"negative storage", "storage: { gibMonth: -0.02 }\n", "storage.gibMonth"},
		{"negative input", "models:\n  m: { input: -1, output: 2 }\n", "models.m.input"},
		{"negative output", "models:\n  m: { input: 1, output: -2 }\n", "models.m.output"},
		{"negative cacheRead", "models:\n  m: { input: 1, output: 2, cacheRead: -0.1 }\n", "models.m.cacheRead"},
		{"negative cacheWrite", "models:\n  m: { input: 1, output: 2, cacheWrite: -0.1 }\n", "models.m.cacheWrite"},
		{"negative reasoning", "models:\n  m: { input: 1, output: 2, reasoning: -0.1 }\n", "models.m.reasoning"},
		{"nan price", "models:\n  m: { input: .nan, output: 2 }\n", "models.m.input"},
		{"inf price", "storage: { gibMonth: .inf }\n", "storage.gibMonth"},
		{"duplicate model", "models:\n  m: { input: 1 }\n  models/M: { input: 1 }\n", "name the same model"},
		{"negative idle threshold", "idle: { cpuThreshold: -0.1 }\n", "idle.cpuThreshold"},
		{"negative idle after", "idle: { after: -5m }\n", "idle.after"},
		{"bad idle after", "idle: { after: soon }\n", "parse rate card"},
		{"warn percent", "budgets: { defaultWarnAtPercent: 150 }\n", "defaultWarnAtPercent"},
		{"bad metric label", "metricLabels: [\"Bad Key\"]\n", "metricLabels"},
		{"duplicate metric label", "metricLabels: [agent, agent]\n", "duplicate key"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("Parse succeeded, want error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestComputeRate(t *testing.T) {
	c := testCard(t)
	tests := []struct {
		pool string
		want ComputeRate
	}{
		{"", ComputeRate{VCPUHour: 0.04, GiBHour: 0.005}},
		{"unknown", ComputeRate{VCPUHour: 0.04, GiBHour: 0.005}},
		{"gpu", ComputeRate{VCPUHour: 0.10, GiBHour: 0.01}},
		{"cheapcpu", ComputeRate{VCPUHour: 0.01, GiBHour: 0.005}},
	}
	for _, tc := range tests {
		if got := c.ComputeRate(tc.pool); got != tc.want {
			t.Errorf("ComputeRate(%q) = %+v, want %+v", tc.pool, got, tc.want)
		}
	}
}

func TestModelRates_Lookup(t *testing.T) {
	c := testCard(t)
	tests := []struct {
		model string
		want  float64 // input price; 0 = not found
	}{
		{"m-basic", 1},
		{"M-BASIC", 1},
		{"models/m-basic", 1},
		{"publishers/google/models/m-full", 2},
		{"m-full-002", 2},
		{"m-full-preview", 0},
		{"m", 0},
		{"", 0},
	}
	for _, tc := range tests {
		r, ok := c.ModelRates(tc.model)
		if got := map[bool]float64{true: r.Input}[ok]; got != tc.want {
			t.Errorf("ModelRates(%q) = %v,%v; want input %v", tc.model, r.Input, ok, tc.want)
		}
	}
}

// The shipped example must load and price the models the PoC uses.
func TestDeployRateCard(t *testing.T) {
	c, err := LoadFile("../../deploy/ratecard.yaml")
	if err != nil {
		t.Fatalf("deploy/ratecard.yaml: %v", err)
	}
	for _, m := range []string{"gemini-2.5-flash", "gemini-2.5-pro", "gemini-2.5-flash-lite"} {
		r, ok := c.ModelRates(m)
		if !ok || r.Input <= 0 || r.Output <= 0 {
			t.Errorf("model %s: rates %+v, found %v", m, r, ok)
		}
	}
	if c.Idle.After != 5*time.Minute || c.Idle.CPUThreshold != 0.02 {
		t.Errorf("idle = %+v", c.Idle)
	}
	if strings.Join(c.MetricLabels, ",") != "agent,customer" {
		t.Errorf("metricLabels = %v", c.MetricLabels)
	}
}
