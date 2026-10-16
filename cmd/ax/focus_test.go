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

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// focusRateCard is the rate card of the export tests (PoC list prices).
const focusRateCard = `currency: USD
compute: { default: { vcpuHour: 0.0445, gibHour: 0.0049 } }
storage: { gibMonth: 0.02 }
models:
  gemini-3-flash-preview: { input: 0.50, output: 3.00, cacheRead: 0.05 }
  gemini-2.5-pro: { input: 1.25, output: 10.0, cacheRead: 0.125 }
`

func mustCard(t *testing.T, yaml string) *cost.RateCard {
	t.Helper()
	c, err := cost.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// usageServer is a real ax-server (in-memory task and usage stores) serving
// gRPC on 127.0.0.1.
type usageServer struct {
	addr  string
	tasks *memory.MemoryStore
	usage usage.Store
	srv   *server.Server
}

// startUsageServer starts ax-server with usage enabled; card is the server's
// current rate card (used for charge list prices) and now its clock.
func startUsageServer(t *testing.T, card *cost.RateCard, now time.Time) *usageServer {
	t.Helper()
	pricer := cost.NewPricer(card)
	u := &usageServer{tasks: memory.NewStore(), usage: usage.NewMemoryStore()}
	u.srv = server.NewServer(u.tasks, server.Options{Usage: &server.UsageOptions{
		Store: u.usage, Pricer: pricer, RateCard: pricer.RateCard,
		Clock:  func() time.Time { return now },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = u.srv.GRPCServer().Serve(ln) }()
	t.Cleanup(u.srv.GRPCServer().Stop)
	u.addr = ln.Addr().String()
	return u
}

func (u *usageServer) addTask(t *testing.T, atespace, name, uid string, labels ...string) {
	t.Helper()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Atespace: atespace, Name: name, CreationTimestamp: timestamppb.New(goldenNow.AddDate(0, -2, 0))},
		Status:   &v1alpha1.TaskStatus{Phase: "Suspended", ActorUid: uid},
	}
	if len(labels) > 0 {
		task.Metadata.Labels = map[string]string{}
		for i := 0; i+1 < len(labels); i += 2 {
			task.Metadata.Labels[labels[i]] = labels[i+1]
		}
	}
	if err := u.tasks.SaveTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}

// addDay records amounts a on date for a task, priced with pricer at write
// time (costs are frozen, as with the ingestor and puller).
func (u *usageServer) addDay(t *testing.T, atespace, name, date string, a usage.Amounts, pricer usage.Pricer) {
	t.Helper()
	if err := u.usage.Update(context.Background(), usage.Key{Atespace: atespace, Name: name}, func(r *usage.TaskUsage) error {
		if r.Days == nil {
			r.Days = map[string]*usage.DayUsage{}
		}
		if r.Days[date] == nil {
			r.Days[date] = &usage.DayUsage{}
		}
		r.Days[date].Amounts.Add(a)
		r.Amounts.Add(a)
		r.ChargeDelta(usage.Delta{Days: map[string]*usage.DayDelta{date: {Amounts: a}}}, pricer, "")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// focusFixture populates the golden export data set.
func focusFixture(t *testing.T) *usageServer {
	t.Helper()
	card := mustCard(t, focusRateCard)
	u := startUsageServer(t, card, goldenNow)
	u.addTask(t, "default", "coder-acme", "uid-coder-acme", "agent", "coder", "customer", "acme")
	u.addTask(t, "default", "reviewer-acme", "uid-reviewer-acme", "agent", "reviewer", "customer", "acme")
	u.addTask(t, "default", "coder-old", "uid-coder-old", "agent", "coder", "customer", "globex")
	u.addTask(t, "default", "unlabeled", "")
	u.addTask(t, "team", "coder-team", "uid-coder-team", "agent", "coder")

	u.addDay(t, "default", "coder-acme", "2026-10-15", usage.Amounts{
		CPUSeconds: 1800, MemoryGiBSeconds: 7200, IdleCPUSeconds: 300, IdleMemoryGiBSeconds: 1200,
		ActiveSeconds: 1500, IdleSeconds: 360, StorageGiBSeconds: 43200,
		Tokens: map[string]usage.TokenCounts{
			"gemini-3-flash-preview": {Input: 85000, Output: 3000, CacheRead: 20000, CacheWrite: 1000, Reasoning: 1200},
			"some-unpriced-model":    {Input: 500, Output: 100},
		},
	}, card)
	u.addDay(t, "default", "coder-acme", "2026-10-16", usage.Amounts{
		CPUSeconds: 600, MemoryGiBSeconds: 2400, ActiveSeconds: 600,
		Tokens: map[string]usage.TokenCounts{"gemini-3-flash-preview": {Input: 10000, Output: 800}},
	}, card)
	u.addDay(t, "default", "reviewer-acme", "2026-10-16", usage.Amounts{
		CPUSeconds: 12.5, MemoryGiBSeconds: 20, ActiveSeconds: 80,
		Tokens: map[string]usage.TokenCounts{"gemini-2.5-pro": {Input: 9000, Output: 400, CacheRead: 3000}},
	}, card)
	u.addDay(t, "default", "coder-old", "2026-09-10", usage.Amounts{CPUSeconds: 3600, MemoryGiBSeconds: 7200, ActiveSeconds: 3600}, card)
	u.addDay(t, "default", "unlabeled", "2026-10-16", usage.Amounts{StorageGiBSeconds: 86400 * 2}, card)
	u.addDay(t, "team", "coder-team", "2026-10-16", usage.Amounts{CPUSeconds: 300, ActiveSeconds: 300}, card)
	return u
}

func TestGoldenUsage(t *testing.T) {
	u := focusFixture(t)
	prevNow := nowFunc
	nowFunc = func() time.Time { return goldenNow }
	t.Cleanup(func() { nowFunc = prevNow })

	for _, tc := range []struct {
		golden   string
		atespace string
		args     []string
	}{
		// Default range: 2026-09-17..2026-10-16 (coder-old's day is outside).
		{"cost_export_focus_csv", "default", []string{"export"}},
		{"cost_export_json", "default", []string{"export", "--format", "json", "-l", "agent=coder", "--start=2026-09-01"}},
		{"cost_summary_usage", "default", []string{"summary"}},
		{"cost_summary_usage_range", "default", []string{"summary", "--by", "customer", "--start", "2026-09-01", "--end", "2026-10-15"}},
		{"cost_summary_usage_json", "team", []string{"summary", "-o", "json"}},
	} {
		t.Run(tc.golden, func(t *testing.T) {
			var buf bytes.Buffer
			if err := runCost(u.addr, tc.atespace, tc.args, &buf); err != nil {
				t.Fatalf("command failed: %v\noutput:\n%s", err, buf.String())
			}
			checkGolden(t, tc.golden, buf.String())
		})
	}

	// -o writes the same bytes to a file.
	var stdout bytes.Buffer
	if err := runCost(u.addr, "default", []string{"export"}, &stdout); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "focus.csv")
	var none bytes.Buffer
	if err := runCost(u.addr, "default", []string{"export", "-o", path}, &none); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != stdout.String() || none.Len() != 0 {
		t.Errorf("export -o wrote %q (err %v), stdout %q; want the stdout export", data, err, none.String())
	}
}

// parseFocusCSV parses an export and returns its rows as column→value maps.
func parseFocusCSV(t *testing.T, data string) []map[string]string {
	t.Helper()
	recs, err := csv.NewReader(strings.NewReader(data)).ReadAll()
	if err != nil {
		t.Fatalf("invalid CSV: %v", err)
	}
	if len(recs) == 0 || strings.Join(recs[0], ",") != strings.Join(focusColumns, ",") {
		t.Fatalf("bad header: %v", recs)
	}
	var rows []map[string]string
	for _, r := range recs[1:] {
		m := map[string]string{}
		for i, c := range focusColumns {
			m[c] = r[i]
		}
		rows = append(rows, m)
	}
	return rows
}

func sumColumn(t *testing.T, rows []map[string]string, col string) float64 {
	t.Helper()
	var s float64
	for _, r := range rows {
		v, err := strconv.ParseFloat(r[col], 64)
		if err != nil {
			t.Fatalf("%s %q: %v", col, r[col], err)
		}
		s += v
	}
	return s
}

// Acceptance 2 (T16): the BilledCost of an export sums to the tasks' cost
// totals for the period (within 1e-9), also when the server's rate card
// changed after the usage was priced (frozen costs are split, not re-priced).
func TestCostExportBilledCostMatchesTaskTotals(t *testing.T) {
	priced := mustCard(t, focusRateCard)
	current := mustCard(t, strings.ReplaceAll(strings.ReplaceAll(focusRateCard, "0.0445", "0.09"), "output: 3.00", "output: 2.00"))
	u := startUsageServer(t, current, goldenNow)
	prevNow := nowFunc
	nowFunc = func() time.Time { return goldenNow }
	t.Cleanup(func() { nowFunc = prevNow })

	r := rand.New(rand.NewPCG(7, 11))
	models := []string{"gemini-3-flash-preview", "gemini-2.5-pro", "unknown-model"}
	periodTotal := map[string]float64{} // atespace → Σ day cost.total in 2026-10-01..2026-10-16
	for i := range 12 {
		atespace := []string{"default", "team"}[i%2]
		name := fmt.Sprintf("task-%02d", i)
		u.addTask(t, atespace, name, "uid-"+name, "agent", []string{"coder", "reviewer", "triage"}[i%3], "customer", "c"+strconv.Itoa(i%4))
		for d := range 75 { // 2026-08-03 .. 2026-10-16
			if r.IntN(3) == 0 {
				continue
			}
			date := goldenNow.AddDate(0, 0, -d).Format(time.DateOnly)
			a := usage.Amounts{
				CPUSeconds: r.Float64() * 5000, MemoryGiBSeconds: r.Float64() * 20000, StorageGiBSeconds: r.Float64() * 1e6,
				ActiveSeconds: r.Float64() * 3600, Tokens: map[string]usage.TokenCounts{},
			}
			a.IdleCPUSeconds, a.IdleMemoryGiBSeconds = a.CPUSeconds*r.Float64()/2, a.MemoryGiBSeconds*r.Float64()/2
			for _, m := range models {
				if r.IntN(2) == 0 {
					in := r.Int64N(500000)
					out := r.Int64N(50000)
					a.Tokens[m] = usage.TokenCounts{Input: in, Output: out, CacheRead: in / 4, Reasoning: out / 3}
				}
			}
			u.addDay(t, atespace, name, date, a, priced)
			if date >= "2026-10-01" {
				periodTotal[atespace] += priced.Price(usage.Delta{Days: map[string]*usage.DayDelta{date: {Amounts: a}}}, "").Total
			}
		}
	}

	for _, atespace := range []string{"default", "team"} {
		// Whole lifetime: Σ BilledCost = Σ Task.status.usage.cost.total.
		var lifetime float64
		resp, err := u.srv.ListTasks(context.Background(), &v1alpha1.ListTasksRequest{Atespace: atespace, Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range resp.GetTasks() {
			lifetime += task.GetStatus().GetUsage().GetCost().GetTotal()
		}
		var buf bytes.Buffer
		if err := runCost(u.addr, atespace, []string{"export", "--start", "2026-01-01"}, &buf); err != nil {
			t.Fatal(err)
		}
		rows := parseFocusCSV(t, buf.String())
		if got := sumColumn(t, rows, "BilledCost"); math.Abs(got-lifetime) > 1e-9 || lifetime == 0 {
			t.Errorf("%s: Σ BilledCost = %.12f, Σ task cost totals = %.12f", atespace, got, lifetime)
		}
		for _, col := range []string{"EffectiveCost", "ListCost", "ContractedCost"} {
			if got := sumColumn(t, rows, col); math.Abs(got-lifetime) > 1e-9 {
				t.Errorf("%s: Σ %s = %v, want %v", atespace, col, got, lifetime)
			}
		}

		// A period: Σ BilledCost = Σ day cost totals in the period, and the
		// summary over the same period agrees.
		buf.Reset()
		if err := runCost(u.addr, atespace, []string{"export", "--format", "json", "--start", "2026-10-01"}, &buf); err != nil {
			t.Fatal(err)
		}
		var jrows []focusRow
		if err := json.Unmarshal(buf.Bytes(), &jrows); err != nil {
			t.Fatal(err)
		}
		var got float64
		for _, row := range jrows {
			got += row.BilledCost
			if row.ChargePeriodStart < "2026-10-01" {
				t.Errorf("row outside the period: %+v", row)
			}
		}
		if math.Abs(got-periodTotal[atespace]) > 1e-9 {
			t.Errorf("%s period: Σ BilledCost = %.12f, want %.12f", atespace, got, periodTotal[atespace])
		}
		buf.Reset()
		if err := runCost(u.addr, atespace, []string{"summary", "-o", "json", "--start", "2026-10-01"}, &buf); err != nil {
			t.Fatal(err)
		}
		var s costSummary
		if err := json.Unmarshal(buf.Bytes(), &s); err != nil {
			t.Fatal(err)
		}
		if math.Abs(s.Total.Cost.Total-periodTotal[atespace]) > 1e-6 || s.Source != "usage" || s.Start != "2026-10-01" || s.End != "2026-10-16" {
			t.Errorf("%s summary = %+v (total %v), want total %v", atespace, s, s.Total.Cost.Total, periodTotal[atespace])
		}
	}
}

// fakeUsageServer serves canned DailyUsage (and nothing else).
type fakeUsageServer struct {
	v1alpha1.UnimplementedAXServer
	days []*v1alpha1.DailyUsage
	err  error
}

func (f *fakeUsageServer) ListUsage(context.Context, *v1alpha1.ListUsageRequest) (*v1alpha1.ListUsageResponse, error) {
	return &v1alpha1.ListUsageResponse{Days: f.days}, f.err
}

func startFake(t *testing.T, impl v1alpha1.AXServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	v1alpha1.RegisterAXServer(srv, impl)
	t.Cleanup(srv.Stop)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String()
}

// Days without server-side charges still export with BilledCost summing to
// cost.total (one compute row, tokens per model, storage).
func TestCostExportFallbackCharges(t *testing.T) {
	days := []*v1alpha1.DailyUsage{
		{Atespace: "default", Task: "t1", Date: "2026-10-16", CpuSeconds: 7200, StorageGibSeconds: 2592000,
			Labels: map[string]string{"agent": "a"},
			Tokens: []*v1alpha1.ModelTokenUsage{{Model: "gemini-x", Input: 300, Output: 100}, {Model: "other", Input: 100}},
			Cost:   &v1alpha1.CostBreakdown{Currency: "USD", Compute: 0.1, Idle: 0.01, Tokens: 0.3, Storage: 0.02, Total: 0.42}},
		{Atespace: "default", Task: "t2", Date: "2026-10-31", Cost: &v1alpha1.CostBreakdown{Tokens: 0.5, Total: 0.5}},
	}
	addr := startFake(t, &fakeUsageServer{days: days})
	var buf bytes.Buffer
	if err := runCost(addr, "default", []string{"export", "--end", "2026-10-31"}, &buf); err != nil {
		t.Fatal(err)
	}
	rows := parseFocusCSV(t, buf.String())
	if got := sumColumn(t, rows, "BilledCost"); math.Abs(got-0.92) > 1e-12 {
		t.Errorf("Σ BilledCost = %v, want 0.92", got)
	}
	want := []string{
		"compute 2 vCPU-Hours 0.1 Sandbox compute",
		"tokens 400 Tokens 0.24 gemini-x tokens",
		"tokens 100 Tokens 0.06 other tokens",
		"storage 1 GiB-Months 0.02 Workspace storage",
		"tokens 0 Tokens 0.5 model tokens",
	}
	if len(rows) != len(want) {
		t.Fatalf("rows = %v", rows)
	}
	for i, r := range rows {
		got := strings.Join([]string{r["x_ChargeKind"], r["ConsumedQuantity"], r["ConsumedUnit"], r["BilledCost"], r["ChargeDescription"]}, " ")
		if got != want[i] && !(i == 2 && strings.HasPrefix(got, "tokens 100 Tokens 0.0599999")) {
			t.Errorf("row %d = %q, want %q", i, got, want[i])
		}
		if r["ListUnitPrice"] != "" {
			t.Errorf("row %d has a list price without a rate card: %q", i, r["ListUnitPrice"])
		}
	}
	if r := rows[4]; r["BillingPeriodStart"] != "2026-10-01T00:00:00Z" || r["BillingPeriodEnd"] != "2026-11-01T00:00:00Z" ||
		r["ChargePeriodStart"] != "2026-10-31T00:00:00Z" || r["ChargePeriodEnd"] != "2026-11-01T00:00:00Z" ||
		r["BillingCurrency"] != "USD" || r["Tags"] != "{}" {
		t.Errorf("period/currency/tags of t2 = %v", r)
	}
}

// Without ListUsage, export fails and summary falls back to ListTasks.
func TestCostListUsageUnavailable(t *testing.T) {
	for _, err := range []error{status.Error(codes.Unimplemented, "old server"), status.Error(codes.FailedPrecondition, "usage off")} {
		addr := startFake(t, &fakeUsageServer{err: err})
		var buf bytes.Buffer
		if e := runCost(addr, "default", []string{"export"}, &buf); e == nil || !strings.Contains(e.Error(), "ListUsage") {
			t.Errorf("export with %v = %v, want a ListUsage error", err, e)
		}
		// Summary falls back to ListTasks (Unimplemented on this fake).
		if e := runCost(addr, "default", []string{"summary"}, &buf); status.Code(errorsCause(e)) != codes.Unimplemented ||
			!strings.Contains(e.Error(), "listing tasks") {
			t.Errorf("summary with %v = %v, want the ListTasks fallback", err, e)
		}
	}
	// Other errors are returned as is.
	addr := startFake(t, &fakeUsageServer{err: status.Error(codes.Internal, "boom")})
	var buf bytes.Buffer
	if e := runCost(addr, "default", []string{"summary"}, &buf); e == nil || !strings.Contains(e.Error(), "listing usage") {
		t.Errorf("summary with Internal = %v", e)
	}
}

// errorsCause unwraps to the innermost error (for status.Code).
func errorsCause(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok || u.Unwrap() == nil {
			return err
		}
		err = u.Unwrap()
	}
}

func TestParseCostExportArgs(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		want    costExportOptions
		wantErr string
	}{
		{args: nil, want: costExportOptions{format: "focus-csv"}},
		{args: []string{"--format", "json", "-l", "agent=x", "--start", "2026-10-01", "--end=2026-10-16", "-o", "out.json"},
			want: costExportOptions{format: "json", selector: "agent=x", start: "2026-10-01", end: "2026-10-16", file: "out.json"}},
		{args: []string{"--format=csv"}, wantErr: "unsupported export format"},
		{args: []string{"--start", "2026-13-01"}, wantErr: "invalid --start"},
		{args: []string{"--end", "16.10.2026"}, wantErr: "invalid --end"},
		{args: []string{"--start", "2026-10-02", "--end", "2026-10-01"}, wantErr: "is after"},
		{args: []string{"-l", "a!=b"}, wantErr: "selector"},
		{args: []string{"--by", "agent"}, wantErr: "unexpected argument"},
	} {
		got, err := parseCostExportArgs(tc.args)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("parseCostExportArgs(%q) error = %v, want %q", tc.args, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseCostExportArgs(%q) = %+v, %v; want %+v", tc.args, got, err, tc.want)
		}
	}
	if o, err := parseCostSummaryArgs([]string{"--start", "2026-10-01", "--end", "2026-10-02"}); err != nil || o.start != "2026-10-01" || o.end != "2026-10-02" {
		t.Errorf("summary dates = %+v, %v", o, err)
	}
	if _, err := parseCostSummaryArgs([]string{"--end", "x"}); err == nil {
		t.Error("summary accepted a bad --end")
	}
	for _, tc := range []struct{ start, end, wantStart, wantEnd string }{
		{"", "", "2026-09-17", "2026-10-16"},
		{"2026-10-10", "", "2026-10-10", "2026-10-16"},
		{"", "2026-03-01", "2026-01-31", "2026-03-01"},
	} {
		if s, e := resolveDateRange(tc.start, tc.end, goldenNow); s != tc.wantStart || e != tc.wantEnd {
			t.Errorf("resolveDateRange(%q, %q) = %s, %s; want %s, %s", tc.start, tc.end, s, e, tc.wantStart, tc.wantEnd)
		}
	}
}
