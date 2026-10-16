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

package server_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// addUsageDay adds amounts a on date to the usage record of key and prices
// them with the test rate card, as the ingestor and puller do.
func addUsageDay(t *testing.T, us usage.Store, key usage.Key, date string, a usage.Amounts, uid string) {
	t.Helper()
	card, err := cost.Parse([]byte(testRateCard))
	if err != nil {
		t.Fatal(err)
	}
	if err := us.Update(context.Background(), key, func(u *usage.TaskUsage) error {
		if uid != "" {
			u.State.ActorUID = uid
		}
		if u.Days == nil {
			u.Days = map[string]*usage.DayUsage{}
		}
		if u.Days[date] == nil {
			u.Days[date] = &usage.DayUsage{}
		}
		u.Days[date].Amounts.Add(a)
		u.Amounts.Add(a)
		u.ChargeDelta(usage.Delta{Days: map[string]*usage.DayDelta{date: {Amounts: a}}}, card, "")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func rowKey(d *v1alpha1.DailyUsage) string { return d.Date + " " + d.Atespace + "/" + d.Task }

// Acceptance 1 (T16): ListUsage filters by atespace, label selector and an
// inclusive date range (default: the last 30 days) and returns one row per
// task and day with labels, actor uid, amounts, cost and charges.
func TestListUsage_FiltersAndDateRange(t *testing.T) {
	e := newFinopsEnv(t, envOptions{})
	ctx := context.Background()
	// The default range ends today (server clock): avoid running across UTC
	// midnight.
	if now := time.Now().UTC(); now.Add(15*time.Second).Day() != now.Day() {
		time.Sleep(16 * time.Second)
	}
	day := func(back int) string { return time.Now().UTC().AddDate(0, 0, -back).Format(usage.DateLayout) }
	today, d29, d30 := day(0), day(29), day(30)

	e.saveTask(t, "default", "a1", "Running", withLabels("agent", "coder", "customer", "acme"),
		func(task *v1alpha1.Task) { task.Status.ActorUid = "uid-a1" })
	e.saveTask(t, "default", "a2", "Suspended", withLabels("agent", "reviewer"))
	e.saveTask(t, "team", "b1", "Running", withLabels("agent", "coder"))
	e.saveTask(t, "default", "nousage", "Running", withLabels("agent", "coder"))

	tokens := map[string]usage.TokenCounts{"gemini-2.5-flash": {Input: 1000, Output: 500}}
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "a1"}, today, usage.Amounts{CPUSeconds: 10, ActiveSeconds: 60, Tokens: tokens}, "uid-from-usage")
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "a1"}, d29, usage.Amounts{CPUSeconds: 20, MemoryGiBSeconds: 5, IdleSeconds: 3}, "")
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "a1"}, d30, usage.Amounts{CPUSeconds: 40}, "")
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "a1"}, "2026-01-15", usage.Amounts{CPUSeconds: 1}, "")
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "a2"}, today, usage.Amounts{CPUSeconds: 2, StorageGiBSeconds: 100}, "uid-a2")
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "a2"}, "2026-01-16", usage.Amounts{CPUSeconds: 3}, "")
	addUsageDay(t, e.usage, usage.Key{Atespace: "team", Name: "b1"}, today, usage.Amounts{CPUSeconds: 4}, "uid-b1")
	addUsageDay(t, e.usage, usage.Key{Atespace: "team", Name: "b1"}, "2026-01-14", usage.Amounts{CPUSeconds: 5}, "")
	// An orphan record (no task, e.g. deleted) is not listed.
	addUsageDay(t, e.usage, usage.Key{Atespace: "default", Name: "ghost"}, today, usage.Amounts{CPUSeconds: 99}, "uid-ghost")

	for _, tc := range []struct {
		name string
		req  *v1alpha1.ListUsageRequest
		want []string
	}{
		{"default range, all atespaces", &v1alpha1.ListUsageRequest{},
			[]string{d29 + " default/a1", today + " default/a1", today + " default/a2", today + " team/b1"}},
		{"star atespace", &v1alpha1.ListUsageRequest{Atespace: "*"},
			[]string{d29 + " default/a1", today + " default/a1", today + " default/a2", today + " team/b1"}},
		{"atespace", &v1alpha1.ListUsageRequest{Atespace: "team"}, []string{today + " team/b1"}},
		{"selector", &v1alpha1.ListUsageRequest{LabelSelector: "agent=coder"},
			[]string{d29 + " default/a1", today + " default/a1", today + " team/b1"}},
		{"selector and atespace", &v1alpha1.ListUsageRequest{Atespace: "default", LabelSelector: "agent=coder,customer=acme"},
			[]string{d29 + " default/a1", today + " default/a1"}},
		{"explicit inclusive range", &v1alpha1.ListUsageRequest{StartDate: "2026-01-14", EndDate: "2026-01-15"},
			[]string{"2026-01-14 team/b1", "2026-01-15 default/a1"}},
		{"single day", &v1alpha1.ListUsageRequest{StartDate: "2026-01-16", EndDate: "2026-01-16"}, []string{"2026-01-16 default/a2"}},
		{"start only (end = today)", &v1alpha1.ListUsageRequest{StartDate: d30, Atespace: "default"},
			[]string{d30 + " default/a1", d29 + " default/a1", today + " default/a1", today + " default/a2"}},
		{"end only (30 days back)", &v1alpha1.ListUsageRequest{EndDate: "2026-01-16"},
			[]string{"2026-01-14 team/b1", "2026-01-15 default/a1", "2026-01-16 default/a2"}},
		{"no match", &v1alpha1.ListUsageRequest{LabelSelector: "agent=nobody"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := e.srv.ListUsage(ctx, tc.req)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, d := range resp.GetDays() {
				got = append(got, rowKey(d))
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("rows = %q\nwant   %q", got, tc.want)
			}
		})
	}

	// Row contents (via gRPC): labels, uid (task status first, then the
	// usage record), amounts, cost and charges summing to the day cost.
	client := startGRPC(t, e.srv)
	resp, err := client.ListUsage(ctx, &v1alpha1.ListUsageRequest{Atespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]*v1alpha1.DailyUsage{}
	for _, d := range resp.GetDays() {
		byKey[rowKey(d)] = d
		var sum float64
		for _, c := range d.GetCharges() {
			sum += c.GetCost()
		}
		if math.Abs(sum-d.GetCost().GetTotal()) > 1e-12 {
			t.Errorf("%s: charges sum to %v, cost total %v", rowKey(d), sum, d.GetCost().GetTotal())
		}
	}
	a1 := byKey[today+" default/a1"]
	if a1.GetTaskUid() != "uid-a1" || a1.GetLabels()["customer"] != "acme" || a1.GetCpuSeconds() != 10 || a1.GetActiveSeconds() != 60 {
		t.Errorf("a1 today = %v", a1)
	}
	// $0.001/cpu-s × 10 + (1000 × $1 + 500 × $2)/1M = 0.01 + 0.002.
	if c := a1.GetCost(); !approx(c.GetTotal(), 0.012) || !approx(c.GetCompute(), 0.01) || !approx(c.GetTokens(), 0.002) || c.GetCurrency() != "USD" {
		t.Errorf("a1 cost = %v", c)
	}
	if len(a1.GetTokens()) != 1 || a1.GetTokens()[0].GetInput() != 1000 {
		t.Errorf("a1 tokens = %v", a1.GetTokens())
	}
	var kinds []string
	for _, c := range a1.GetCharges() {
		kinds = append(kinds, fmt.Sprintf("%s:%s:%s=%g %s @%g", c.GetKind(), c.GetModel(), c.GetTokenType(), c.GetQuantity(), c.GetUnit(), c.GetListUnitPrice()))
	}
	wantKinds := []string{
		"cpu::=" + fmt.Sprint(10.0/3600) + " vCPU-Hours @3.6",
		"tokens:gemini-2.5-flash:input=1000 Tokens @1",
		"tokens:gemini-2.5-flash:output=500 Tokens @2",
	}
	if !slices.Equal(kinds, wantKinds) {
		t.Errorf("a1 charges = %q, want %q", kinds, wantKinds)
	}
	if a2 := byKey[today+" default/a2"]; a2.GetTaskUid() != "uid-a2" || a2.GetStorageGibSeconds() != 100 {
		t.Errorf("a2 today = %v", a2)
	}
	if d := byKey[d29+" default/a1"]; d.GetMemoryGibSeconds() != 5 || d.GetIdleSeconds() != 3 || d.GetTaskUid() != "uid-a1" {
		t.Errorf("a1 d29 = %v", d)
	}

	// Invalid requests.
	for _, req := range []*v1alpha1.ListUsageRequest{
		{StartDate: "2026-1-5"},
		{EndDate: "yesterday"},
		{StartDate: "2026-02-01", EndDate: "2026-01-31"},
		{LabelSelector: "agent!=x"},
	} {
		if _, err := client.ListUsage(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("ListUsage(%v) = %v, want InvalidArgument", req, err)
		}
	}
}

func TestListUsage_Disabled(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	if _, err := srv.ListUsage(context.Background(), &v1alpha1.ListUsageRequest{}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ListUsage without usage = %v, want FailedPrecondition", err)
	}
}

func TestUsageDateRange(t *testing.T) {
	now := time.Date(2026, 10, 16, 23, 59, 0, 0, time.FixedZone("X", -3*3600)) // 2026-10-17 02:59 UTC
	for _, tc := range []struct {
		start, end         string
		wantStart, wantEnd string
		wantErr            bool
	}{
		{"", "", "2026-09-18", "2026-10-17", false},
		{"", "2026-03-01", "2026-01-31", "2026-03-01", false},
		{"2026-10-01", "", "2026-10-01", "2026-10-17", false},
		{"2026-10-01", "2026-10-01", "2026-10-01", "2026-10-01", false},
		{"2026-10-02", "2026-10-01", "", "", true},
		{"2026-10-32", "", "", "", true},
		{"", "20261001", "", "", true},
	} {
		s, e, err := server.UsageDateRange(tc.start, tc.end, now)
		if (err != nil) != tc.wantErr || s != tc.wantStart || e != tc.wantEnd {
			t.Errorf("UsageDateRange(%q, %q) = %q, %q, %v; want %q, %q (err %v)", tc.start, tc.end, s, e, err, tc.wantStart, tc.wantEnd, tc.wantErr)
		}
	}
}
