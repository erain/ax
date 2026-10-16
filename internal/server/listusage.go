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

package server

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// DefaultUsageDays is the number of UTC days ListUsage returns when the
// request has no start_date (ending at end_date, default today).
const DefaultUsageDays = 30

// usagePageSize is the task store page size used by ListUsage.
const usagePageSize = 500

// ListUsage returns one DailyUsage per task and UTC day with usage in the
// requested range, read from the per-day buckets of the usage records.
// Tasks are selected by atespace ("" or "*" = all) and label selector; their
// labels and actor uid come from the task store. Deleted tasks have no usage
// record, so they are not included. Days are inclusive YYYY-MM-DD; the
// default is the last DefaultUsageDays days. Rows are sorted by date,
// atespace and task.
func (s *Server) ListUsage(ctx context.Context, req *v1alpha1.ListUsageRequest) (*v1alpha1.ListUsageResponse, error) {
	if s.fin == nil {
		return nil, status.Error(codes.FailedPrecondition, "usage accounting is not enabled on this server")
	}
	if req == nil {
		req = &v1alpha1.ListUsageRequest{}
	}
	selector, err := v1alpha1.ParseLabelSelector(req.LabelSelector)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	start, end, err := UsageDateRange(req.StartDate, req.EndDate, s.fin.clock())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	atespace := req.Atespace
	if atespace == "*" {
		atespace = ""
	}

	tasks, err := s.listAllTasks(ctx, atespace, selector)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing tasks: %v", err)
	}
	records, err := s.fin.store.List(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing usage: %v", err)
	}

	card := s.fin.card()
	resp := &v1alpha1.ListUsageResponse{}
	for _, t := range tasks {
		key := keyOf(t)
		u := records[key]
		if u == nil {
			continue
		}
		uid := t.GetStatus().GetActorUid()
		if uid == "" {
			uid = u.State.ActorUID
		}
		for _, date := range slices.Sorted(maps.Keys(u.Days)) {
			day := u.Days[date]
			if day == nil || date < start || date > end {
				continue
			}
			resp.Days = append(resp.Days, &v1alpha1.DailyUsage{
				Atespace:          key.Atespace,
				Task:              key.Name,
				TaskUid:           uid,
				Labels:            maps.Clone(t.GetMetadata().GetLabels()),
				Date:              date,
				CpuSeconds:        day.CPUSeconds,
				MemoryGibSeconds:  day.MemoryGiBSeconds,
				ActiveSeconds:     day.ActiveSeconds,
				IdleSeconds:       day.IdleSeconds,
				StorageGibSeconds: day.StorageGiBSeconds,
				Tokens:            usage.TokensToProto(day.Tokens),
				Cost:              usage.CostToProto(day.Cost),
				Charges:           chargesToProto(card.Breakdown(day, u.State.Pool)),
			})
		}
	}
	slices.SortFunc(resp.Days, func(a, b *v1alpha1.DailyUsage) int {
		if c := strings.Compare(a.Date, b.Date); c != 0 {
			return c
		}
		if c := strings.Compare(a.Atespace, b.Atespace); c != 0 {
			return c
		}
		return strings.Compare(a.Task, b.Task)
	})
	return resp, nil
}

// UsageDateRange resolves a ListUsage date range (inclusive YYYY-MM-DD UTC
// days): an empty end is today (per now), an empty start is
// DefaultUsageDays−1 days before end. It rejects malformed dates and
// start > end.
func UsageDateRange(startDate, endDate string, now time.Time) (start, end string, err error) {
	y, m, d := now.UTC().Date()
	endT := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	if endDate != "" {
		if endT, err = time.Parse(usage.DateLayout, endDate); err != nil {
			return "", "", fmt.Errorf("invalid end_date %q: want YYYY-MM-DD", endDate)
		}
	}
	startT := endT.AddDate(0, 0, -(DefaultUsageDays - 1))
	if startDate != "" {
		if startT, err = time.Parse(usage.DateLayout, startDate); err != nil {
			return "", "", fmt.Errorf("invalid start_date %q: want YYYY-MM-DD", startDate)
		}
	}
	if startT.After(endT) {
		return "", "", fmt.Errorf("start_date %s is after end_date %s", startT.Format(usage.DateLayout), endT.Format(usage.DateLayout))
	}
	return startT.Format(usage.DateLayout), endT.Format(usage.DateLayout), nil
}

// listAllTasks pages through the tasks of atespace ("" = all) matching
// selector. Like ListRunningTasks it stops on an empty page and drops
// duplicates.
func (s *Server) listAllTasks(ctx context.Context, atespace string, selector v1alpha1.LabelSelector) ([]*v1alpha1.Task, error) {
	seen := map[usage.Key]bool{}
	var out []*v1alpha1.Task
	for page, offset := 0, int64(0); page < maxRunningPages; page, offset = page+1, offset+usagePageSize {
		tasks, err := s.store.ListTasks(ctx, atespace, selector, usagePageSize, offset)
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			break
		}
		for _, t := range tasks {
			if k := keyOf(t); !seen[k] {
				seen[k] = true
				out = append(out, t)
			}
		}
	}
	return out, nil
}

func chargesToProto(lines []cost.ChargeLine) []*v1alpha1.UsageCharge {
	out := make([]*v1alpha1.UsageCharge, 0, len(lines))
	for _, l := range lines {
		c := &v1alpha1.UsageCharge{
			Kind:            l.Kind,
			Model:           l.Model,
			TokenType:       l.TokenType,
			Quantity:        l.Quantity,
			Unit:            l.Unit,
			PricingQuantity: l.PricingQuantity,
			PricingUnit:     l.PricingUnit,
			Cost:            l.Cost,
			IdleCost:        l.IdleCost,
		}
		if l.HasListPrice {
			c.ListUnitPrice = proto.Float64(l.ListUnitPrice)
		}
		out = append(out, c)
	}
	return out
}
