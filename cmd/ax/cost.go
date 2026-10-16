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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

const costUsage = `Usage:
  ax cost summary [--by <label>] [-l k=v,...] [-a <atespace>] [-o table|json]

Roll up task usage and cost by the value of a task label.

Flags:
  --by string            Task label to group by (default "agent"); tasks without it are grouped as <none>
  -l, --selector string  Only include tasks matching this label selector (k=v,...)
  -a, --atespace string  Atespace scope (default "default")
  -o, --output string    Output format: table or json (default "table")`

// noLabelGroup is the group of tasks that do not carry the --by label.
const noLabelGroup = "<none>"

// costSummaryOptions are the parsed flags of `ax cost summary`.
type costSummaryOptions struct {
	by       string
	selector string
	output   string
}

// parseCostSummaryArgs parses the flags of `ax cost summary` (the arguments
// after "summary").
func parseCostSummaryArgs(args []string) (costSummaryOptions, error) {
	opts := costSummaryOptions{by: labelAgent, output: "table"}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, value, hasValue := arg, "", false
		if strings.HasPrefix(arg, "-") {
			if k, v, ok := strings.Cut(arg, "="); ok {
				name, value, hasValue = k, v, true
			}
		}
		var dst *string
		switch name {
		case "--by":
			dst = &opts.by
		case "-l", "--selector":
			dst = &opts.selector
		case "-o", "--output":
			dst = &opts.output
		default:
			return opts, fmt.Errorf("unexpected argument %q for 'ax cost summary'", arg)
		}
		if !hasValue {
			if i+1 >= len(args) {
				return opts, fmt.Errorf("flag %s requires a value", name)
			}
			value = args[i+1]
			i++
		}
		*dst = value
	}
	if err := v1alpha1.ValidateLabelKey(opts.by); err != nil {
		return opts, fmt.Errorf("invalid --by label %q: %v", opts.by, err)
	}
	if _, err := v1alpha1.ParseLabelSelector(opts.selector); err != nil {
		return opts, err
	}
	if opts.output != "table" && opts.output != "json" {
		return opts, fmt.Errorf("unsupported output format %q (want table or json)", opts.output)
	}
	return opts, nil
}

// parseCostArgs parses `ax cost <subcommand> [flags]`. It writes the usage
// text and returns done=true for help requests; main calls it before resolving
// the server so help and usage errors never contact a cluster.
func parseCostArgs(args []string, out io.Writer) (opts costSummaryOptions, done bool, err error) {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(out, costUsage)
		if len(args) == 0 {
			return opts, true, fmt.Errorf("specify a subcommand (e.g. 'ax cost summary')")
		}
		return opts, true, nil
	}
	if args[0] != "summary" {
		return opts, true, fmt.Errorf("unknown cost subcommand %q (want summary)", args[0])
	}
	for _, a := range args[1:] {
		if a == "-h" || a == "--help" {
			fmt.Fprintln(out, costUsage)
			return opts, true, nil
		}
	}
	opts, err = parseCostSummaryArgs(args[1:])
	return opts, err != nil, err
}

// runCost implements `ax cost <subcommand>`.
func runCost(serverURL, atespace string, args []string, out io.Writer) error {
	opts, done, err := parseCostArgs(args, out)
	if done || err != nil {
		return err
	}

	client, conn, err := getAXClient(serverURL)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := taskUsageSource{client: client}
	rows, err := src.rows(ctx, atespace, opts.selector)
	if err != nil {
		return err
	}
	s := summarize(rows, opts.by)
	s.Atespace = atespace
	s.Selector = opts.selector
	s.Source = src.name()
	if opts.output == "json" {
		return writeCostSummaryJSON(out, s)
	}
	return writeCostSummaryTable(out, s)
}

// usageRow is one unit of usage attributed to a task. With the ListTasks
// source it holds a task's lifetime totals; a ListUsage source (T16) yields
// one row per task and UTC day, which summarize handles the same way (tasks
// are counted once per group).
type usageRow struct {
	Atespace      string
	Task          string
	Labels        map[string]string
	ActiveSeconds float64
	IdleSeconds   float64
	Tokens        int64
	Cost          *v1alpha1.CostBreakdown
}

// usageSource produces the usage rows that `ax cost summary` aggregates.
type usageSource interface {
	name() string
	rows(ctx context.Context, atespace, selector string) ([]usageRow, error)
}

var _ usageSource = taskUsageSource{}

// taskUsageSource reads lifetime usage from Task.status.usage via ListTasks,
// paging through all matching tasks.
type taskUsageSource struct {
	client v1alpha1.AXClient
}

// taskPageSize is the ListTasks page size used by taskUsageSource.
const taskPageSize = 500

func (taskUsageSource) name() string { return "tasks" }

func (s taskUsageSource) rows(ctx context.Context, atespace, selector string) ([]usageRow, error) {
	var rows []usageRow
	for offset := int64(0); ; offset += taskPageSize {
		resp, err := s.client.ListTasks(ctx, &v1alpha1.ListTasksRequest{
			Atespace: atespace, LabelSelector: selector, Limit: taskPageSize, Offset: offset,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tasks: %w", err)
		}
		for _, t := range resp.GetTasks() {
			rows = append(rows, taskUsageRow(t))
		}
		if len(resp.GetTasks()) < taskPageSize {
			return rows, nil
		}
	}
}

// taskUsageRow converts a task's lifetime usage into a usageRow. Tasks without
// usage yield a zero row so they still count in TASKS.
func taskUsageRow(t *v1alpha1.Task) usageRow {
	u := t.GetStatus().GetUsage()
	return usageRow{
		Atespace:      t.GetMetadata().GetAtespace(),
		Task:          t.GetMetadata().GetName(),
		Labels:        t.GetMetadata().GetLabels(),
		ActiveSeconds: u.GetActiveDuration().AsDuration().Seconds(),
		IdleSeconds:   u.GetIdleDuration().AsDuration().Seconds(),
		Tokens:        totalTokens(u.GetTokens()),
		Cost:          u.GetCost(),
	}
}

// costTotals are summed costs; field meanings follow CostBreakdown (idle is a
// subset of compute; total = compute + tokens + storage).
type costTotals struct {
	Compute                 float64 `json:"compute"`
	Idle                    float64 `json:"idle"`
	Tokens                  float64 `json:"tokens"`
	Storage                 float64 `json:"storage"`
	Total                   float64 `json:"total"`
	TokensPartiallyUnpriced bool    `json:"tokensPartiallyUnpriced,omitempty"`
}

// costGroup is one row of `ax cost summary`.
type costGroup struct {
	Group         string     `json:"group"`
	Tasks         int        `json:"tasks"`
	ActiveSeconds float64    `json:"activeSeconds"`
	IdleSeconds   float64    `json:"idleSeconds"`
	Tokens        int64      `json:"tokens"`
	Cost          costTotals `json:"cost"`

	taskSet map[string]bool
}

func (g *costGroup) add(r usageRow) {
	if g.taskSet == nil {
		g.taskSet = make(map[string]bool)
	}
	g.taskSet[r.Atespace+"/"+r.Task] = true
	g.Tasks = len(g.taskSet)
	g.ActiveSeconds += r.ActiveSeconds
	g.IdleSeconds += r.IdleSeconds
	g.Tokens += r.Tokens
	if c := r.Cost; c != nil {
		g.Cost.Compute += c.GetCompute()
		g.Cost.Idle += c.GetIdle()
		g.Cost.Tokens += c.GetTokens()
		g.Cost.Storage += c.GetStorage()
		g.Cost.Total += c.GetTotal()
		g.Cost.TokensPartiallyUnpriced = g.Cost.TokensPartiallyUnpriced || c.GetTokensPartiallyUnpriced()
	}
}

// costSummary is the result of `ax cost summary` (also its JSON form).
type costSummary struct {
	By       string       `json:"by"`
	Atespace string       `json:"atespace"`
	Selector string       `json:"selector,omitempty"`
	Source   string       `json:"source"`
	Currency string       `json:"currency"`
	Groups   []*costGroup `json:"groups"`
	Total    *costGroup   `json:"total"`
}

// summarize groups rows by the value of label `by`, sorted by total cost
// (descending, ties by group name).
func summarize(rows []usageRow, by string) costSummary {
	s := costSummary{By: by, Groups: []*costGroup{}, Total: &costGroup{Group: "TOTAL"}}
	groups := make(map[string]*costGroup)
	for _, r := range rows {
		key := r.Labels[by]
		if key == "" {
			key = noLabelGroup
		}
		g := groups[key]
		if g == nil {
			g = &costGroup{Group: key}
			groups[key] = g
			s.Groups = append(s.Groups, g)
		}
		g.add(r)
		s.Total.add(r)
		if s.Currency == "" && r.Cost.GetCurrency() != "" {
			s.Currency = r.Cost.GetCurrency()
		}
	}
	if s.Currency == "" {
		s.Currency = v1alpha1.BudgetCurrency
	}
	sort.SliceStable(s.Groups, func(i, j int) bool {
		a, b := s.Groups[i], s.Groups[j]
		if a.Cost.Total != b.Cost.Total {
			return a.Cost.Total > b.Cost.Total
		}
		return a.Group < b.Group
	})
	return s
}

func writeCostSummaryTable(out io.Writer, s costSummary) error {
	w := tabwriter.NewWriter(out, 0, 8, 3, ' ', 0)
	fmt.Fprintln(w, "GROUP\tTASKS\tACTIVE\tIDLE\tTOKENS\tCOMPUTE\tTOKENS$\tSTORAGE\tTOTAL")
	unpriced := false
	row := func(g *costGroup) {
		tokensCost := formatMoney(g.Cost.Tokens, s.Currency)
		if g.Cost.TokensPartiallyUnpriced {
			tokensCost += "*"
			unpriced = true
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			g.Group,
			strconv.Itoa(g.Tasks),
			formatSeconds(g.ActiveSeconds),
			formatSeconds(g.IdleSeconds),
			formatCount(g.Tokens),
			formatMoney(g.Cost.Compute, s.Currency),
			tokensCost,
			formatMoney(g.Cost.Storage, s.Currency),
			formatMoney(g.Cost.Total, s.Currency),
		)
	}
	for _, g := range s.Groups {
		row(g)
	}
	row(s.Total)
	if err := w.Flush(); err != nil {
		return err
	}
	if unpriced {
		fmt.Fprintf(out, "* %s\n", unpricedNote)
	}
	return nil
}

// writeCostSummaryJSON writes the summary as JSON. Amounts are rounded to
// 1e-6 (and seconds to 1e-3) to drop float summation noise; they are not
// rounded to display precision.
func writeCostSummaryJSON(out io.Writer, s costSummary) error {
	for _, g := range append(append([]*costGroup(nil), s.Groups...), s.Total) {
		g.ActiveSeconds = roundTo(g.ActiveSeconds, 1e3)
		g.IdleSeconds = roundTo(g.IdleSeconds, 1e3)
		for _, v := range []*float64{&g.Cost.Compute, &g.Cost.Idle, &g.Cost.Tokens, &g.Cost.Storage, &g.Cost.Total} {
			*v = roundTo(*v, 1e6)
		}
	}
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}

func roundTo(v, scale float64) float64 { return math.Round(v*scale) / scale }
