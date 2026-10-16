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
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

const costUsage = `Usage:
  ax cost summary [--by <label>] [-l k=v,...] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [-o table|json]
  ax cost export [--format focus-csv|json] [-l k=v,...] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [-o <file>]

summary rolls up task usage and cost by the value of a task label. export
writes FOCUS (FinOps Open Cost and Usage Specification) rows: one per task,
UTC day and charge (vCPU, memory, tokens per model and token type, storage).

Both read per-day usage (ListUsage) for the UTC days --start..--end
(inclusive; default: the 30 days ending today). Against a server without
ListUsage, summary falls back to lifetime totals from ListTasks.

Flags:
  --by string            summary: task label to group by (default "agent"); tasks without it are grouped as <none>
  -l, --selector string  Only include tasks matching this label selector (k=v,...)
  --start string         First UTC day, YYYY-MM-DD (default: 29 days before --end)
  --end string           Last UTC day, YYYY-MM-DD (default: today)
  -o, --output string    summary: output format, table or json (default "table");
                         export: output file (default: stdout)
  --format string        export: focus-csv or json (default "focus-csv")
  -a, --atespace string  Atespace scope (global flag; default "default")`

// noLabelGroup is the group of tasks that do not carry the --by label.
const noLabelGroup = "<none>"

// defaultUsageDays is the default date range of ListUsage (keep in sync with
// server.DefaultUsageDays).
const defaultUsageDays = 30

// costSummaryOptions are the parsed flags of `ax cost summary`.
type costSummaryOptions struct {
	by       string
	selector string
	output   string
	start    string
	end      string
}

// costExportOptions are the parsed flags of `ax cost export`.
type costExportOptions struct {
	format   string
	selector string
	start    string
	end      string
	file     string
}

// costCommand is a parsed `ax cost` invocation.
type costCommand struct {
	sub     string // "summary" or "export"
	summary costSummaryOptions
	export  costExportOptions
}

// parseCostFlags parses `--name value` / `--name=value` flags into flags.
func parseCostFlags(sub string, args []string, flags map[string]*string) error {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, value, hasValue := arg, "", false
		if strings.HasPrefix(arg, "-") {
			if k, v, ok := strings.Cut(arg, "="); ok {
				name, value, hasValue = k, v, true
			}
		}
		dst, ok := flags[name]
		if !ok {
			return fmt.Errorf("unexpected argument %q for 'ax cost %s'", arg, sub)
		}
		if !hasValue {
			if i+1 >= len(args) {
				return fmt.Errorf("flag %s requires a value", name)
			}
			value = args[i+1]
			i++
		}
		*dst = value
	}
	return nil
}

// validateDateFlags checks --start/--end (YYYY-MM-DD, start <= end).
func validateDateFlags(start, end string) error {
	for _, f := range []struct{ name, v string }{{"--start", start}, {"--end", end}} {
		if f.v == "" {
			continue
		}
		if _, err := time.Parse(time.DateOnly, f.v); err != nil {
			return fmt.Errorf("invalid %s %q: want YYYY-MM-DD", f.name, f.v)
		}
	}
	if start != "" && end != "" && start > end {
		return fmt.Errorf("--start %s is after --end %s", start, end)
	}
	return nil
}

// resolveDateRange applies the ListUsage defaults to validated --start/--end:
// an empty end is today (UTC), an empty start is 29 days before end.
func resolveDateRange(start, end string, now time.Time) (string, string) {
	if end == "" {
		end = now.UTC().Format(time.DateOnly)
	}
	if start == "" {
		e, _ := time.Parse(time.DateOnly, end)
		start = e.AddDate(0, 0, -(defaultUsageDays - 1)).Format(time.DateOnly)
	}
	return start, end
}

// parseCostSummaryArgs parses the flags of `ax cost summary` (the arguments
// after "summary").
func parseCostSummaryArgs(args []string) (costSummaryOptions, error) {
	opts := costSummaryOptions{by: labelAgent, output: "table"}
	if err := parseCostFlags("summary", args, map[string]*string{
		"--by": &opts.by, "-l": &opts.selector, "--selector": &opts.selector,
		"-o": &opts.output, "--output": &opts.output, "--start": &opts.start, "--end": &opts.end,
	}); err != nil {
		return opts, err
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
	return opts, validateDateFlags(opts.start, opts.end)
}

// parseCostExportArgs parses the flags of `ax cost export` (the arguments
// after "export").
func parseCostExportArgs(args []string) (costExportOptions, error) {
	opts := costExportOptions{format: formatFocusCSV}
	if err := parseCostFlags("export", args, map[string]*string{
		"--format": &opts.format, "-l": &opts.selector, "--selector": &opts.selector,
		"-o": &opts.file, "--output": &opts.file, "--start": &opts.start, "--end": &opts.end,
	}); err != nil {
		return opts, err
	}
	if _, err := v1alpha1.ParseLabelSelector(opts.selector); err != nil {
		return opts, err
	}
	if opts.format != formatFocusCSV && opts.format != formatJSON {
		return opts, fmt.Errorf("unsupported export format %q (want %s or %s)", opts.format, formatFocusCSV, formatJSON)
	}
	return opts, validateDateFlags(opts.start, opts.end)
}

// parseCostArgs parses `ax cost <subcommand> [flags]`. It writes the usage
// text and returns done=true for help requests; main calls it before resolving
// the server so help and usage errors never contact a cluster.
func parseCostArgs(args []string, out io.Writer) (cmd costCommand, done bool, err error) {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprintln(out, costUsage)
		if len(args) == 0 {
			return cmd, true, fmt.Errorf("specify a subcommand (e.g. 'ax cost summary')")
		}
		return cmd, true, nil
	}
	cmd.sub = args[0]
	if cmd.sub != "summary" && cmd.sub != "export" {
		return cmd, true, fmt.Errorf("unknown cost subcommand %q (want summary or export)", args[0])
	}
	for _, a := range args[1:] {
		if a == "-h" || a == "--help" {
			fmt.Fprintln(out, costUsage)
			return cmd, true, nil
		}
	}
	if cmd.sub == "export" {
		cmd.export, err = parseCostExportArgs(args[1:])
	} else {
		cmd.summary, err = parseCostSummaryArgs(args[1:])
	}
	return cmd, err != nil, err
}

// runCost implements `ax cost <subcommand>`.
func runCost(serverURL, atespace string, args []string, out io.Writer) error {
	cmd, done, err := parseCostArgs(args, out)
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

	if cmd.sub == "export" {
		return runCostExport(ctx, client, atespace, cmd.export, out)
	}
	return runCostSummary(ctx, client, atespace, cmd.summary, out)
}

// runCostSummary implements `ax cost summary`: per-day usage from ListUsage,
// or lifetime totals from ListTasks when the server has no ListUsage.
func runCostSummary(ctx context.Context, client v1alpha1.AXClient, atespace string, opts costSummaryOptions, out io.Writer) error {
	start, end := resolveDateRange(opts.start, opts.end, nowFunc())
	var src usageSource = listUsageSource{client: client, start: start, end: end}
	rows, err := src.rows(ctx, atespace, opts.selector)
	if listUsageUnavailable(err) {
		if opts.start != "" || opts.end != "" {
			fmt.Fprintf(os.Stderr, "Warning: the server does not serve per-day usage (%s); showing lifetime totals, --start/--end ignored\n",
				status.Convert(err).Message())
		}
		src = taskUsageSource{client: client}
		rows, err = src.rows(ctx, atespace, opts.selector)
	}
	if err != nil {
		return err
	}
	s := summarize(rows, opts.by)
	s.Atespace = atespace
	s.Selector = opts.selector
	s.Source = src.name()
	if ls, ok := src.(listUsageSource); ok {
		s.Start, s.End = ls.start, ls.end
	}
	if opts.output == "json" {
		return writeCostSummaryJSON(out, s)
	}
	return writeCostSummaryTable(out, s)
}

// listUsageUnavailable reports whether a ListUsage error means the server
// cannot serve per-day usage (an older server, or usage accounting off).
func listUsageUnavailable(err error) bool {
	switch status.Code(err) {
	case codes.Unimplemented, codes.FailedPrecondition:
		return true
	}
	return false
}

// usageRow is one unit of usage attributed to a task: a task's lifetime
// totals (taskUsageSource) or one task and UTC day (listUsageSource), which
// summarize handles the same way (tasks are counted once per group).
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

var _ usageSource = listUsageSource{}

// listUsageSource reads per-task, per-day usage via ListUsage for the UTC
// days start..end (inclusive).
type listUsageSource struct {
	client     v1alpha1.AXClient
	start, end string
}

func (listUsageSource) name() string { return "usage" }

func (s listUsageSource) rows(ctx context.Context, atespace, selector string) ([]usageRow, error) {
	days, err := s.days(ctx, atespace, selector)
	if err != nil {
		return nil, err
	}
	rows := make([]usageRow, 0, len(days))
	for _, d := range days {
		rows = append(rows, usageRow{
			Atespace:      d.GetAtespace(),
			Task:          d.GetTask(),
			Labels:        d.GetLabels(),
			ActiveSeconds: d.GetActiveSeconds(),
			IdleSeconds:   d.GetIdleSeconds(),
			Tokens:        totalTokens(d.GetTokens()),
			Cost:          d.GetCost(),
		})
	}
	return rows, nil
}

// days calls ListUsage. Errors keep their gRPC status (listUsageUnavailable).
func (s listUsageSource) days(ctx context.Context, atespace, selector string) ([]*v1alpha1.DailyUsage, error) {
	resp, err := s.client.ListUsage(ctx, &v1alpha1.ListUsageRequest{
		Atespace: atespace, LabelSelector: selector, StartDate: s.start, EndDate: s.end,
	})
	if err != nil {
		if listUsageUnavailable(err) {
			return nil, err
		}
		return nil, fmt.Errorf("listing usage: %w", err)
	}
	return resp.GetDays(), nil
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
	Start    string       `json:"start,omitempty"`
	End      string       `json:"end,omitempty"`
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
	if s.Start != "" {
		fmt.Fprintf(out, "Period: %s to %s (UTC days, inclusive)\n", s.Start, s.End)
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
