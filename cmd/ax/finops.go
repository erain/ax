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

// Formatting of usage, cost and budget data shared by `ax get tasks`,
// `ax describe task` and `ax cost summary`.

import (
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

// unpricedNote explains CostBreakdown.tokens_partially_unpriced.
const unpricedNote = "tokens partially unpriced: some models are not on the rate card; their tokens are counted but not priced"

// formatMoney renders an amount with 4 decimals below 1 and 2 decimals from 1
// up ($0.0123, $1.23). USD (or an empty currency) uses a "$" prefix; any other
// currency is appended as a code ("1.23 EUR").
func formatMoney(amount float64, currency string) string {
	decimals := 2
	if math.Abs(amount) < 0.99995 {
		decimals = 4
	}
	s := strconv.FormatFloat(amount, 'f', decimals, 64)
	if s == "-0.0000" {
		s = "0.0000"
	}
	if currency == "" || currency == v1alpha1.BudgetCurrency {
		if strings.HasPrefix(s, "-") {
			return "-$" + s[1:]
		}
		return "$" + s
	}
	return s + " " + currency
}

// taskCost returns the COST column value of a task: its total cost, or "-"
// when the task has no usage yet.
func taskCost(t *v1alpha1.Task) string {
	c := t.GetStatus().GetUsage().GetCost()
	if c == nil {
		return "-"
	}
	return formatMoney(c.GetTotal(), c.GetCurrency())
}

// formatDuration renders a duration rounded to whole seconds ("25m0s").
func formatDuration(d time.Duration) string {
	return d.Round(time.Second).String()
}

// formatSeconds renders a number of seconds like formatDuration.
func formatSeconds(s float64) string {
	return formatDuration(time.Duration(s * float64(time.Second)))
}

// formatBytes renders a byte count with binary (IEC) units ("12.3 MiB").
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	v := float64(n)
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	i := -1
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// formatCount renders an integer with thousands separators ("1,234,567").
func formatCount(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

// totalTokens sums input and output tokens over all models. Following the
// semconv convention used by the runner, input already includes cache
// reads/writes and output already includes reasoning tokens.
func totalTokens(tokens []*v1alpha1.ModelTokenUsage) int64 {
	var n int64
	for _, t := range tokens {
		n += t.GetInput() + t.GetOutput()
	}
	return n
}

// formatLabels renders labels as sorted "k=v" pairs joined by sep, or
// "<none>".
func formatLabels(labels map[string]string, sep string) string {
	if len(labels) == 0 {
		return "<none>"
	}
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, sep)
}

// describeIndent aligns values in `ax describe task` output.
const describeIndent = "              "

// writeTaskLabels writes the Labels line of `ax describe task`, one label per
// line.
func writeTaskLabels(out io.Writer, labels map[string]string) {
	fmt.Fprintf(out, "Labels:       %s\n", formatLabels(labels, "\n"+describeIndent))
}

// writeTaskBudget writes the Budget block of `ax describe task`. Nothing is
// written when the task has no budget.
func writeTaskBudget(out io.Writer, b *v1alpha1.BudgetSpec) {
	if b == nil {
		return
	}
	fmt.Fprintln(out, "Budget:")
	if d := b.GetActiveDeadline(); d != nil {
		fmt.Fprintf(out, "  Active Deadline:  %s\n", formatDuration(d.AsDuration()))
	}
	if b.GetMaxTokens() > 0 {
		fmt.Fprintf(out, "  Max Tokens:       %s\n", formatCount(b.GetMaxTokens()))
	}
	if m := b.GetMaxCost(); m != nil {
		fmt.Fprintf(out, "  Max Cost:         %s\n", formatMoney(m.GetAmount(), m.GetCurrency()))
	}
	if b.GetWarnAtPercent() > 0 {
		fmt.Fprintf(out, "  Warn At:          %d%%\n", b.GetWarnAtPercent())
	} else {
		fmt.Fprintln(out, "  Warn At:          server default")
	}
	action := b.GetOnExceeded().String()
	if b.GetOnExceeded() == v1alpha1.ExceededAction_EXCEEDED_ACTION_UNSPECIFIED {
		action = v1alpha1.ExceededAction_WARN.String()
	}
	fmt.Fprintf(out, "  On Exceeded:      %s\n", action)
}

// writeTaskUsage writes the Usage and Cost blocks of `ax describe task`.
func writeTaskUsage(out io.Writer, u *v1alpha1.UsageStatus) error {
	if u == nil {
		fmt.Fprintln(out, "Usage:        <none>")
		return nil
	}
	fmt.Fprintln(out, "Usage:")
	fmt.Fprintf(out, "  Active:           %s\n", formatDuration(u.GetActiveDuration().AsDuration()))
	fmt.Fprintf(out, "  Idle:             %s\n", formatDuration(u.GetIdleDuration().AsDuration()))
	fmt.Fprintf(out, "  CPU:              %.2f s\n", u.GetCpuSeconds())
	fmt.Fprintf(out, "  Memory:           %.2f GiB·s\n", u.GetMemoryGibSeconds())
	fmt.Fprintf(out, "  Workspace:        %s\n", formatBytes(u.GetWorkspaceBytes()))
	if ts := u.GetUpdateTime(); ts != nil {
		fmt.Fprintf(out, "  Updated:          %s\n", ts.AsTime().UTC().Format(time.RFC3339))
	}
	if len(u.GetTokens()) == 0 {
		fmt.Fprintln(out, "  Tokens:           <none>")
	} else {
		fmt.Fprintln(out, "  Tokens:")
		tokens := append([]*v1alpha1.ModelTokenUsage(nil), u.GetTokens()...)
		sort.SliceStable(tokens, func(i, j int) bool { return tokens[i].GetModel() < tokens[j].GetModel() })
		w := tabwriter.NewWriter(out, 0, 8, 3, ' ', 0)
		fmt.Fprintln(w, "    MODEL\tINPUT\tOUTPUT\tCACHE-READ\tCACHE-WRITE\tREASONING")
		for _, t := range tokens {
			fmt.Fprintf(w, "    %s\t%s\t%s\t%s\t%s\t%s\n", t.GetModel(),
				formatCount(t.GetInput()), formatCount(t.GetOutput()), formatCount(t.GetCacheRead()),
				formatCount(t.GetCacheWrite()), formatCount(t.GetReasoning()))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}

	c := u.GetCost()
	if c == nil {
		fmt.Fprintln(out, "Cost:         <none>")
		return nil
	}
	cur := c.GetCurrency()
	fmt.Fprintln(out, "Cost:")
	fmt.Fprintf(out, "  Compute:          %s\n", formatMoney(c.GetCompute(), cur))
	fmt.Fprintf(out, "    of which idle:  %s\n", formatMoney(c.GetIdle(), cur))
	tokensCost := formatMoney(c.GetTokens(), cur)
	if c.GetTokensPartiallyUnpriced() {
		tokensCost += " *"
	}
	fmt.Fprintf(out, "  Tokens:           %s\n", tokensCost)
	fmt.Fprintf(out, "  Storage:          %s\n", formatMoney(c.GetStorage(), cur))
	fmt.Fprintf(out, "  Total:            %s\n", formatMoney(c.GetTotal(), cur))
	if c.GetTokensPartiallyUnpriced() {
		fmt.Fprintf(out, "  * %s\n", unpricedNote)
	}
	return nil
}

// writeTaskConditions writes the Conditions table of `ax describe task`.
func writeTaskConditions(out io.Writer, conditions []*v1alpha1.Condition) error {
	if len(conditions) == 0 {
		return nil
	}
	fmt.Fprintln(out, "\nConditions:")
	w := tabwriter.NewWriter(out, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "  TYPE\tSTATUS\tREASON\tMESSAGE")
	for _, c := range conditions {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\n", c.GetType(), c.GetStatus(), c.GetReason(), c.GetMessage())
	}
	return w.Flush()
}
