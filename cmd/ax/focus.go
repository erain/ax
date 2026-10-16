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

// `ax cost export`: FOCUS (FinOps Open Cost and Usage Specification) rows
// from ListUsage. The column mapping is documented in docs/finops-focus.md.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// Export formats.
const (
	formatFocusCSV = "focus-csv"
	formatJSON     = "json"
)

// Charge kinds of v1alpha1.UsageCharge.
const (
	chargeCPU     = "cpu"
	chargeMemory  = "memory"
	chargeTokens  = "tokens"
	chargeStorage = "storage"
)

// labelCustomer is the task label exported as x_Customer (labelAgent is
// exported as x_Agent).
const labelCustomer = "customer"

// FOCUS values used by the export.
const (
	focusProvider        = "AX"
	focusSandboxService  = "AX Agent Sandbox"
	focusGeminiService   = "Gemini API"
	focusModelService    = "Model API"
	focusCatCompute      = "Compute"
	focusCatAI           = "AI and Machine Learning"
	focusCatStorage      = "Storage"
	focusChargeUsage     = "Usage"
	focusFreqUsageBased  = "Usage-Based"
	focusResourceType    = "AX Task"
	focusTimestampLayout = "2006-01-02T15:04:05Z"
)

// focusRow is one FOCUS row. Field order is the column order; the JSON names
// are the FOCUS column names (x_ = AX extension columns).
type focusRow struct {
	BillingPeriodStart string          `json:"BillingPeriodStart"`
	BillingPeriodEnd   string          `json:"BillingPeriodEnd"`
	ChargePeriodStart  string          `json:"ChargePeriodStart"`
	ChargePeriodEnd    string          `json:"ChargePeriodEnd"`
	BilledCost         float64         `json:"BilledCost"`
	EffectiveCost      float64         `json:"EffectiveCost"`
	ListCost           float64         `json:"ListCost"`
	ContractedCost     float64         `json:"ContractedCost"`
	BillingCurrency    string          `json:"BillingCurrency"`
	ProviderName       string          `json:"ProviderName"`
	PublisherName      string          `json:"PublisherName"`
	ServiceName        string          `json:"ServiceName"`
	ServiceCategory    string          `json:"ServiceCategory"`
	ChargeCategory     string          `json:"ChargeCategory"`
	ChargeFrequency    string          `json:"ChargeFrequency"`
	ChargeDescription  string          `json:"ChargeDescription"`
	SubAccountId       string          `json:"SubAccountId"`
	SubAccountName     string          `json:"SubAccountName"`
	ResourceId         string          `json:"ResourceId"`
	ResourceName       string          `json:"ResourceName"`
	ResourceType       string          `json:"ResourceType"`
	ConsumedQuantity   float64         `json:"ConsumedQuantity"`
	ConsumedUnit       string          `json:"ConsumedUnit"`
	PricingQuantity    float64         `json:"PricingQuantity"`
	PricingUnit        string          `json:"PricingUnit"`
	ListUnitPrice      *float64        `json:"ListUnitPrice"`
	Tags               json.RawMessage `json:"Tags"` // a JSON object (FOCUS KeyValueFormat)
	XAgent             string          `json:"x_Agent"`
	XCustomer          string          `json:"x_Customer"`
	XModel             string          `json:"x_Model"`
	XTokenType         string          `json:"x_TokenType"`
	XChargeKind        string          `json:"x_ChargeKind"`
	XTaskUid           string          `json:"x_TaskUid"`
	XIdleCost          float64         `json:"x_IdleCost"`
}

// focusColumns is the CSV header (same order as focusRow).
var focusColumns = []string{
	"BillingPeriodStart", "BillingPeriodEnd", "ChargePeriodStart", "ChargePeriodEnd",
	"BilledCost", "EffectiveCost", "ListCost", "ContractedCost", "BillingCurrency",
	"ProviderName", "PublisherName", "ServiceName", "ServiceCategory",
	"ChargeCategory", "ChargeFrequency", "ChargeDescription",
	"SubAccountId", "SubAccountName", "ResourceId", "ResourceName", "ResourceType",
	"ConsumedQuantity", "ConsumedUnit", "PricingQuantity", "PricingUnit", "ListUnitPrice",
	"Tags", "x_Agent", "x_Customer", "x_Model", "x_TokenType", "x_ChargeKind", "x_TaskUid", "x_IdleCost",
}

func (r focusRow) csvRecord() []string {
	price := ""
	if r.ListUnitPrice != nil {
		price = formatFloat(*r.ListUnitPrice)
	}
	return []string{
		r.BillingPeriodStart, r.BillingPeriodEnd, r.ChargePeriodStart, r.ChargePeriodEnd,
		formatFloat(r.BilledCost), formatFloat(r.EffectiveCost), formatFloat(r.ListCost), formatFloat(r.ContractedCost), r.BillingCurrency,
		r.ProviderName, r.PublisherName, r.ServiceName, r.ServiceCategory,
		r.ChargeCategory, r.ChargeFrequency, r.ChargeDescription,
		r.SubAccountId, r.SubAccountName, r.ResourceId, r.ResourceName, r.ResourceType,
		formatFloat(r.ConsumedQuantity), r.ConsumedUnit, formatFloat(r.PricingQuantity), r.PricingUnit, price,
		string(r.Tags), r.XAgent, r.XCustomer, r.XModel, r.XTokenType, r.XChargeKind, r.XTaskUid, formatFloat(r.XIdleCost),
	}
}

// formatFloat writes the shortest decimal that round-trips (no exponent), so
// summing the parsed CSV gives the same float sums as the server's values.
func formatFloat(v float64) string {
	if v == 0 {
		return "0"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// runCostExport implements `ax cost export`.
func runCostExport(ctx context.Context, client v1alpha1.AXClient, atespace string, opts costExportOptions, out io.Writer) error {
	start, end := resolveDateRange(opts.start, opts.end, nowFunc())
	days, err := listUsageSource{client: client, start: start, end: end}.days(ctx, atespace, opts.selector)
	if listUsageUnavailable(err) {
		return fmt.Errorf("cost export needs per-day usage (ListUsage), which this server does not serve: %w", err)
	}
	if err != nil {
		return err
	}
	rows := focusRows(days)

	if opts.file != "" {
		f, err := os.Create(opts.file)
		if err != nil {
			return err
		}
		if err := writeFocus(f, opts.format, rows); err != nil {
			f.Close()
			return err
		}
		return f.Close()
	}
	return writeFocus(out, opts.format, rows)
}

func writeFocus(out io.Writer, format string, rows []focusRow) error {
	if format == formatJSON {
		enc := json.NewEncoder(out)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if rows == nil {
			rows = []focusRow{}
		}
		return enc.Encode(rows)
	}
	w := csv.NewWriter(out)
	if err := w.Write(focusColumns); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write(r.csvRecord()); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// focusRows converts daily usage into FOCUS rows: one per task, day and
// charge. The BilledCost of a day's rows sums to the day's cost.total.
func focusRows(days []*v1alpha1.DailyUsage) []focusRow {
	var rows []focusRow
	for _, d := range days {
		day, err := time.Parse(time.DateOnly, d.GetDate())
		if err != nil {
			continue // the server only returns YYYY-MM-DD
		}
		month := time.Date(day.Year(), day.Month(), 1, 0, 0, 0, 0, time.UTC)
		currency := d.GetCost().GetCurrency()
		if currency == "" {
			currency = v1alpha1.BudgetCurrency
		}
		tags, _ := json.Marshal(d.GetLabels()) // sorted keys; never fails for map[string]string
		if d.GetLabels() == nil {
			tags = []byte("{}")
		}
		resourceID := d.GetAtespace() + "/" + d.GetTask()
		base := focusRow{
			BillingPeriodStart: month.Format(focusTimestampLayout),
			BillingPeriodEnd:   month.AddDate(0, 1, 0).Format(focusTimestampLayout),
			ChargePeriodStart:  day.Format(focusTimestampLayout),
			ChargePeriodEnd:    day.AddDate(0, 0, 1).Format(focusTimestampLayout),
			BillingCurrency:    currency,
			ProviderName:       focusProvider,
			ChargeCategory:     focusChargeUsage,
			ChargeFrequency:    focusFreqUsageBased,
			SubAccountId:       d.GetAtespace(),
			SubAccountName:     d.GetAtespace(),
			ResourceId:         resourceID,
			ResourceName:       d.GetTask(),
			ResourceType:       focusResourceType,
			Tags:               tags,
			XAgent:             d.GetLabels()[labelAgent],
			XCustomer:          d.GetLabels()[labelCustomer],
			XTaskUid:           d.GetTaskUid(),
		}
		charges, fromServer := d.GetCharges(), true
		if len(charges) == 0 {
			charges, fromServer = fallbackCharges(d), false
		}
		for _, c := range charges {
			r := base
			r.BilledCost, r.EffectiveCost, r.ListCost, r.ContractedCost = c.GetCost(), c.GetCost(), c.GetCost(), c.GetCost()
			r.ConsumedQuantity, r.ConsumedUnit = c.GetQuantity(), c.GetUnit()
			r.PricingQuantity, r.PricingUnit = c.GetPricingQuantity(), c.GetPricingUnit()
			if c.ListUnitPrice != nil {
				p := c.GetListUnitPrice()
				r.ListUnitPrice = &p
			}
			r.XChargeKind = c.GetKind()
			r.XIdleCost = c.GetIdleCost()
			switch c.GetKind() {
			case chargeTokens:
				r.ServiceName, r.PublisherName = modelService(c.GetModel())
				r.ServiceCategory = focusCatAI
				r.XModel, r.XTokenType = c.GetModel(), c.GetTokenType()
				r.ChargeDescription = tokenDescription(c, fromServer)
			case chargeStorage:
				r.ServiceName, r.PublisherName, r.ServiceCategory = focusSandboxService, focusProvider, focusCatStorage
				r.ChargeDescription = "Workspace storage"
			case chargeMemory:
				r.ServiceName, r.PublisherName, r.ServiceCategory = focusSandboxService, focusProvider, focusCatCompute
				r.ChargeDescription = "Sandbox memory"
			default: // cpu
				r.ServiceName, r.PublisherName, r.ServiceCategory = focusSandboxService, focusProvider, focusCatCompute
				r.ChargeDescription = "Sandbox vCPU"
				if c.GetKind() != chargeCPU {
					r.ChargeDescription = "Sandbox compute"
				}
			}
			rows = append(rows, r)
		}
	}
	return rows
}

// modelService returns the FOCUS ServiceName and PublisherName for a model.
func modelService(model string) (service, publisher string) {
	if strings.Contains(strings.ToLower(model), "gemini") {
		return focusGeminiService, "Google"
	}
	return focusModelService, ""
}

// tokenDescription describes a tokens charge; withPrices says whether the
// charge comes from the server, whose charges carry rate card prices.
func tokenDescription(c *v1alpha1.UsageCharge, withPrices bool) string {
	model := c.GetModel()
	if model == "" {
		model = "model"
	}
	desc := model + " tokens"
	switch typ := c.GetTokenType(); typ {
	case "":
	case "input":
		desc = model + " uncached input tokens"
	default:
		desc = fmt.Sprintf("%s %s tokens", model, strings.ReplaceAll(typ, "_", " "))
	}
	if withPrices && c.ListUnitPrice == nil && c.GetQuantity() > 0 {
		desc += " (not on rate card)"
	}
	return desc
}

// fallbackCharges derives charges from a DailyUsage without server-side
// charges: one compute row (vCPU-Hours; its cost includes memory), tokens per
// model (cost split by token count) and storage. The costs still sum to
// cost.total; ListUnitPrice is unknown.
func fallbackCharges(d *v1alpha1.DailyUsage) []*v1alpha1.UsageCharge {
	c := d.GetCost()
	var out []*v1alpha1.UsageCharge
	add := func(u *v1alpha1.UsageCharge) {
		if u.Quantity != 0 || u.Cost != 0 {
			out = append(out, u)
		}
	}
	vcpuHours := d.GetCpuSeconds() / 3600
	add(&v1alpha1.UsageCharge{Kind: "compute", Quantity: vcpuHours, Unit: "vCPU-Hours", PricingQuantity: vcpuHours,
		PricingUnit: "vCPU-Hours", Cost: c.GetCompute(), IdleCost: c.GetIdle()})

	var total int64
	for _, t := range d.GetTokens() {
		total += t.GetInput() + t.GetOutput()
	}
	remaining := c.GetTokens()
	tokens := d.GetTokens()
	for i, t := range tokens {
		n := t.GetInput() + t.GetOutput()
		share := 0.0
		switch {
		case i == len(tokens)-1:
			share = remaining
		case total > 0:
			share = c.GetTokens() * float64(n) / float64(total)
		}
		remaining -= share
		add(&v1alpha1.UsageCharge{Kind: chargeTokens, Model: t.GetModel(), Quantity: float64(n), Unit: "Tokens",
			PricingQuantity: float64(n) / 1e6, PricingUnit: "1M Tokens", Cost: share})
	}
	if len(tokens) == 0 && c.GetTokens() != 0 {
		add(&v1alpha1.UsageCharge{Kind: chargeTokens, Unit: "Tokens", PricingUnit: "1M Tokens", Cost: c.GetTokens()})
	}

	gibMonths := d.GetStorageGibSeconds() / (30 * 86400)
	add(&v1alpha1.UsageCharge{Kind: chargeStorage, Quantity: gibMonths, Unit: "GiB-Months", PricingQuantity: gibMonths,
		PricingUnit: "GiB-Months", Cost: c.GetStorage()})
	return out
}
