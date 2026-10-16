# FOCUS export: column mapping

`ax cost export` writes task usage and cost as [FOCUS](https://focus.finops.org/) (FinOps Open Cost
and Usage Specification) rows. The columns follow FOCUS 1.2. Agent- and token-specific data goes in
`x_` extension columns. This page lists where each column comes from. For the command itself, see
[cli.md](cli.md#ax-cost-export). For how usage is measured and priced, see [finops.md](finops.md).

```
ax cost export [--format focus-csv|json] [-l k=v,...] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [-a <atespace>] [-o <file>]
```

## Source: `ListUsage`

The export calls the `ListUsage` RPC. It returns one `DailyUsage` per task and UTC day in the range.
Each record holds:

- the task's labels and actor uid
- the day's amounts: CPU seconds, memory GiB·s, active and idle seconds, storage GiB·s, tokens per model
- the frozen day cost (`CostBreakdown`)
- `charges`: the day cost split into priced components

ax-server builds the charges from the day bucket of the task's usage record and its current rate card:

| Charge `kind` | Quantity (unit) | Pricing unit | List unit price (rate card) |
|---|---|---|---|
| `cpu` | CPU seconds / 3600 (`vCPU-Hours`) | `vCPU-Hours` | `compute.<pool>.vcpuHour` |
| `memory` | memory GiB·s / 3600 (`GiB-Hours`) | `GiB-Hours` | `compute.<pool>.gibHour` |
| `tokens` | tokens of one model and token type (`Tokens`) | `1M Tokens` | `models.<model>.<type>` |
| `storage` | workspace GiB·s / (30 × 86400) (`GiB-Months`) | `GiB-Months` | `storage.gibMonth` |

Token types do not overlap, so their quantities add up to the billed tokens. Reported counts follow
the OTel GenAI conventions, where input includes cache reads and writes and output includes reasoning.

| Token type | Quantity |
|---|---|
| `input` | input − cache read − cache write (uncached input) |
| `cache_read` | cache read tokens |
| `cache_write` | cache write tokens |
| `output` | output − reasoning |
| `reasoning` | reasoning tokens |

`cache_read` and `cache_write` fall back to the input price when the card has no separate rate, and
`reasoning` falls back to the output price.

**Costs are frozen.** ax-server prices usage when it is recorded and never re-prices it. The day's
`compute`, `tokens` and `storage` costs are split over their charges in proportion to each charge's
cost at current list prices, and the last charge takes the rounding remainder. Two consequences:

- The charge costs of a day always sum to the day's `cost.total`.
- The split is exact while the rate card is unchanged. After a rate card change, or when the harness
  reported a provider cost for a model, it is an allocation of the frozen total.

The idle cost (part of compute) is split the same way over `cpu` and `memory`. Without list-price
weights (no rate card, all prices zero), compute goes to the `cpu` row and token cost is split by
token count.

## Columns

One row per (task, UTC day, charge). Rows with zero quantity and zero cost are omitted. Amounts are
written as the shortest decimal that round-trips, with no rounding, so the sum of `BilledCost` equals
the sum of the tasks' cost totals for the period (tested to 1e-9).

| Column | Value |
|---|---|
| `BillingPeriodStart` / `BillingPeriodEnd` | The calendar month of the charge day, UTC (`2026-10-01T00:00:00Z` / `2026-11-01T00:00:00Z`; the end is exclusive) |
| `ChargePeriodStart` / `ChargePeriodEnd` | The UTC day (`2026-10-16T00:00:00Z` / `2026-10-17T00:00:00Z`) |
| `BilledCost`, `EffectiveCost`, `ListCost`, `ContractedCost` | The charge's priced cost. All four are equal: there are no discounts, commitments or credits. |
| `BillingCurrency` | `CostBreakdown.currency` (`USD`) |
| `ProviderName` | `AX` |
| `PublisherName` | `AX` for sandbox charges, `Google` for Gemini models, empty for other models |
| `ServiceName` | `AX Agent Sandbox` (cpu, memory, storage), `Gemini API` (models whose name contains `gemini`), `Model API` (other models) |
| `ServiceCategory` | `Compute` (cpu, memory), `AI and Machine Learning` (tokens), `Storage` (storage) |
| `ChargeCategory` | `Usage` |
| `ChargeFrequency` | `Usage-Based` |
| `ChargeDescription` | `Sandbox vCPU`, `Sandbox memory`, `Workspace storage`, `<model> <type> tokens`; `(not on rate card)` is appended for unpriced models |
| `SubAccountId` / `SubAccountName` | The atespace |
| `ResourceId` | `<atespace>/<task>` |
| `ResourceName` | The task name |
| `ResourceType` | `AX Task` |
| `ConsumedQuantity` / `ConsumedUnit` | The charge quantity and unit (`vCPU-Hours`, `GiB-Hours`, `Tokens`, `GiB-Months`) |
| `PricingQuantity` / `PricingUnit` | Same as consumed, except tokens: tokens / 1e6 in `1M Tokens` |
| `ListUnitPrice` | The **current** rate card price per pricing unit. It is empty when there is none (a model not on the rate card). Because costs are frozen, `ListUnitPrice × PricingQuantity` can differ from `ListCost` after a rate card change. |
| `Tags` | The task labels as a JSON object (`{"agent":"coder","customer":"acme"}`, `{}` without labels). In `--format json` this is a nested object. |
| `x_Agent` | Task label `agent` |
| `x_Customer` | Task label `customer` |
| `x_Model` | Model name (tokens rows) |
| `x_TokenType` | `input` (uncached), `cache_read`, `cache_write`, `output`, `reasoning` (tokens rows) |
| `x_ChargeKind` | `cpu`, `memory`, `tokens`, `storage` (`compute` for the single-row fallback below) |
| `x_TaskUid` | The actor uid of the task (`status.actorUid`, else the usage record's). It tells apart tasks re-created with the same name. |
| `x_IdleCost` | The part of `BilledCost` consumed while the task was idle (cpu and memory rows). Already included in `BilledCost`. |

### Example

```csv
BillingPeriodStart,BillingPeriodEnd,ChargePeriodStart,ChargePeriodEnd,BilledCost,EffectiveCost,ListCost,ContractedCost,BillingCurrency,ProviderName,PublisherName,ServiceName,ServiceCategory,ChargeCategory,ChargeFrequency,ChargeDescription,SubAccountId,SubAccountName,ResourceId,ResourceName,ResourceType,ConsumedQuantity,ConsumedUnit,PricingQuantity,PricingUnit,ListUnitPrice,Tags,x_Agent,x_Customer,x_Model,x_TokenType,x_ChargeKind,x_TaskUid,x_IdleCost
2026-10-01T00:00:00Z,2026-11-01T00:00:00Z,2026-10-15T00:00:00Z,2026-10-16T00:00:00Z,0.02225,0.02225,0.02225,0.02225,USD,AX,AX,AX Agent Sandbox,Compute,Usage,Usage-Based,Sandbox vCPU,default,default,default/coder-acme,coder-acme,AX Task,0.5,vCPU-Hours,0.5,vCPU-Hours,0.0445,"{""agent"":""coder"",""customer"":""acme""}",coder,acme,,,cpu,uid-coder-acme,0.0037083333333333326
2026-10-01T00:00:00Z,2026-11-01T00:00:00Z,2026-10-15T00:00:00Z,2026-10-16T00:00:00Z,0.032,0.032,0.032,0.032,USD,AX,Google,Gemini API,AI and Machine Learning,Usage,Usage-Based,gemini-3-flash-preview uncached input tokens,default,default,default/coder-acme,coder-acme,AX Task,64000,Tokens,0.064,1M Tokens,0.5,"{""agent"":""coder"",""customer"":""acme""}",coder,acme,gemini-3-flash-preview,input,tokens,uid-coder-acme,0
2026-10-01T00:00:00Z,2026-11-01T00:00:00Z,2026-10-15T00:00:00Z,2026-10-16T00:00:00Z,0.0003333333333333333,0.0003333333333333333,0.0003333333333333333,0.0003333333333333333,USD,AX,AX,AX Agent Sandbox,Storage,Usage,Usage-Based,Workspace storage,default,default,default/coder-acme,coder-acme,AX Task,0.016666666666666666,GiB-Months,0.016666666666666666,GiB-Months,0.02,"{""agent"":""coder"",""customer"":""acme""}",coder,acme,,,storage,uid-coder-acme,0
```

The full sample is [cmd/ax/testdata/cost_export_focus_csv.golden](../cmd/ax/testdata/cost_export_focus_csv.golden).

### Fallback without charges

If a `DailyUsage` carries no `charges`, the CLI derives the rows from the day's amounts:

- one compute row: quantity in `vCPU-Hours`, cost = `cost.compute`, which includes memory
- one tokens row per model, with `cost.tokens` split by token count
- one storage row

`ListUnitPrice` is empty in these rows. `BilledCost` still sums to `cost.total`.

## Not emitted (PoC)

- **Account and invoice columns** (`BillingAccountId`, `BillingAccountName`, `InvoiceIssuerName`, `InvoiceId`): AX has no billing accounts. A consumer can add constants.
- **Pricing and SKU columns** (`ChargeClass`, `PricingCategory`, `SkuId`, `SkuPriceId`, `ServiceSubcategory`): not emitted.
- **Discount and reservation columns** (commitment discounts, capacity reservations): there are none.
- **Location columns** (`RegionId`, `AvailabilityZone`): not emitted.
- **Deleted tasks**: not exported, because their usage record is deleted with the task.
- **Underlying cloud bill**: the export covers AX's attribution of usage to tasks at rate card prices.
  It does not reconcile with the GKE or Gemini bill.
- **Currency**: USD only, as the rate card is.

## FOCUS 1.5 and the `x_` columns

FOCUS 1.2 has no columns for AI agents or token usage. That is why this export uses `x_Agent`,
`x_Model` and `x_TokenType`. Agent and token columns are planned for FOCUS 1.5.

When FOCUS 1.5 is published, we expect these mappings:

- `x_Agent` maps to the agent identifier column. `x_Customer` stays a tag or moves to the
  allocation columns.
- `x_Model` and `x_TokenType` map to the model and token-type columns.
- Token rows already use `ConsumedUnit = Tokens` and `PricingUnit = 1M Tokens`. They should need no
  change beyond the new column names.

Until then, consumers can read the same values from `Tags` and the `x_` columns.
