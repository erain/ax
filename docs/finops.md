# FinOps: usage, cost, budgets and metrics

ax-server attributes compute and token usage to each Task, prices it with a
rate card, raises soft-budget and idle conditions, and exports OTel metrics and
per-task summary logs. This is a PoC feature.

## ax-server flags

| Flag | Default | Meaning |
|---|---|---|
| `--otlp-addr` | `:4318` | OTLP/HTTP listener for Substrate usage events (`POST /v1/logs`, plus `GET /healthz`). Empty disables the ingestor. |
| `--rate-card` | (empty) | Rate card YAML path. The file is re-read when it changes (checked at most every 10s), and an invalid edit keeps the previous card. Empty uses a built-in card that prices nothing (usage is still recorded; tokens are flagged partially unpriced). |
| `--usage-pull-interval` | `60s` | How often runner token usage is pulled from Running tasks. `0` disables periodic pulls; Suspend/Delete still flush. |
| `--runner-port` | `80` | Port of the task runner's `GET /usage` (direct pulls only). |
| `--usage-router-addr` | `$ATENET_ROUTER_ADDR` | Pull runner usage through the Substrate atenet-router (`GET http://<addr>/usage`, header `ate-target-actor: <atespace>/<task>`). Required on Substrate v0.5.0, whose workers do not expose actor ports on the pod IP. Empty = direct `GET <workerIP>:<runner-port>/usage`. |
| `--summary-interval` | `10m` | How often an `ax.task.usage_summary` line is logged per Running task. Budget and Idle conditions are re-checked on the same tick. `0` disables. |
| `--default-task-image` | `gcr.io/ax-substrate/ate-images/ax-task-runner` | Runner image for tasks without `spec.image`. |
| `--redis-addr` | `localhost:6379` | Task store, locks and usage store (`ax:usage:{atespace}:{name}`) share this Redis. Empty uses in-memory stores: a single replica for development only. |

Metrics export uses the standard OTel environment variables. When
`OTEL_EXPORTER_OTLP_ENDPOINT` (or `..._METRICS_ENDPOINT`) is set, metrics are
exported over OTLP/HTTP every 30s (`OTEL_METRIC_EXPORT_INTERVAL`, in ms) with
cumulative temporality. Otherwise metrics are off. On GKE, also set
`OTEL_RESOURCE_ATTRIBUTES=k8s.pod.ip=$(POD_IP),k8s.pod.name=$(POD_NAME),k8s.namespace.name=$(POD_NAMESPACE)`,
because metrics reach the managed collector through the sidecar collector.

## Data flow

```
Substrate ateapi/ateom --OTLP--> otel-collector --(filtered ate.actor.* logs)--> ax-server :4318 /v1/logs
                                                                                 (CPU, memory, active/idle time)
task runner GET /usage (via atenet-router, or <workerIP>:<runner-port>)  <--pull every 60s + on Suspend/Delete-- ax-server
                                                                                 (tokens per model, workspace bytes)
                     both --> usage.Store.Update (accounting + pricing, per UTC day)
                               ├─ hooks (non-zero deltas): OTel metrics + budget check
                               └─ every committed write (also zero-delta state changes): budget check
GetTask / ListTasks / WatchTask: status.usage, status.actorUid and fresh BudgetWarning /
BudgetExceeded / Idle conditions are merged at read time (ListTasks does one usage List per request).
```

- Usage is only recorded for actors that match an existing AX task (the actor
  is named after the task). `status.actorUid` comes from Substrate (the
  EnsureActor/ResumeActor response). When the uid changes, the ingestor resets
  the usage record, for example when a task is deleted and re-created.
- Budgets (`spec.budget`: `activeDeadline`, `maxTokens`, `maxCost`,
  `warnAtPercent`) are soft (WARN only). A check first evaluates without a
  lock. Only when a condition transition is due does it take the task lock,
  re-read the task and the usage record, and persist the conditions. Each
  crossing is reported once: an `ax.budget.threshold_crossed` metric, a
  `budget threshold crossed` log line and a `reason=threshold` summary line.
  Suspend and Resume also re-evaluate, because Idle depends on the phase.
- SuspendTask and DeleteTask first pull the runner's latest usage (Running
  tasks only). This happens before taking the task lock, because the hooks
  the pull triggers take that lock. Then they log a `reason=suspend` or
  `reason=delete` summary. Delete order: summary, then the task record, then
  the usage record. Late events for the deleted task then find no task and are
  dropped.
- Lock order: the task lock may be held while calling the task store and the
  usage store's Get/List/Delete. It is never held while calling usage
  `Update`, the puller, or usage hooks.

## Rate card

See `deploy/ratecard.yaml`. Mount it as a ConfigMap and pass `--rate-card`.

```yaml
currency: USD                                   # PoC: USD only
compute: { default: { vcpuHour: 0.0445, gibHour: 0.0049 }, pools: {} }
storage: { gibMonth: 0.020 }                    # GiB-month = 30 days
models:                                         # USD per 1M tokens
  gemini-2.5-flash: { input: 0.30, output: 2.50, cacheRead: 0.03 }
idle: { cpuThreshold: 0.02, after: 5m }         # Idle: < 0.02 vCPU for 5m, or the command exited
budgets: { defaultWarnAtPercent: 80 }
metricLabels: [agent, customer]                 # task labels promoted to metric attributes (ax.label.<key>)
```

Token counts follow the OTel GenAI conventions: input includes cache
read/write tokens, and output includes reasoning tokens. A provider-reported
cost overrides the card. A model with tokens but no entry in the card sets
`cost.tokensPartiallyUnpriced`. Costs are frozen when they are recorded, so a
rate-card change only affects new usage.

## Metrics and logs

Metrics (no per-task attributes; common attributes are `ax.atespace` and
`ax.label.<key>`):

- `ax.task.cost` (`{USD}`, attribute `ax.cost.kind` = compute|idle|tokens|storage;
  the kinds are disjoint and sum to the total)
- `ax.task.tokens` (attributes `gen_ai.request.model`, `gen_ai.token.type`)
- `ax.task.cpu.time`, `ax.task.active.time`, `ax.task.idle.time`
- `ax.budget.threshold_crossed`

Each per-task summary is one slog line with `msg="ax.task.usage_summary"`
and a `reason` of suspend, delete, threshold or periodic. It carries usage
totals, tokens and the cost breakdown.
