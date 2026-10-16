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
| `--usage-router-addr` | `$ATENET_ROUTER_ADDR` | Pull runner usage through the Substrate atenet-router (`GET http://<addr>/usage`, header `ate-target-actor: <atespace>/<task>`). Required on Substrate v0.5.0, whose workers do not expose actor ports on the pod IP. Empty = direct `GET <workerIP>:<runner-port>/usage`. Must be `host:port` (no scheme or path); ax-server refuses to start otherwise. |
| `--summary-interval` | `10m` | How often an `ax.task.usage_summary` line is logged per Running task. Budget and Idle conditions are re-checked on the same tick. `0` disables. |
| `--log-format` | `text` | `text`, or `json`: one JSON object per line, with the level as `severity`. Cloud Logging parses JSON lines into `jsonPayload`, so summaries can be filtered by field. The FinOps deploy uses `json`. |
| `--default-task-image` | `gcr.io/ax-substrate/ate-images/ax-task-runner` | Runner image for tasks without `spec.image`. Substrate v0.5.0 only accepts digest-pinned images (`<repo>@sha256:<digest>`), so on v0.5.0 set this to a digest ref (the built-in default is a tag and is rejected), and any explicit `spec.image` must be one too. A rejected image fails the task with `Ready=False`, reason `ActorTemplateInvalid` and Substrate's message. Other ActorTemplate creation errors fail it with `ActorTemplateCreationFailed`, or leave it `Pending` (`SubstrateUnavailable`) when transient; there is no fallback to the `--template` base template. |
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
- **Router pulls resume suspended actors.** atenet-router (v0.5.0) resumes
  the target actor on every request and has no opt-out, so a pull of a
  suspended actor would undo the suspension. That is why only Running tasks
  with a worker IP are pulled. A sweep works from a task list taken at its
  start, so right before each router request the puller also re-reads the task
  (one task-store GET) and skips the pull unless it is still Running on the
  same worker IP. It also skips it when the usage record's newest actor state
  event is a closing one (suspending, suspended, crashed, deleting, deleted,
  reverting),
  which catches a Substrate-side suspension that the task store has not seen
  yet. Skipped pulls show as `skipped=N` on the `usage puller sweep` log line.
  SuspendTask's flush runs while the task is still Running, so it is not
  skipped. The window that remains is milliseconds (between the re-check and
  the request) plus the ingest latency of state events. Two side effects of
  the state check: after an AX ResumeTask, router pulls are skipped until the
  ingestor records the `resuming`/`running` event or a new-epoch sample; and
  if state events stop arriving while the newest one is a closing state, a
  Running task is not pulled, so its tokens arrive late (on the first pull
  after events resume) but are not lost, because the runner report is
  cumulative.
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
idle: { cpuThreshold: 0.02, after: 5m }         # Idle: < 0.02 vCPU and no new tokens for 5m, or the command exited
budgets: { defaultWarnAtPercent: 80 }
metricLabels: [agent, customer]                 # task labels promoted to metric attributes (ax.label.<key>)
```

**Idle rule.** A task is idle when its cpu rate stays below `idle.cpuThreshold`
for `idle.after`, or when the runner reported that the command exited. An agent
waiting on an LLM call uses almost no cpu (below 0.02 vCPU on the cluster), so
a runner report that brings new tokens (a non-zero token delta) also counts as
activity. The low-cpu streak never starts before the last such report
(`lastTokenActivity` in the usage record): a report restarts a running streak
at its report time. Idle that was already counted is never taken back, so a
report that arrives late does not change past idle seconds; it only delays the
next idle interval. The `Idle` condition uses the same streak, so it turns
`True` exactly when idle time starts to accrue (message `cpu below 0.02 vCPU
and no new tokens for 6m0s; ...`). The report time is when the runner served the
pull, so token activity is dated up to one pull interval late; an agent that
gets an LLM response at least every `idle.after` is never idle.

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
and a `reason` of suspend, delete, threshold, periodic or completed. It carries usage
totals, tokens and the cost breakdown.

### Dashboard

`deploy/finops/dashboard.json` is a Cloud Monitoring dashboard, "AX Agent
FinOps (PoC)". Its PromQL widgets read the metrics above through Managed
Prometheus. On GKE the managed collector writes them through the Telemetry API,
which keeps the dots, so names and labels are quoted:
`sum by ("ax.label.agent") (increase({"ax.task.cost"}[1h]))`. Counter widgets
use `(X unless X offset W) or increase(X[W])`. With plain `increase()`, a series
created inside the window loses its first increment, such as a new agent's
first token cost or a first budget crossing. ax-server also enables the OTel
SDK's per-series start timestamps (`OTEL_GO_X_PER_SERIES_START_TIMESTAMPS=true`,
the default unless set), so ad-hoc `increase()`/`rate()` queries count the
first increment too. A logs panel
shows the summary lines. Its filter matches both `--log-format=json`
(`jsonPayload.msg`) and text (`textPayload`). To create or update the dashboard:

```sh
deploy/finops/apply-dashboard.sh [--project yiyu-gke-dev]
```

## Suspend on completion

A task with `spec.onCompletion: SUSPEND_ON_EXIT` is suspended once its runner
reports `commandExited`. KEEP and unspecified tasks only get `Idle=True`
(reason `CommandExited`).

- **Trigger:** every committed usage write (the puller's pull, or ingest) and
  the periodic summary sweep check the committed record. If the task is
  Running and eligible, one background completion starts per task. The writer
  never waits for the suspend.
- **Completion:** it follows the same path as `SuspendTask`:
  1. Flush, then take the task lock.
  2. Re-check eligibility on fresh reads of the task and the usage record.
  3. Reconcile the suspend.
  4. Set phase `Completed` and condition `Completed=True` with reason
     `SuspendedOnExit`. The message gives the exit code and the runner
     session.
  5. Persist once and log a `reason=completed` summary line.

  Because of the locked re-check, concurrent reports and replicas suspend only
  once. A failed suspend persists nothing. It is retried by a later usage
  write, 30s after the failure at the earliest.
- **Suspend:** `SuspendTask` on a Completed task is a no-op. It returns the
  task unchanged (phase `Completed`, `Completed=True`) without a reconcile.
- **Resume:** a Completed task resumes like a Suspended one. The condition
  becomes `Completed=False` (reason `Resumed`) and keeps the completed runner
  session. That session is never completed again. On gVisor, resume restores
  the checkpointed process tree, so the runner keeps its session and keeps
  reporting `commandExited`. The task then stays Running with
  `Idle=True/CommandExited` until it is suspended or deleted. If a new runner
  session starts (a fresh container) and its command exits, the task completes
  again.
