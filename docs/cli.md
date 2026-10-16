# CLI: usage and cost

ax-server records the usage of every task (compute from Agent Substrate, model tokens reported by the
harness through the runner) and prices it with a rate card. The result is part of the task itself,
under `status.usage`, so `ax get task <name>` (YAML) already shows it. The commands below format it.

`ax get task <name> -o json` prints the same object as JSON (protojson: same field names as the
YAML; 64-bit integers such as `maxTokens` or token counts are JSON strings). `-o yaml` is the default.
`ax get tasks -o json|yaml` prints the list (`{"tasks": [...]}`) instead of the table.

Money is printed with 4 decimals below $1 (`$0.0123`) and 2 decimals from $1 up (`$1.23`).

## `ax get tasks`

The last column, `COST`, is the task's total cost so far (`-` until the task has usage).
`--show-labels` adds a `LABELS` column, and `-l k=v,...` filters by labels.

```console
$ ax get tasks --show-labels
NAME            AGENT      ATESPACE   PHASE       ACTOR           WORKER-IP    AGE   COST      LABELS
coder-acme      coder      default    Running     coder-acme      10.20.3.67   40m   $0.1100   agent=coder,customer=acme
coder-globex    coder      default    Suspended   coder-globex    <none>       3h    $1.23     agent=coder,customer=globex
reviewer-acme   reviewer   default    Running     reviewer-acme   10.20.3.68   1m    $0.0042   agent=reviewer,customer=acme
unlabeled       -          default    Pending     <none>          <none>       1d    -         <none>
```

## `ax describe task <name>`

Shows the task's labels, its budget (if `spec.budget` is set), usage, cost breakdown, and
conditions:

- **Usage:** active and idle time, CPU seconds, memory GiB·s, workspace size, and tokens per model.
  Token counts follow the OpenTelemetry GenAI convention: `INPUT` includes cache reads and writes,
  and `OUTPUT` includes reasoning tokens.
- **Cost:** `Compute` (of which idle), `Tokens`, `Storage`, and `Total` (= compute + tokens +
  storage). When some model is not on the rate card, its tokens are counted but not priced. The
  tokens cost is then marked `*` with a note.
- **Conditions:** includes the FinOps conditions `BudgetWarning`, `BudgetExceeded`, and `Idle`.

```console
$ ax describe task coder-acme
Name:         coder-acme
Atespace:     default
Labels:       agent=coder
              customer=acme
Phase:        Running
Actor:        coder-acme
Worker IP:    10.20.3.67
Image:        us-docker.pkg.dev/demo/ax-task-runner:finops
Command:      [python3 /opt/finops-agent/main.py]
Budget:
  Active Deadline:  30m0s
  Max Tokens:       100,000
  Max Cost:         $1.00
  Warn At:          80%
  On Exceeded:      WARN
Usage:
  Active:           25m12s
  Idle:             6m0s
  CPU:              812.35 s
  Memory:           1520.50 GiB·s
  Workspace:        12.3 MiB
  Updated:          2026-10-16T11:59:30Z
  Tokens:
    MODEL                    INPUT    OUTPUT   CACHE-READ   CACHE-WRITE   REASONING
    gemini-3-flash-preview   85,000   3,000    20,000       1,000         1,200
    some-unpriced-model      500      100      0            0             0
Cost:
  Compute:          $0.0187
    of which idle:  $0.0031
  Tokens:           $0.0912 *
  Storage:          $0.0001
  Total:            $0.1100
  * tokens partially unpriced: some models are not on the rate card; their tokens are counted but not priced

Conditions:
  TYPE           STATUS  REASON                 MESSAGE
  Ready          True    ActorRunning           
  BudgetWarning  True    ActiveDeadlineWarning  active 25m12s of 30m0s (84%); tokens 88,600 of 100,000 (88%)
  Idle           True    LowCPU                 cpu below 0.02 vCPU and no new tokens for 6m0s; idle 6m0s total, idle cost $0.0031
```

## `ax cost summary`

```
ax cost summary [--by <label>] [-l k=v,...] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [-a <atespace>] [-o table|json]
```

Rolls up usage and cost by the value of a task label over the UTC days `--start` to `--end`
(inclusive). The default is the 30 days ending today. `--by` defaults to `agent`, and tasks without
the label are grouped as `<none>`. Groups are sorted by total cost, highest first, followed by a
`TOTAL` row. `TOKENS` is input + output tokens. `TOKENS$` is the token cost.

```console
$ ax cost summary
GROUP      TASKS   ACTIVE     IDLE    TOKENS    COMPUTE   TOKENS$   STORAGE   TOTAL
coder      2       2h30m12s   16m0s   548,600   $0.2287   $1.11*    $0.0046   $1.34
reviewer   1       1m20s      0s      9,400     $0.0003   $0.0039   $0.0000   $0.0042
<none>     1       0s         0s      0         $0.0000   $0.0000   $0.0000   $0.0000
TOTAL      4       2h31m32s   16m0s   558,000   $0.2290   $1.12*    $0.0046   $1.35
* tokens partially unpriced: some models are not on the rate card; their tokens are counted but not priced

$ ax cost summary --by customer -l agent=coder
GROUP    TASKS   ACTIVE     IDLE    TOKENS    COMPUTE   TOKENS$    STORAGE   TOTAL
globex   1       2h5m0s     10m0s   460,000   $0.2100   $1.02      $0.0045   $1.23
acme     1       25m12s     6m0s    88,600    $0.0187   $0.0912*   $0.0001   $0.1100
TOTAL    2       2h30m12s   16m0s   548,600   $0.2287   $1.11*     $0.0046   $1.34
* tokens partially unpriced: some models are not on the rate card; their tokens are counted but not priced
```

`-o json` prints the same data with unrounded amounts. Seconds are rounded to milliseconds and money
to micro-units:

```console
$ ax cost summary -o json | jq '.groups[0]'
{
  "group": "coder",
  "tasks": 2,
  "activeSeconds": 9012,
  "idleSeconds": 960,
  "tokens": 548600,
  "cost": { "compute": 0.2287, "idle": 0.0151, "tokens": 1.1112, "storage": 0.0046, "total": 1.3445, "tokensPartiallyUnpriced": true }
}
```

The summary reads per-day usage from the `ListUsage` API, so `TASKS` counts the tasks with usage in
the period, and a `Period:` line follows the table (`start` and `end` in JSON, with `"source": "usage"`).
Against a server without `ListUsage` (an older server, or one with usage accounting disabled), it falls
back to each task's lifetime usage from `ListTasks` (`"source": "tasks"`). In that case `--start` and
`--end` are ignored, with a warning. Deleted tasks are not included, because their usage is deleted with
them.

## `ax cost export`

```
ax cost export [--format focus-csv|json] [-l k=v,...] [--start YYYY-MM-DD] [--end YYYY-MM-DD] [-a <atespace>] [-o <file>]
```

Writes [FOCUS](https://focus.finops.org/) cost and usage rows, with one row per task, UTC day and charge:

- vCPU and memory (`Compute`)
- tokens per model and token type (`AI and Machine Learning`)
- workspace storage (`Storage`)

The sum of `BilledCost` equals the tasks' cost totals for the period. The period and `-l` work as
for `summary`. `--format json` writes the same rows as a JSON array. `-o` writes to a file instead of
stdout. The column mapping is in [finops-focus.md](finops-focus.md).

```console
$ ax cost export --start 2026-10-01 -o october.csv
$ ax cost export -l customer=acme --format json | jq 'group_by(.x_Agent) | map({agent: .[0].x_Agent, cost: (map(.BilledCost) | add)})'
```

Export needs `ListUsage`. Against an older server it fails.
