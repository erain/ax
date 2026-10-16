# FinOps demo agent (Gemini harness)

A small "productivity agent" for the AX FinOps demo. It makes `ITERATIONS`
planning/summarization calls to the Gemini API with the `google-genai` SDK,
writes each answer to `OUT_DIR` (default `./out`, so `/workspace/out` in a
task), reports the token usage of every call to the task runner, prints its own
totals, and then idles so the task shows up as idle.

| File | Purpose |
|---|---|
| `agent.py` | The harness. |
| `agent_test.py` | Unit tests for the usage conversion (`python3 -m unittest agent_test.py`). |
| `requirements.txt` | `google-genai`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp-proto-http`. |
| `Dockerfile`, `cloudbuild.yaml` | Task image: ax-task-runner plus the harness and its dependencies. |
| `task-otlp.yaml`, `task-json.yaml` | Labeled Tasks, one per report mode. |
| `egress-policy.yaml` | Substrate egress policy that allows `generativelanguage.googleapis.com:443`. |

## Configuration

| Env | Default | |
|---|---|---|
| `GEMINI_API_KEY` | (required) | Gemini API key. |
| `MODEL` | `gemini-3-flash-preview` | `gemini-2.5-flash` is no longer served by the Gemini API. |
| `ITERATIONS` | `3` | Number of model calls. |
| `SLEEP_SECONDS` | `5` | Pause between calls. |
| `IDLE_SECONDS` | `600` | Sleep after the last call. |
| `REPORT_MODE` | `otlp` | `otlp` or `json`; `--report=<mode>` overrides it. |
| `OUT_DIR` | `out` | Output directory. |
| `CONTEXT_REPEAT` | `12` | Size of the shared prompt prefix. |

## Usage reporting

The two modes exercise the two reporting paths of
[ax-task-runner](../../docs/runner.md#token-usage-reporting):

- `otlp`: one OpenTelemetry GenAI client span per call (`gen_ai.operation.name=chat`,
  `gen_ai.provider.name=gcp.gemini`, request/response model, usage attributes),
  exported over OTLP/HTTP to `$OTEL_EXPORTER_OTLP_ENDPOINT`, the runner's loopback
  receiver. The calls are children of an `invoke_agent` span that carries the run
  totals; the runner skips usage on aggregate spans, so they are not counted twice.
- `json`: one record per call, POSTed to `$AX_METADATA_URL/metadata/v1alpha1/ax/usage`
  with the Gemini `response_id` as `requestId`, so retries are not counted twice.

Counts follow the OpenTelemetry GenAI convention: input includes cached tokens,
and output includes reasoning tokens. Gemini's `usage_metadata` maps as follows:

| Reported | From `usage_metadata` |
|---|---|
| input | `prompt_token_count` (already includes `cached_content_token_count`) |
| cacheRead | `cached_content_token_count` |
| output | `candidates_token_count + thoughts_token_count` |
| reasoning | `thoughts_token_count` |
| cacheWrite | 0 (Gemini implicit caching has no write charge) |

Usage is keyed by the response model (`model_version`), the same key the runner
uses. After the last call the harness prints one machine-readable line, which is
what validation compares with the runner's `GET /usage`:

```
FINOPS_HARNESS_TOTALS {"byModel": {"gemini-3-flash-preview": {...}}, "cacheRead": 0, "cacheWrite": 0, "calls": 2, "failures": 0, "input": 2563, "mode": "otlp", "model": "gemini-3-flash-preview", "output": 2102, "reasoning": 1550}
```

Only calls that succeed are counted; a call that still fails after retries is
logged, and the harness exits 1 once it has finished idling.

## Image

Build from the repository root. The Dockerfile compiles ax-task-runner in a
golang stage, so no local toolchain or Docker is needed:

```sh
gcloud builds submit --project yiyu-gke-dev --config examples/finops-agent/cloudbuild.yaml \
  --substitutions _IMAGE=us-central1-docker.pkg.dev/yiyu-gke-dev/ax-finops/ax-task-runner,_TAG=finops-$(git rev-parse --short HEAD) .
```

This pushes `:finops-<sha>` and `:finops-latest`.

Substrate v0.5.0 only accepts digest-pinned images in ActorTemplates, so the
task YAMLs here omit `spec.image` and rely on ax-server's
`--default-task-image`, which `deploy/finops/ax-server-patch.yaml` pins to the
image digest. After a rebuild, update that flag to the new digest:

```sh
gcloud artifacts docker images describe \
  us-central1-docker.pkg.dev/yiyu-gke-dev/ax-finops/ax-task-runner:finops-latest \
  --format='value(image_summary.digest)'
```

## Running on a cluster

1. Store the key where the reconciler looks for it. ax-server reads the Secret
   `gemini-api-secret`, key `GEMINI_API_KEY`, in the namespace named after the
   task's atespace, and sets it as `GEMINI_API_KEY` in every task's environment.
   This script reads the key from `$GEMINI_API_KEY` and does not print it:

   ```sh
   deploy/finops/gemini-secret.sh --context <ctx> default
   ```

   Do not put the key in `spec.env`, which takes literal values only and is
   stored with the Task. The reconciler-injected key is also a plain environment
   variable on the per-task ActorTemplate (see SECURITY.md).
2. `ax apply -f examples/finops-agent/task-otlp.yaml`. New tasks start
   suspended: the actor exists, but the harness is not running yet.
3. Allow egress to Gemini for the task's actor, which has the task's name, and
   then resume the task:

   ```sh
   kubectl ate --context <ctx> create egress-policy finops-agent-otlp -a default \
     -f examples/finops-agent/egress-policy.yaml
   ax resume task finops-agent-otlp
   ```

   The policy is per actor and must exist before the harness's first call. The
   egress gateway caches a missing policy for up to about 10s, so create it while
   the task is still suspended.
4. Watch it: `ax get tasks -l agent=finops-agent`, then compare
   `ax get task finops-agent-otlp -o yaml` (status.usage.tokens) with the
   `FINOPS_HARNESS_TOTALS` line in the task log. AX has no `ax logs` command;
   the harness's stdout is in the actor's container log:

   ```sh
   kubectl ate --context <ctx> logs actor finops-agent-otlp -a default --source containers \
     | jq -Rr 'fromjson? | .message // empty' | grep FINOPS_HARNESS_TOTALS
   ```

   The puller fetches the runner's counters every 60s while the task is
   Running, so allow about a minute before comparing. Deleting the task deletes
   the actor and its egress policy.

## End-to-end demo (`demo.sh`)

`demo.sh` runs a small fleet of this harness on a FinOps-enabled cluster and
captures the cost evidence:

- **Fleet:** 6 tasks, agents `summarizer|planner|triage` × fictional customers
  `acme|globex`. The tasks mix `gemini-3-flash-preview`, `gemini-2.5-flash-lite`
  and `gemini-3.1-pro-preview`, and both report modes.
- **Scenarios:** a tight `maxCost` (`BudgetWarning`, then `BudgetExceeded`), an
  idle task (`Idle` plus idle cost), a `SUSPEND_ON_EXIT` task (`Completed`), and
  a task that is suspended mid-run and then resumed.
- **Outputs:** `ax get tasks`, `ax describe task`, `ax cost summary --by
  agent|customer`, a FOCUS CSV and the dashboard PromQL totals. All of them are
  written to `--out DIR`.

```sh
examples/finops-agent/demo.sh                     # about 10 minutes; keeps the tasks
examples/finops-agent/demo.sh --cleanup-only      # delete the demo tasks later
examples/finops-agent/demo.sh --promql-only --out DIR   # re-query the dashboard metrics
```

The demo tasks are kept at the end, Suspended (one is Completed). A suspended
task accrues no compute, but its usage stays visible. Deleting a task deletes
its usage, so it also disappears from `ax cost summary` and `ax cost export`.
The models must be on the deployed rate card (`deploy/ratecard.yaml`).
