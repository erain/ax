#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# AX Agent FinOps end-to-end demo.
#
# Runs a small fleet of labeled Gemini-calling tasks (3 agents x 2 fictional
# customers) on an AX + Agent Substrate cluster with FinOps enabled
# (deploy/finops), drives the interesting cost scenarios, and captures the
# CLI, FOCUS export and dashboard (PromQL) evidence into an output directory.
#
#   task                      model                    report  scenario
#   <p>-summarizer-acme       gemini-3-flash-preview   otlp    tight maxCost -> BudgetWarning, then BudgetExceeded
#   <p>-summarizer-globex     gemini-2.5-flash-lite    json    onCompletion SUSPEND_ON_EXIT -> Completed
#   <p>-planner-acme          $PRO_MODEL               otlp    pro model; idles after its calls
#   <p>-planner-globex        gemini-3-flash-preview   json    calls, then idles -> Idle + idle cost
#   <p>-triage-acme           gemini-3-flash-preview   json    suspended mid-run, then resumed (the
#                                                              command re-runs: Substrate v0.5.0
#                                                              resume restores the filesystem only)
#   <p>-triage-globex         $PRO_MODEL               otlp    pro model, generous budget (no crossing)
#
# Every task is labeled agent=<agent>, customer=<customer>, demo=<prefix>.
#
# Steps: build the ax CLI; apply the Gemini Secret; start a self-healing
# port-forward; create the tasks (Suspended); create one per-actor egress
# policy each (Gemini API only); wait out the gateway's negative cache; resume
# them all; poll until every scenario is observed (suspending/resuming the
# triage task on the way); suspend whatever still runs; let late usage events
# settle; capture outputs; wait for the metrics pipeline and query the
# dashboard's PromQL.
#
# End state (default): all demo tasks are kept, Suspended (or Completed), so
# their usage and cost stay visible (`ax get tasks -l demo=<prefix>`), and a
# suspended task accrues no compute cost. Usage is deleted with the task, so
# deleting the tasks also removes them from `ax cost summary`/`export`.
#
# Usage:
#   examples/finops-agent/demo.sh [--prefix demo] [--out DIR] [--cleanup] [--no-promql]
#   examples/finops-agent/demo.sh --cleanup-only [--prefix demo]
#   examples/finops-agent/demo.sh --promql-only --out DIR
#
#   --prefix P       task name prefix and value of the demo=<P> label (default "demo")
#   --out DIR        output directory (default /tmp/ax-finops-demo-<prefix>-<time>)
#   --cleanup        delete the demo tasks after capturing the outputs
#   --cleanup-only   only delete existing tasks labeled demo=<prefix>, then exit
#   --no-promql      skip the Cloud Monitoring PromQL step
#   --promql-only    only (re)run the PromQL step for an existing --out DIR
#                    (window from DIR/start.txt; CLI total from DIR/cost-summary-agent.json)
#
# Env: KUBE_CONTEXT (gke_yiyu-gke-dev_us-central1-a_ax-finops-poc), ATESPACE
#      (default), AX_SERVER (use this address instead of a port-forward),
#      PF_PORT (18418), KUBECTL_ATE (/tmp/substrate-bin/kubectl-ate),
#      FLASH_MODEL (gemini-3-flash-preview), LITE_MODEL (gemini-2.5-flash-lite),
#      PRO_MODEL (gemini-3.1-pro-preview), PROJECT (yiyu-gke-dev),
#      DASHBOARD_ID, DEMO_TIMEOUT_SECONDS (1500), SETTLE_SECONDS (90),
#      PROMQL_TIMEOUT_SECONDS (420), GEMINI_API_KEY (else read via `zsh -ic`;
#      never printed).
# Needs: go, kubectl, kubectl-ate (Substrate v0.5.0), jq, perl, curl, gcloud
#        (PromQL only; uses application-default credentials).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CTX="${KUBE_CONTEXT:-gke_yiyu-gke-dev_us-central1-a_ax-finops-poc}"
ATESPACE="${ATESPACE:-default}"
PF_PORT="${PF_PORT:-18418}"
KUBECTL_ATE="${KUBECTL_ATE:-/tmp/substrate-bin/kubectl-ate}"
FLASH_MODEL="${FLASH_MODEL:-gemini-3-flash-preview}"
LITE_MODEL="${LITE_MODEL:-gemini-2.5-flash-lite}"
PRO_MODEL="${PRO_MODEL:-gemini-3.1-pro-preview}"
PROJECT="${PROJECT:-yiyu-gke-dev}"
DASHBOARD_ID="${DASHBOARD_ID:-5d98b08a-b202-41f6-84bd-b17b8c1c6e84}"
DEMO_TIMEOUT_SECONDS="${DEMO_TIMEOUT_SECONDS:-1500}"
SETTLE_SECONDS="${SETTLE_SECONDS:-90}"
PROMQL_TIMEOUT_SECONDS="${PROMQL_TIMEOUT_SECONDS:-420}"
SUSPEND_PAUSE_SECONDS=45

PREFIX=demo
OUT=""
CLEANUP=0
CLEANUP_ONLY=0
PROMQL=1
PROMQL_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --cleanup) CLEANUP=1; shift ;;
    --cleanup-only) CLEANUP_ONLY=1; shift ;;
    --no-promql) PROMQL=0; shift ;;
    --promql-only) PROMQL_ONLY=1; shift ;;
    -h|--help) sed -n '17,/^set -euo/p' "$0" | sed '$d'; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done
OUT="${OUT:-/tmp/ax-finops-demo-$PREFIX-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"
WORK="$(mktemp -d /tmp/ax-finops-demo.XXXXXX)"
TIMELINE="$OUT/timeline.log"

T_BUDGET="$PREFIX-summarizer-acme"
T_DONE="$PREFIX-summarizer-globex"
T_PRO1="$PREFIX-planner-acme"
T_IDLE="$PREFIX-planner-globex"
T_SR="$PREFIX-triage-acme"
T_PRO2="$PREFIX-triage-globex"
TASKS="$T_BUDGET $T_DONE $T_PRO1 $T_IDLE $T_SR $T_PRO2"

say() { printf '%s ==> %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf '%s FAIL: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }
event() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$TIMELINE"; }
# Bound every cluster call: a dropped port-forward can otherwise hang forever.
tmo() { local s=$1; shift; perl -e 'alarm shift; exec @ARGV or die "exec: $!"' "$s" "$@"; }
ate() { tmo 60 "$KUBECTL_ATE" --context "$CTX" "$@"; }
AX_BIN="$WORK/ax"
SERVER="${AX_SERVER:-127.0.0.1:$PF_PORT}"
ax() { tmo 60 "$AX_BIN" --context "$CTX" --server "$SERVER" -a "$ATESPACE" "$@"; }

PF_LOOP=""
start_pf() {
  [ -n "${AX_SERVER:-}" ] && return 0
  if nc -z 127.0.0.1 "$PF_PORT" 2>/dev/null; then die "localhost:$PF_PORT already in use (set PF_PORT)"; fi
  # Restart loop: kubectl port-forward drops now and then; re-establish it.
  ( while :; do
      kubectl --context "$CTX" -n ax-system port-forward --address 127.0.0.1 svc/ax-server "$PF_PORT:8080" \
        >>"$WORK/pf.log" 2>&1 || true
      sleep 1
    done ) &
  PF_LOOP=$!
  disown "$PF_LOOP" 2>/dev/null || true # no "Terminated" job message on exit
  for _ in $(seq 1 30); do nc -z 127.0.0.1 "$PF_PORT" 2>/dev/null && return 0; sleep 1; done
  die "port-forward did not come up: $(tail -3 "$WORK/pf.log")"
}
kick_pf() { [ -n "$PF_LOOP" ] && pkill -P "$PF_LOOP" kubectl 2>/dev/null || true; sleep 2; }
stop_pf() {
  if [ -n "$PF_LOOP" ]; then
    local kids
    kids="$(pgrep -P "$PF_LOOP" || true)"
    kill "$PF_LOOP" 2>/dev/null || true
    # shellcheck disable=SC2086
    [ -z "$kids" ] || kill $kids 2>/dev/null || true
    wait "$PF_LOOP" 2>/dev/null || true
    PF_LOOP=""
  fi
}
# ax_retry <outfile> <ax args...>: read-only call with retries (restarts the
# port-forward between attempts).
ax_retry() {
  local out=$1; shift
  for _ in 1 2 3 4 5 6; do
    if ax "$@" >"$out.tmp" 2>"$WORK/ax.err"; then mv "$out.tmp" "$out"; return 0; fi
    say "ax $* failed ($(head -c 160 "$WORK/ax.err" | tr '\n' ' ')); retrying"
    kick_pf
  done
  return 1
}
cleanup_exit() {
  local rc=$?
  stop_pf
  rm -rf "$WORK"
  exit $rc
}
trap cleanup_exit EXIT

# run_promql: query the dashboard's metrics (Managed Prometheus) over
# [demo start - 5m, now] for the demo agents, wait until they catch up with the
# CLI total (or PROMQL_TIMEOUT_SECONDS), and write $OUT/promql.json.
run_promql() {
  local TOTAL TOK AGENTS COST TOKS CROSS ACTIVE IDLE W got pdeadline first q
  TOTAL="$(jq '.total.cost.total' "$OUT/cost-summary-agent.json")"
  TOK="$(gcloud auth application-default print-access-token 2>/dev/null)" ||
    die "no ADC token (gcloud auth application-default login)"
  promql() { # promql <query> -> JSON response
    curl -sS -m 30 -H "Authorization: Bearer $TOK" -H "x-goog-user-project: $PROJECT" \
      --data-urlencode "query=$1" \
      "https://monitoring.googleapis.com/v1/projects/$PROJECT/location/global/prometheus/api/v1/query"
  }
  # Selectors live in variables: an inline "{a,b}" inside "$(...)" is
  # brace-expanded by bash 3.2.
  AGENTS='"ax.label.agent"=~"summarizer|planner|triage"'
  COST="{\"ax.task.cost\",$AGENTS}"
  TOKS="{\"ax.task.tokens\",$AGENTS}"
  CROSS="{\"ax.budget.threshold_crossed\",$AGENTS}"
  ACTIVE="{\"ax.task.active.time\",$AGENTS}"
  IDLE="{\"ax.task.idle.time\",$AGENTS}"
  # Counter over the window: series born inside the window count fully,
  # older series by increase() (same form as deploy/finops/dashboard.json).
  win() { echo "(($1 unless $1 offset ${W}s) or increase($1[${W}s]))"; }
  pdeadline=$(( $(date +%s) + PROMQL_TIMEOUT_SECONDS ))
  say "waiting for Cloud Monitoring to catch up with \$$TOTAL (metrics export every 30s + ingestion lag)"
  while :; do
    W=$(( $(date +%s) - START_EPOCH + 300 ))
    promql "sum($(win "$COST"))" >"$WORK/pq-total.json" || true
    got="$(jq -r '.data.result[0].value[1] // "0"' "$WORK/pq-total.json" 2>/dev/null || echo 0)"
    say "PromQL total \$$got vs ax cost summary \$$TOTAL"
    if jq -e -n --argjson g "$got" --argjson t "$TOTAL" '$g >= 0.97 * $t' >/dev/null ||
      [ "$(date +%s)" -ge "$pdeadline" ]; then
      break
    fi
    sleep 30
  done
  {
    printf '{"window_seconds":%s,"time":"%s","cli_total":%s,"queries":[' "$W" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$TOTAL"
    first=1
    for q in "sum($(win "$COST"))" \
      "sum by (\"ax.label.agent\") ($(win "$COST"))" \
      "sum by (\"ax.label.customer\") ($(win "$COST"))" \
      "sum by (\"ax.cost.kind\") ($(win "$COST"))" \
      "sum by (\"gen_ai.request.model\", \"gen_ai.token.type\") ($(win "$TOKS"))" \
      "sum by (\"ax.label.agent\", \"ax.budget.dimension\", \"ax.budget.level\") ($(win "$CROSS"))" \
      "sum($(win "$ACTIVE"))" \
      "sum($(win "$IDLE"))"; do
      [ $first = 1 ] || printf ','
      first=0
      printf '{"query":%s,"result":%s}' "$(jq -Rn --arg q "$q" '$q')" "$(promql "$q" | jq -c '.data.result // []')"
    done
    printf ']}\n'
  } >"$OUT/promql.json"
  say "PromQL results -> $OUT/promql.json"
  jq -r '.queries[] | "  \(.query | .[0:100])...\n" + ([.result[] | "      \(.metric | to_entries | map("\(.key)=\(.value)") | join(",")) = \(.value[1])"] | join("\n"))' "$OUT/promql.json"
  say "dashboard: https://console.cloud.google.com/monitoring/dashboards/builder/$DASHBOARD_ID?project=$PROJECT"
}

delete_demo_tasks() {
  ax_retry "$WORK/list.json" get tasks -l "demo=$PREFIX" -o json || die "listing demo tasks"
  for t in $(jq -r '.tasks // [] | .[].metadata.name' "$WORK/list.json"); do
    say "deleting task $t"
    ax delete task "$t" >/dev/null || echo "WARN: delete $t failed" >&2
  done
}

if [ "$PROMQL_ONLY" = 1 ]; then
  [ -f "$OUT/start.txt" ] && [ -f "$OUT/cost-summary-agent.json" ] || die "--promql-only needs --out DIR from an earlier run"
  START_EPOCH=$(date -j -u -f %Y-%m-%dT%H:%M:%SZ "$(cat "$OUT/start.txt")" +%s 2>/dev/null ||
    date -u -d "$(cat "$OUT/start.txt")" +%s)
  PROMQL_TIMEOUT_SECONDS=0 run_promql
  exit 0
fi

# --- prerequisites ------------------------------------------------------------
command -v jq >/dev/null || die "jq missing"
[ -x "$KUBECTL_ATE" ] || die "kubectl-ate not found at $KUBECTL_ATE"
(cd "$REPO" && go build -o "$AX_BIN" ./cmd/ax) || die "building the ax CLI"
start_pf

if [ "$CLEANUP_ONLY" = 1 ]; then
  delete_demo_tasks
  exit 0
fi

say "context $CTX, atespace $ATESPACE, prefix $PREFIX, output $OUT"
say "models: flash=$FLASH_MODEL lite=$LITE_MODEL pro=$PRO_MODEL"
tmo 90 kubectl --context "$CTX" -n ax-system rollout status deployment/ax-server --timeout=60s >/dev/null ||
  die "ax-server is not ready"
ax_retry "$WORK/list.json" get tasks -l "demo=$PREFIX" -o json || die "ax-server unreachable"
[ "$(jq '.tasks // [] | length' "$WORK/list.json")" = 0 ] ||
  die "tasks labeled demo=$PREFIX already exist; run with --cleanup-only first or pick another --prefix"

# The rate card must price every model the demo calls.
tmo 60 kubectl --context "$CTX" -n ax-system get configmap ax-ratecard -o jsonpath='{.data.ratecard\.yaml}' \
  >"$OUT/ratecard.yaml"
for m in "$FLASH_MODEL" "$LITE_MODEL" "$PRO_MODEL"; do
  grep -q "^  $m:" "$OUT/ratecard.yaml" || die "model $m is not on the deployed rate card (deploy/ratecard.yaml)"
done

say "applying Secret $ATESPACE/gemini-api-secret (GEMINI_API_KEY; not shown)"
if [ -n "${GEMINI_API_KEY:-}" ]; then
  (cd "$REPO" && deploy/finops/gemini-secret.sh --context "$CTX" "$ATESPACE" </dev/null >/dev/null)
else
  (cd "$REPO" && zsh -ic "deploy/finops/gemini-secret.sh --context '$CTX' '$ATESPACE'" </dev/null >/dev/null 2>&1)
fi || die "gemini-secret.sh failed (export GEMINI_API_KEY)"

# --- the fleet ------------------------------------------------------------------
# task_yaml <name> <agent> <customer> <model> <mode> <iterations> <sleep> <idle> <context-repeat> [extra spec yaml]
task_yaml() {
  cat <<EOF
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: $1
  atespace: $ATESPACE
  labels:
    agent: $2
    customer: $3
    demo: $PREFIX
spec:
  command: ["python3", "/opt/finops-agent/agent.py"]
  env:
    - {name: MODEL, value: "$4"}
    - {name: REPORT_MODE, value: "$5"}
    - {name: ITERATIONS, value: "$6"}
    - {name: SLEEP_SECONDS, value: "$7"}
    - {name: IDLE_SECONDS, value: "$8"}
    - {name: CONTEXT_REPEAT, value: "$9"}
${10:-}
EOF
}
BUDGET_SPEC='  budget:
    maxCost: {currency: USD, amount: 0.01}
    warnAtPercent: 50
    onExceeded: WARN'
GENEROUS_SPEC='  budget:
    maxCost: {currency: USD, amount: 1.00}
    activeDeadline: 2h
    onExceeded: WARN'
# Calls 75s apart (longer than the 60s usage pull) so the budget task crosses
# 50% (warning) on one pull and 100% (exceeded) on a later one.
task_yaml "$T_BUDGET" summarizer acme "$FLASH_MODEL" otlp 5 75 1800 12 "$BUDGET_SPEC" >"$WORK/$T_BUDGET.yaml"
task_yaml "$T_DONE" summarizer globex "$LITE_MODEL" json 4 5 0 12 '  onCompletion: SUSPEND_ON_EXIT' >"$WORK/$T_DONE.yaml"
task_yaml "$T_PRO1" planner acme "$PRO_MODEL" otlp 2 10 1800 12 >"$WORK/$T_PRO1.yaml"
task_yaml "$T_IDLE" planner globex "$FLASH_MODEL" json 3 5 1800 12 >"$WORK/$T_IDLE.yaml"
task_yaml "$T_SR" triage acme "$FLASH_MODEL" json 4 30 1800 24 >"$WORK/$T_SR.yaml"
task_yaml "$T_PRO2" triage globex "$PRO_MODEL" otlp 3 5 1800 12 "$GENEROUS_SPEC" >"$WORK/$T_PRO2.yaml"

for t in $TASKS; do
  cp "$WORK/$t.yaml" "$OUT/task-$t.yaml"
  ax apply -f "$WORK/$t.yaml" || die "apply $t"
  # Substrate egress policies are per actor (actor name = task name); AX
  # creates tasks Suspended, so the actor exists and has not run yet.
  ate create egress-policy "$t" -a "$ATESPACE" -f "$REPO/examples/finops-agent/egress-policy.yaml" >/dev/null ||
    die "egress policy for $t"
  event "created $t (Suspended) + egress policy generativelanguage.googleapis.com:443"
done
sleep 11 # the egress gateway caches a missing policy for ~10s
START_EPOCH=$(date +%s)
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$START_UTC" >"$OUT/start.txt"
for t in $TASKS; do
  ax resume task "$t" >/dev/null || die "resume $t"
done
event "resumed all 6 tasks"

# --- drive and observe the scenarios ---------------------------------------------
jq_task() { jq -r --arg t "$1" ".tasks // [] | map(select(.metadata.name == \$t))[0] | $2" "$WORK/tasks.json"; }
cond() { jq_task "$1" "((.status.conditions // []) | map(select(.type == \"$2\"))[0]) as \$c | if \$c then \"\(\$c.status)/\(\$c.reason)\" else \"-\" end"; }
tokens_of() { jq_task "$1" '[.status.usage.tokens // [] | .[] | ((.input // "0") | tonumber) + ((.output // "0") | tonumber)] | add // 0'; }

SR_STATE=0 # 0 running first time, 1 suspended, 2 resumed, 3 resumed run reported tokens
SR_TOKENS=0
SR_SUSPEND_AT=0
deadline=$((START_EPOCH + DEMO_TIMEOUT_SECONDS))
while :; do
  sleep 20
  ax_retry "$WORK/tasks.json" get tasks -l "demo=$PREFIX" -o json || { say "poll failed"; continue; }
  # Transitions (phase + condition states) go to the timeline.
  for t in $TASKS; do
    s="phase=$(jq_task "$t" '.status.phase // "-"') warn=$(cond "$t" BudgetWarning) exceeded=$(cond "$t" BudgetExceeded)"
    s="$s idle=$(cond "$t" Idle) completed=$(cond "$t" Completed)"
    if [ "$s" != "$(cat "$WORK/state.$t" 2>/dev/null || true)" ]; then
      echo "$s" >"$WORK/state.$t"
      event "$t $s cost=\$$(jq_task "$t" '.status.usage.cost.total // 0') tokens=$(tokens_of "$t")"
    fi
  done
  now=$(date +%s)
  case $SR_STATE in
    0) if [ "$(tokens_of "$T_SR")" -gt 0 ] && [ "$(jq_task "$T_SR" '.status.phase')" = Running ]; then
         SR_TOKENS=$(tokens_of "$T_SR")
         ax suspend task "$T_SR" >/dev/null || die "suspend $T_SR"
         SR_SUSPEND_AT=$now; SR_STATE=1
         event "SUSPEND $T_SR mid-run (tokens so far $SR_TOKENS)"
       fi ;;
    1) if [ $((now - SR_SUSPEND_AT)) -ge "$SUSPEND_PAUSE_SECONDS" ]; then
         ax_retry "$WORK/sr.json" get task "$T_SR" -o json || true
         SR_TOKENS=$(jq '[.status.usage.tokens // [] | .[] | ((.input // "0") | tonumber) + ((.output // "0") | tonumber)] | add // 0' "$WORK/sr.json")
         ax resume task "$T_SR" >/dev/null || die "resume $T_SR"
         SR_STATE=2
         event "RESUME $T_SR (tokens at suspend $SR_TOKENS; the command re-runs in a new runner session)"
       fi ;;
    2) if [ "$(tokens_of "$T_SR")" -gt "$SR_TOKENS" ]; then
         SR_STATE=3
         event "$T_SR resumed run reported new tokens ($(tokens_of "$T_SR") > $SR_TOKENS)"
       fi ;;
  esac
  ok=1
  [ "$(cond "$T_BUDGET" BudgetExceeded)" = "True/MaxCostExceeded" ] || ok=0
  [ "$(jq_task "$T_DONE" '.status.phase')" = Completed ] || ok=0
  [ "$(cond "$T_IDLE" Idle | cut -d/ -f1)" = True ] || ok=0
  jq -e '(.status.usage.cost.idle // 0) > 0' <<<"$(jq --arg t "$T_IDLE" '.tasks[] | select(.metadata.name == $t)' "$WORK/tasks.json")" >/dev/null || ok=0
  [ "$SR_STATE" = 3 ] || ok=0
  for t in $TASKS; do
    jq -e '(.status.usage.cost.total // 0) > 0' <<<"$(jq --arg t "$t" '.tasks[] | select(.metadata.name == $t)' "$WORK/tasks.json")" >/dev/null || ok=0
  done
  [ "$ok" = 1 ] && break
  [ "$now" -lt "$deadline" ] || die "scenarios not all observed within ${DEMO_TIMEOUT_SECONDS}s (see $TIMELINE)"
done
event "all scenarios observed after $(( $(date +%s) - START_EPOCH ))s"

# --- freeze: suspend whatever still runs, let late usage events settle -------------
for t in $(jq -r '.tasks[] | select(.status.phase == "Running") | .metadata.name' "$WORK/tasks.json"); do
  ax suspend task "$t" >/dev/null || die "suspend $t"
  event "SUSPEND $t (end of demo: keep, but stop compute)"
done
say "waiting ${SETTLE_SECONDS}s for the final usage samples and closing state events"
sleep "$SETTLE_SECONDS"

# --- capture ------------------------------------------------------------------------
ax_retry "$OUT/get-tasks.txt" get tasks -l "demo=$PREFIX" --show-labels || die "get tasks"
ax_retry "$OUT/get-tasks.json" get tasks -l "demo=$PREFIX" -o json || die "get tasks json"
for t in $TASKS; do ax_retry "$OUT/describe-$t.txt" describe task "$t" || die "describe $t"; done
for by in agent customer; do
  ax_retry "$OUT/cost-summary-$by.txt" cost summary --by "$by" -l "demo=$PREFIX" || die "summary $by"
  ax_retry "$OUT/cost-summary-$by.json" cost summary --by "$by" -l "demo=$PREFIX" -o json || die "summary $by json"
done
ax_retry "$OUT/focus.csv" cost export --format focus-csv -l "demo=$PREFIX" || die "focus export"
# Re-read after the export: nothing may have moved while capturing.
ax_retry "$WORK/summary-again.json" cost summary --by agent -l "demo=$PREFIX" -o json || die "summary re-read"
jq -e -n --slurpfile a "$OUT/cost-summary-agent.json" --slurpfile b "$WORK/summary-again.json" \
  '$a[0].total.cost.total == $b[0].total.cost.total' >/dev/null ||
  say "WARN: total moved during capture ($(jq .total.cost.total "$OUT/cost-summary-agent.json") -> $(jq .total.cost.total "$WORK/summary-again.json"))"
# ax-server log lines about the demo tasks (budget crossings, completion,
# usage summaries) plus rate card reloads.
tmo 60 kubectl --context "$CTX" -n ax-system logs deploy/ax-server --since-time="$START_UTC" >"$WORK/server.log" 2>/dev/null || true
grep -E "\"$PREFIX-|\"msg\":\"rate card reloaded\"" "$WORK/server.log" >"$OUT/ax-server-log.jsonl" || true

echo
cat "$OUT/get-tasks.txt"
echo
cat "$OUT/describe-$T_BUDGET.txt"
echo
cat "$OUT/cost-summary-agent.txt"
echo
cat "$OUT/cost-summary-customer.txt"
echo
say "FOCUS export: $(($(wc -l <"$OUT/focus.csv") - 1)) rows -> $OUT/focus.csv"
head -4 "$OUT/focus.csv" | cut -c1-240

# --- dashboard: PromQL over the demo window -----------------------------------------
[ "$PROMQL" = 1 ] && run_promql

if [ "$CLEANUP" = 1 ]; then
  delete_demo_tasks
  event "deleted the demo tasks (--cleanup)"
else
  say "kept the demo tasks (Suspended/Completed): ax get tasks -l demo=$PREFIX; delete with $0 --cleanup-only --prefix $PREFIX"
fi
say "outputs in $OUT"
