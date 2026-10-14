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

# Requires a live AX server, Substrate, and a working gVisor source WorkerPool.
# Missing prerequisites fail locally and in CI; this test never silently skips.
set -euo pipefail
: "${AX_SERVER:?Set AX_SERVER to the server under test}"
: "${AX_TEST_IMAGE:?Set AX_TEST_IMAGE to an ax-task-runner image accessible to Substrate}"
: "${AX_TEST_SNAPSHOTS_BUCKET:?Set AX_TEST_SNAPSHOTS_BUCKET to writable snapshot storage}"
AX_BIN=${AX_BIN:-./bin/ax}
SOURCE_NAMESPACE=${SOURCE_NAMESPACE:-ate-demo-counter}
SOURCE_POOL=${SOURCE_POOL:-counter}
TEST_TIMEOUT=${TEST_TIMEOUT:-120}
for tool in kubectl jq timeout; do command -v "$tool" >/dev/null; done
[[ -x "$AX_BIN" ]] || { echo 'Build the CLI with make build'; exit 1; }
name=${AX_TEST_ATESPACE:-ax-scheduling-e2e}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
atespace_created=false
ax() { timeout 30 "$AX_BIN" --server "$AX_SERVER" -a "$name" "$@"; }
ate() { timeout 60 kubectl ate "$@"; }
run() { printf '\n$'; printf ' %q' "$@"; printf '\n'; "$@"; }
cleanup() {
  result=$?
  trap - EXIT
  if (( result != 0 )); then
    ax get tasks || true
    kubectl get pods -n "$name" || true
  fi
  if [[ "$atespace_created" == true ]]; then
    for task in holder queued canceled deleted; do
      ax delete task "$task" >/dev/null 2>&1 || true
      ate delete actor "$task" -a "$name" >/dev/null 2>&1 || true
    done
    ate delete actor-template fixture -a "$name" >/dev/null 2>&1 || true
    ate delete atespace "$name" >/dev/null 2>&1 || true
  fi
  kubectl delete namespace "$name" --wait=false >/dev/null 2>&1 || true
  rm -rf "$work"
  exit "$result"
}
phase() { ax get task "$1" | awk '/^    phase:/ {print $2}'; }
wait_phase() {
  local end=$((SECONDS + TEST_TIMEOUT)) actual
  while (( SECONDS < end )); do
    actual=$(phase "$1")
    [[ "$actual" != Failed ]] || { echo "FAIL: $1 became Failed"; return 1; }
    if [[ "$actual" == "$2" ]]; then echo "PASS: $1 -> $2"; return; fi
    sleep 1
  done
  echo "FAIL: $1 did not become $2 within ${TEST_TIMEOUT}s"
  return 1
}

printf 'AX scheduling e2e: one worker, four resume requests\n'
kubectl get workerpool "$SOURCE_POOL" -n "$SOURCE_NAMESPACE" -o json |
  jq --arg ns "$name" '{apiVersion,kind,metadata:{name:"workers",namespace:$ns,labels:{workload:$ns}},spec:(.spec|.replicas=1|.template.resources={requests:{cpu:"250m",memory:"512Mi"},limits:{cpu:"1",memory:"1Gi"}})}' > "$work/pool.json"
kubectl create namespace "$name" >/dev/null
trap cleanup EXIT
kubectl apply -f "$work/pool.json" >/dev/null
kubectl wait -n "$name" --for=jsonpath='{.status.readyReplicas}'=1 workerpool/workers --timeout=120s >/dev/null
ate create atespace "$name" >/dev/null
atespace_created=true
# Pre-existing actors let the fixture pin placement without changing AX's template policy.
# The real AX reconciler still ensures, resumes, suspends, and deletes these actors.
jq -n --arg ns "$name" --arg image "$AX_TEST_IMAGE" --arg bucket "$AX_TEST_SNAPSHOTS_BUCKET" '{
 metadata:{name:"fixture",atespace:$ns},
 workerSelector:{matchLabels:{workload:$ns}},
 resources:{limits:[{name:"cpu",quantity:"1"},{name:"memory",quantity:"512Mi"}]},
 sandboxConfig:{sandboxClass:"SANDBOX_CLASS_GVISOR",configName:"gvisor-default"},
 containers:[{name:"guest",image:$image,command:["/usr/local/bin/ax-task-runner"],
 env:[{name:"AX_TASK_YAML",value:"metadata: {name: fixture}\nspec: {debug: true}\n"}],
 wakeupProbe:{httpGet:{path:"/readyz",port:80},timeoutSeconds:30},
 volumeMounts:[{name:"workspace",mountPath:"/workspace"}]}],
 volumes:[{name:"workspace",durableDir:{}}],
 snapshotConfig:{onPause:"SNAPSHOT_CONTENT_SCOPE_DATA",onCommit:"SNAPSHOT_CONTENT_SCOPE_DATA",storageLocation:($bucket+"/"+$ns+"/")}
}' > "$work/template.json"
ate create actor-template -f "$work/template.json" >/dev/null
end=$((SECONDS + TEST_TIMEOUT))
until ate get actor-template fixture -a "$name" -o json | jq -e '.status.goldenSnapshotStatus.goldenTag.name != null' >/dev/null; do
  (( SECONDS < end )) || { echo 'FAIL: fixture golden snapshot not ready'; exit 1; }
  sleep 1
done
for task in holder queued canceled deleted; do
  ate create actor "$task" -a "$name" --template fixture >/dev/null
  cat > "$work/task.yaml" <<YAML
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: $task
  atespace: $name
spec:
  image: $AX_TEST_IMAGE
  debug: true
YAML
  ax apply -f "$work/task.yaml" >/dev/null
done
run ax resume holder
wait_phase holder Running
for task in queued canceled deleted; do
  run ax resume "$task"
  wait_phase "$task" Pending
  ax describe task "$task" | awk '$1 == "Ready" && $2 == "False" && $3 == "WaitingForWorker" {found=1} END {exit !found}'
done
run ax get tasks
run ax describe task queued
printf '\nLeave capacity exhausted across multiple retries.\n'
sleep 12
for task in queued canceled deleted; do
  [[ $(phase "$task") == Pending ]] || { echo "FAIL: $task did not stay Pending"; exit 1; }
done
printf 'PASS: exhausted tasks stay Pending across retries\n'
run ax suspend canceled
run ax delete task deleted
# Restart is optional because the test can target an AX server outside Kubernetes.
if [[ -n ${AX_TEST_SERVER_NAMESPACE:-} ]]; then
  run kubectl rollout restart deployment/ax-server -n "$AX_TEST_SERVER_NAMESPACE"
  kubectl rollout status deployment/ax-server -n "$AX_TEST_SERVER_NAMESPACE" --timeout=120s
  sleep 5
  wait_phase queued Pending
fi
run ax delete task holder
printf '\nNo second resume request: wait for automatic scheduling.\n'
wait_phase queued Running
run ax ssh queued -- sh -ec 'echo capacity-recovered > /workspace/proof; grep -qx capacity-recovered /workspace/proof; cat /workspace/proof'
[[ $(phase canceled) == Suspended ]] || { echo 'FAIL: canceled task resumed'; exit 1; }
if ax get task deleted > "$work/deleted.log" 2>&1; then echo 'FAIL: deleted task reappeared'; exit 1; fi
grep -q 'NotFound' "$work/deleted.log"
# Verify Substrate placement, not just AX status.
ate get actor queued -a "$name" -o json | jq -e '.status.state == "ACTOR_STATE_RUNNING"' >/dev/null
run ax resume canceled
wait_phase canceled Pending
run kubectl scale workerpool/workers -n "$name" --replicas=2
wait_phase canceled Running
run ax ssh canceled -- echo scale-up-recovered
run ax get tasks
printf '\nPASS: automatic recovery, executable sandbox, suspend/delete cancellation, scale-up recovery\n'
