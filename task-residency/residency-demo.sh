#!/usr/bin/env bash
# Demo: task RESIDENCY, OUTCOME, and actor UID in `ax get` / `ax describe`.
# Runs against a live AX + Agent Substrate cluster and fails on any unexpected state.
#   CTX=<kube context> AX=<path to ax built from the PR> ./residency-demo.sh
set -euo pipefail
CTX="${CTX:?set CTX to the kube context}"
AX="${AX:-ax}"
NS="${NS:-default}"
TASK="${TASK:-residency-demo-$RANDOM}"
SYS="${AX_SYSTEM_NAMESPACE:-ax-system}"

ax() { "$AX" --context "$CTX" -a "$NS" "$@"; }
step() { printf '\n\033[1;36m$ %s\033[0m\n' "$*"; }
run() { step "ax $*"; ax "$@"; }
cleanup() { ax delete task "$TASK" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# expect PHASE RESIDENCY OUTCOME: checks the task's row in `ax get tasks`.
expect() {
  local row
  row="$(ax get tasks | awk -v t="$TASK" 'NR==1{for(i=1;i<=NF;i++)c[$i]=i; next} $1==t{print $c["PHASE"], $c["RESIDENCY"], $c["OUTCOME"]}')"
  if [[ "$row" != "$1 $2 $3" ]]; then
    printf '\033[1;31mFAIL: want PHASE/RESIDENCY/OUTCOME = %s %s %s, got: %s\033[0m\n' "$1" "$2" "$3" "$row"; exit 1
  fi
  printf '\033[1;32m✓ phase=%s residency=%s outcome=%s\033[0m\n' "$1" "$2" "$3"
}

# wait_for PHASE RESIDENCY: polls up to 120 s for the task to reach a state.
wait_for() {
  for _ in $(seq 1 60); do
    ax get tasks | awk -v t="$TASK" -v p="$1" -v r="$2" 'NR==1{for(i=1;i<=NF;i++)c[$i]=i; next} $1==t && $c["PHASE"]==p && $c["RESIDENCY"]==r{f=1} END{exit !f}' && return 0
    sleep 2
  done
  printf '\033[1;31mFAIL: task never reached %s/%s\033[0m\n' "$1" "$2"; exit 1
}

cat > "/tmp/$TASK.yaml" <<YAML
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: $TASK
spec:
  command: ["sh", "-c", "sleep infinity"]
YAML

run apply -f "/tmp/$TASK.yaml"
run get tasks
expect Suspended suspended "<none>"

run resume task "$TASK"
run get tasks
expect Running running "<none>"

step "ax describe task $TASK"
ax describe task "$TASK" | sed -n '1,9p'
uid="$(ax describe task "$TASK" | awk -F': *' '/^Actor UID:/{print $2}')"
[[ -n "$uid" && "$uid" != "<none>" ]] || { echo "FAIL: no actor UID"; exit 1; }
printf '\033[1;32m✓ actor UID %s\033[0m\n' "$uid"

# Crash the actor: delete the worker pod that hosts it.
pod="$(kubectl --context "$CTX" -n "$SYS" get pods -o wide | awk -v ip="$(ax get tasks | awk -v t="$TASK" '$1==t{print $(NF-1)}')" '$6==ip{print $1}')"
step "kubectl delete pod $pod   # kill the worker running the actor"
kubectl --context "$CTX" -n "$SYS" delete pod "$pod" --wait=false
wait_for Failed crashed
run get tasks
expect Failed crashed "<none>"

run resume task "$TASK"
run get tasks
expect Running running "<none>"

run suspend task "$TASK"
run get tasks
expect Suspended suspended "<none>"

printf '\n\033[1;32mAll residency checks passed.\033[0m\n'
