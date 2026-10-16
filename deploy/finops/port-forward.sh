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

# Forwards the ax-server API (svc/ax-server:8080 in ax-system) to localhost so
# the local ax CLI can reach it:
#
#   deploy/finops/port-forward.sh [--context <ctx>] [local-port]   # default 8080
#   ax --server localhost:8080 get tasks
#
# Runs in the foreground until interrupted. The kube context is always passed
# explicitly (default: the ax-finops-poc cluster, or $KUBE_CONTEXT).
set -euo pipefail

ctx="${KUBE_CONTEXT:-gke_yiyu-gke-dev_us-central1-a_ax-finops-poc}"
if [ "${1:-}" = "--context" ]; then
  ctx="$2"
  shift 2
fi
port="${1:-8080}"

echo "forwarding localhost:$port -> ax-system/svc/ax-server:8080 (context $ctx)" >&2
exec kubectl --context "$ctx" -n ax-system port-forward --address 127.0.0.1 svc/ax-server "$port:8080"
