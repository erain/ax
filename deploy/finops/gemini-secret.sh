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

# Creates or updates the Secret ax-server reads the Gemini API key from:
# gemini-api-secret/GEMINI_API_KEY in the namespace named after the atespace.
# The reconciler copies it into the environment of every Task in that atespace
# as GEMINI_API_KEY. The key is read from $GEMINI_API_KEY and never printed;
# it reaches kubectl on stdin, not on the command line.
#
#   GEMINI_API_KEY=... deploy/finops/gemini-secret.sh [--context <ctx>] [atespace]
set -euo pipefail

ctx_args=()
if [ "${1:-}" = "--context" ]; then
  ctx_args=(--context "$2")
  shift 2
fi
ns="${1:-default}"

if [ -z "${GEMINI_API_KEY:-}" ]; then
  echo "GEMINI_API_KEY is not set" >&2
  exit 1
fi

kubectl ${ctx_args[@]+"${ctx_args[@]}"} get namespace "$ns" >/dev/null
printf %s "$GEMINI_API_KEY" |
  kubectl ${ctx_args[@]+"${ctx_args[@]}"} -n "$ns" create secret generic gemini-api-secret \
    --from-file=GEMINI_API_KEY=/dev/stdin --dry-run=client -o yaml |
  kubectl ${ctx_args[@]+"${ctx_args[@]}"} apply -f - >/dev/null
echo "secret $ns/gemini-api-secret (key GEMINI_API_KEY) applied"
