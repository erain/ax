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

# Creates or updates the "AX Agent FinOps (PoC)" Cloud Monitoring dashboard
# from deploy/finops/dashboard.json (idempotent). An existing dashboard is
# found by displayName and updated in place with its current etag; otherwise
# a new one is created. Prints the console URL.
#
#   deploy/finops/apply-dashboard.sh [--project yiyu-gke-dev] [--file deploy/finops/dashboard.json]
#
# The logs panel in dashboard.json is scoped to projects/yiyu-gke-dev; with
# --project it is rewritten to the target project.
set -euo pipefail

project="${PROJECT:-yiyu-gke-dev}"
file="$(cd "$(dirname "$0")" && pwd)/dashboard.json"
while [ $# -gt 0 ]; do
  case "$1" in
    --project) project="$2"; shift 2 ;;
    --file) file="$2"; shift 2 ;;
    -h|--help) sed -n '16,24p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
[ -f "$file" ] || { echo "no such file: $file" >&2; exit 1; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

display_name="$(jq -er '.displayName' "$file")"
jq --arg p "projects/$project" '
  (.mosaicLayout.tiles[].widget.logsPanel.resourceNames? // empty) |= map($p)
  | del(.name, .etag)' "$file" > "$tmp/config.json"

# gcloud prints an mTLS warning on stderr in some environments; keep stderr
# for real errors but do not parse it.
existing="$(gcloud monitoring dashboards list --project "$project" \
  --filter="displayName=\"$display_name\"" --format='value(name)')"
count="$(printf '%s' "$existing" | grep -c . || true)"

if [ "$count" -gt 1 ]; then
  echo "found $count dashboards named \"$display_name\" in $project; delete the extras first:" >&2
  printf '  %s\n' $existing >&2
  exit 1
elif [ "$count" -eq 1 ]; then
  name="$existing"
  etag="$(gcloud monitoring dashboards describe "$name" --project "$project" --format='value(etag)')"
  jq --arg etag "$etag" '. + {etag: $etag}' "$tmp/config.json" > "$tmp/update.json"
  gcloud monitoring dashboards update "$name" --project "$project" \
    --config-from-file="$tmp/update.json" --format='value(name)' >/dev/null
  echo "updated $name" >&2
else
  name="$(gcloud monitoring dashboards create --project "$project" \
    --config-from-file="$tmp/config.json" --format='value(name)')"
  echo "created $name" >&2
fi

id="${name##*/}"
echo "https://console.cloud.google.com/monitoring/dashboards/builder/$id?project=$project"
