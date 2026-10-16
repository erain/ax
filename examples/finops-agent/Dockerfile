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

# Task runner image for the AX FinOps demo: ax-task-runner (with token usage
# reporting) plus the Python Gemini harness in /opt/finops-agent and its
# dependencies. Build from the repository root:
#
#   docker build -f examples/finops-agent/Dockerfile -t <repo>/ax-task-runner:finops .
#
# or with Cloud Build: see examples/finops-agent/cloudbuild.yaml.

FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" \
    -o /out/ax-task-runner ./cmd/ax-task-runner

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    openssh-client \
    procps \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Same base content as Dockerfile.task-runner, so the image can stand in for
# the default task image.
RUN pip install --no-cache-dir google-antigravity

COPY examples/finops-agent/requirements.txt /opt/finops-agent/requirements.txt
RUN pip install --no-cache-dir -r /opt/finops-agent/requirements.txt

COPY --from=build /out/ax-task-runner /usr/local/bin/ax-task-runner
COPY cmd/ax-task-runner/antigravity_bootstrap.py /usr/local/bin/antigravity_bootstrap.py
COPY examples/finops-agent/agent.py /opt/finops-agent/agent.py

ENTRYPOINT ["/usr/local/bin/ax-task-runner"]
