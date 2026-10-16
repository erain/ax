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

"""A small Gemini-calling "productivity agent" for the AX FinOps demo.

It runs ITERATIONS planning/summarization calls against the Gemini API,
writes each answer to OUT_DIR, and reports the token usage of every call to
the AX task runner in one of two ways:

  REPORT_MODE=otlp  an OpenTelemetry GenAI "chat" span per call, exported over
                    OTLP/HTTP to $OTEL_EXPORTER_OTLP_ENDPOINT (the runner's
                    loopback receiver, injected by ax-task-runner).
  REPORT_MODE=json  a JSON record per call, POSTed to
                    $AX_METADATA_URL/metadata/v1alpha1/ax/usage.

Counts follow the OpenTelemetry GenAI convention (input includes cached
tokens, output includes reasoning tokens). Gemini's usage_metadata maps as:

  input     = prompt_token_count      (already includes cached_content_token_count)
  cacheRead = cached_content_token_count
  output    = candidates_token_count + thoughts_token_count
  reasoning = thoughts_token_count

After the last call it prints one machine-readable line,

  FINOPS_HARNESS_TOTALS {"model": ..., "input": ..., ...}

and then sleeps IDLE_SECONDS so the task shows up as idle.

Environment:
  GEMINI_API_KEY   (required) Gemini API key.
  MODEL            Gemini model, default gemini-3-flash-preview (gemini-2.5-flash
                   is no longer served by the Gemini API).
  ITERATIONS       number of model calls, default 3.
  SLEEP_SECONDS    pause between calls, default 5.
  IDLE_SECONDS     idle time after the last call, default 600.
  REPORT_MODE      otlp (default) or json; --report=<mode> overrides it.
  OUT_DIR          output directory, default ./out (the task's workspace).
  CONTEXT_REPEAT   size of the shared prompt prefix (a larger prefix makes
                   Gemini implicit caching, and so cacheRead, more likely),
                   default 12.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

from google import genai
from google.genai import errors as genai_errors

TOTALS_MARKER = "FINOPS_HARNESS_TOTALS"
PROVIDER = "gcp.gemini"
RETRYABLE_CODES = {408, 429, 500, 502, 503, 504}

TOPICS = [
    "migrating a monolith's billing module to a service",
    "preparing the quarterly on-call review",
    "rolling out a new CI pipeline to five teams",
    "cleaning up stale feature flags",
    "planning a game day for regional failover",
    "writing the onboarding guide for new SREs",
    "reducing p99 latency of the search API",
    "triaging a backlog of 120 customer bugs",
]

HANDBOOK_PARAGRAPH = (
    "Team handbook. Engineers keep plans short and actionable. Every plan names an owner, "
    "a deadline, the risks, and the metric that proves success. Work is split into steps that "
    "fit in a day. Dependencies on other teams are called out early and confirmed in writing. "
    "Production changes go through review, are rolled out gradually behind a flag, and have a "
    "tested rollback. Incidents get a blameless review within five working days. Cost matters: "
    "prefer the cheapest option that meets the requirement and say what it costs. "
)


def env_int(name, default):
    value = os.environ.get(name, "")
    return int(value) if value.strip() else default


def log(msg, **fields):
    extra = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"finops-agent: {msg} {extra}".rstrip(), flush=True)


def usage_from_metadata(um):
    """Converts Gemini usage_metadata to semconv-inclusive token counts."""
    prompt = (um.prompt_token_count or 0) if um else 0
    cached = (um.cached_content_token_count or 0) if um else 0
    candidates = (um.candidates_token_count or 0) if um else 0
    thoughts = (um.thoughts_token_count or 0) if um else 0
    # This harness uses no tools, so tool_use_prompt_token_count is not used.
    return {
        "input": prompt,
        "output": candidates + thoughts,
        "cacheRead": cached,
        "cacheWrite": 0,  # Gemini implicit caching has no separate write charge.
        "reasoning": thoughts,
    }


class OTLPReporter:
    """Reports each call as an OpenTelemetry GenAI client span."""

    def __init__(self, request_model):
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        self._trace = trace
        self.provider = TracerProvider(resource=Resource.create({"service.name": "finops-agent"}))
        # The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT and appends /v1/traces.
        self.provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        self.tracer = self.provider.get_tracer("ax.examples.finops-agent")
        self.request_model = request_model
        # A parent invoke_agent span groups the run. The runner skips usage on
        # aggregate spans, so putting the run totals on it must not double count.
        self.agent_span = self.tracer.start_span(
            "invoke_agent finops-agent",
            kind=trace.SpanKind.CLIENT,
            attributes={
                "gen_ai.operation.name": "invoke_agent",
                "gen_ai.provider.name": PROVIDER,
                "gen_ai.agent.name": "finops-agent",
                "gen_ai.request.model": request_model,
            },
        )
        self.agent_ctx = trace.set_span_in_context(self.agent_span)

    def record(self, response_model, usage, response_id, finish_reasons, start_ns, end_ns):
        attrs = {
            "gen_ai.operation.name": "chat",
            "gen_ai.provider.name": PROVIDER,
            "gen_ai.request.model": self.request_model,
            "gen_ai.response.model": response_model,
            "gen_ai.usage.input_tokens": usage["input"],
            "gen_ai.usage.output_tokens": usage["output"],
        }
        if usage["cacheRead"]:
            attrs["gen_ai.usage.cache_read.input_tokens"] = usage["cacheRead"]
        if usage["reasoning"]:
            attrs["gen_ai.usage.reasoning.output_tokens"] = usage["reasoning"]
        if response_id:
            attrs["gen_ai.response.id"] = response_id
        if finish_reasons:
            attrs["gen_ai.response.finish_reasons"] = finish_reasons
        span = self.tracer.start_span(
            f"chat {self.request_model}",
            context=self.agent_ctx,
            kind=self._trace.SpanKind.CLIENT,
            attributes=attrs,
            start_time=start_ns,
        )
        span.end(end_time=end_ns)

    def finish(self, totals):
        for key, attr in (("input", "gen_ai.usage.input_tokens"), ("output", "gen_ai.usage.output_tokens")):
            self.agent_span.set_attribute(attr, totals[key])
        self.agent_span.end()
        ok = self.provider.force_flush(timeout_millis=30000)
        self.provider.shutdown()
        if not ok:
            log("WARNING: OTLP span flush timed out; the runner may be missing usage")
        return ok


class JSONReporter:
    """Reports each call to the runner's loopback JSON usage endpoint."""

    def __init__(self):
        base = os.environ.get("AX_METADATA_URL", "")
        if not base:
            raise SystemExit("REPORT_MODE=json needs AX_METADATA_URL (set by ax-task-runner)")
        self.url = base.rstrip("/") + "/metadata/v1alpha1/ax/usage"

    def record(self, response_model, usage, response_id, finish_reasons, start_ns, end_ns):
        body = json.dumps(
            {
                "model": response_model,
                "inputTokens": usage["input"],
                "outputTokens": usage["output"],
                "cacheReadTokens": usage["cacheRead"],
                "cacheWriteTokens": usage["cacheWrite"],
                "reasoningTokens": usage["reasoning"],
                # requestId makes retries safe: the runner counts it once.
                "requestId": response_id or str(uuid.uuid4()),
            }
        ).encode()
        for attempt in range(5):
            try:
                req = urllib.request.Request(
                    self.url, data=body, headers={"Content-Type": "application/json"}, method="POST"
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp.read()
                return
            except (urllib.error.URLError, OSError) as e:
                log("usage POST failed, retrying", attempt=attempt + 1, error=type(e).__name__)
                time.sleep(1 + attempt)
        raise RuntimeError("could not report usage to the runner")

    def finish(self, totals):
        return True


def generate(client, model, prompt):
    """Calls Gemini, retrying transient errors. Only successful calls bill tokens."""
    delay = 2.0
    for attempt in range(6):
        try:
            return client.models.generate_content(model=model, contents=prompt)
        except genai_errors.APIError as e:
            if e.code not in RETRYABLE_CODES or attempt == 5:
                raise
            log("Gemini call failed, retrying", code=e.code, attempt=attempt + 1)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", choices=["otlp", "json"], default=os.environ.get("REPORT_MODE", "otlp"))
    args = parser.parse_args()

    model = os.environ.get("MODEL", "gemini-3-flash-preview")
    iterations = env_int("ITERATIONS", 3)
    sleep_seconds = env_int("SLEEP_SECONDS", 5)
    idle_seconds = env_int("IDLE_SECONDS", 600)
    out_dir = os.environ.get("OUT_DIR", "out")
    context_repeat = env_int("CONTEXT_REPEAT", 12)
    if not os.environ.get("GEMINI_API_KEY"):
        raise SystemExit("GEMINI_API_KEY is not set")

    os.makedirs(out_dir, exist_ok=True)
    reporter = OTLPReporter(model) if args.report == "otlp" else JSONReporter()
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    handbook = HANDBOOK_PARAGRAPH * max(context_repeat, 0)
    log("starting", model=model, iterations=iterations, report=args.report, out_dir=out_dir)

    by_model = {}
    calls = failures = 0
    for i in range(iterations):
        topic = TOPICS[i % len(TOPICS)]
        prompt = (
            f"{handbook}\n\nYou are a productivity assistant for an engineering team. "
            f"Following the handbook, write a concise five-step plan for {topic}, then a "
            f"two-sentence summary of the main risk."
        )
        start_ns = time.time_ns()
        try:
            resp = generate(client, model, prompt)
        except Exception as e:  # keep going; the totals reflect successful calls only
            failures += 1
            log("Gemini call failed", iteration=i + 1, error=f"{type(e).__name__}: {e}")
            continue
        end_ns = time.time_ns()
        usage = usage_from_metadata(resp.usage_metadata)
        # The runner keys usage by the response model, else the request model.
        response_model = resp.model_version or model
        reporter.record(
            response_model,
            usage,
            resp.response_id,
            [str(c.finish_reason.name) for c in (resp.candidates or []) if c.finish_reason],
            start_ns,
            end_ns,
        )
        calls += 1
        agg = by_model.setdefault(response_model, dict.fromkeys(usage, 0))
        for k, v in usage.items():
            agg[k] += v
        with open(os.path.join(out_dir, f"plan-{i + 1:03d}.md"), "w") as f:
            f.write(f"# {topic}\n\n{resp.text or ''}\n")
        log("call done", iteration=i + 1, model=response_model, **usage)
        if i + 1 < iterations and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    sums = {k: sum(m[k] for m in by_model.values()) for k in ("input", "output", "cacheRead", "cacheWrite", "reasoning")}
    flushed = reporter.finish(sums)
    totals = {
        "model": next(iter(by_model)) if len(by_model) == 1 else model,
        **sums,
        "calls": calls,
        "failures": failures,
        "mode": args.report,
        "byModel": by_model,
    }
    with open(os.path.join(out_dir, "totals.json"), "w") as f:
        json.dump(totals, f, indent=2)
    print(f"{TOTALS_MARKER} {json.dumps(totals, sort_keys=True)}", flush=True)

    if idle_seconds > 0:
        log("idling", seconds=idle_seconds)
        time.sleep(idle_seconds)
    log("done")
    return 0 if failures == 0 and flushed else 1


if __name__ == "__main__":
    sys.exit(main())
