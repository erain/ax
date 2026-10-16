// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runner

import (
	"bytes"
	"compress/gzip"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func intKV(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}

func dblKV(k string, v float64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v}}}
}

func span(id byte, attrs ...*commonpb.KeyValue) *tracepb.Span {
	return &tracepb.Span{
		TraceId:    bytes.Repeat([]byte{0xab}, 16),
		SpanId:     []byte{0, 0, 0, 0, 0, 0, 0, id},
		Name:       "chat",
		Attributes: attrs,
	}
}

func traces(spans ...*tracepb.Span) *coltracepb.ExportTraceServiceRequest {
	return &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:   &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "harness")}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}}
}

type tokenPoint struct {
	model, tokenType string
	start            uint64
	sum              float64
}

func tokenMetrics(temporality metricspb.AggregationTemporality, points ...tokenPoint) *colmetricspb.ExportMetricsServiceRequest {
	var dps []*metricspb.HistogramDataPoint
	for _, p := range points {
		dps = append(dps, &metricspb.HistogramDataPoint{
			Attributes:        []*commonpb.KeyValue{strKV("gen_ai.request.model", p.model), strKV("gen_ai.token.type", p.tokenType), strKV("gen_ai.operation.name", "chat")},
			StartTimeUnixNano: p.start,
			TimeUnixNano:      p.start + 1,
			Count:             1,
			Sum:               proto.Float64(p.sum),
		})
	}
	return &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strKV("service.name", "harness")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
			{Name: "some.other.metric", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{DataPoints: dps}}},
			{Name: tokenUsageMetric, Unit: "{token}", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				AggregationTemporality: temporality,
				DataPoints:             dps,
			}}},
		}}},
	}}}
}

// otlpPost sends msg to the handler as protobuf, or as JSON when asJSON.
func otlpPost(t *testing.T, h http.Handler, path string, msg proto.Message, asJSON bool) *httptest.ResponseRecorder {
	t.Helper()
	var (
		body []byte
		err  error
		ct   = "application/x-protobuf"
	)
	if asJSON {
		body, err = protojson.Marshal(msg)
		ct = "application/json"
	} else {
		body, err = proto.Marshal(msg)
	}
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST %s = %d: %s", path, rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != ct {
		t.Errorf("response Content-Type = %q, want %q", got, ct)
	}
	return rec
}

func TestOTLPTraces(t *testing.T) {
	flashCall := span(1,
		strKV("gen_ai.operation.name", "chat"),
		strKV("gen_ai.request.model", "gemini-2.5-flash"),
		strKV("gen_ai.response.model", "gemini-2.5-flash-001"),
		intKV("gen_ai.usage.input_tokens", 1000),
		intKV("gen_ai.usage.output_tokens", 200),
		intKV("gen_ai.usage.cache_read.input_tokens", 300),
		intKV("gen_ai.usage.reasoning.output_tokens", 50),
	)
	legacyNames := span(2,
		strKV("gen_ai.request.model", "claude-x"),
		intKV("gen_ai.usage.input_tokens", 10),
		dblKV("gen_ai.usage.output_tokens", 4),
		intKV("gen_ai.usage.cache_read_input_tokens", 7),
		intKV("gen_ai.usage.cache_creation_input_tokens", 3),
		intKV("gen_ai.usage.reasoning_tokens", 2),
		dblKV("gen_ai.usage.cost.amount", 0.5),
		strKV("gen_ai.usage.cost.currency", "USD"),
	)
	noModel := span(3, intKV("gen_ai.usage.input_tokens", 5))
	agentSummary := span(4,
		strKV("gen_ai.operation.name", "invoke_agent"),
		strKV("gen_ai.request.model", "gemini-2.5-flash"),
		intKV("gen_ai.usage.input_tokens", 1000),
	)
	noUsage := span(5, strKV("gen_ai.request.model", "gemini-2.5-flash"), strKV("http.method", "POST"))

	want := map[string]*TokenUsage{
		"gemini-2.5-flash-001": {Input: 1000, Output: 200, CacheRead: 300, Reasoning: 50},
		"claude-x":             {Input: 10, Output: 4, CacheRead: 7, CacheWrite: 3, Reasoning: 2, CostAmount: 0.5, CostCurrency: "USD"},
		"unknown":              {Input: 5},
	}

	for _, asJSON := range []bool{false, true} {
		name := "protobuf"
		if asJSON {
			name = "json"
		}
		t.Run(name, func(t *testing.T) {
			tr := newTestTracker(t)
			h := newOTLPHandler(tr)
			req := traces(flashCall, legacyNames, noModel, agentSummary, noUsage)
			otlpPost(t, h, "/v1/traces", req, asJSON)
			if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
				t.Fatalf("tokens = %s, want %s", jsonString(got), jsonString(want))
			}
			// An exporter retry delivers the same spans again: not counted twice.
			otlpPost(t, h, "/v1/traces", traces(flashCall, legacyNames), asJSON)
			if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
				t.Fatalf("tokens after duplicate export = %s, want %s", jsonString(got), jsonString(want))
			}
		})
	}
}

func TestOTLPTraces_AttributeAliases(t *testing.T) {
	tests := []struct {
		name  string
		attrs []*commonpb.KeyValue
		want  *TokenUsage
	}{
		{
			name: "cache_write is the current cache creation name",
			attrs: []*commonpb.KeyValue{
				intKV("gen_ai.usage.input_tokens", 100),
				intKV("gen_ai.usage.output_tokens", 10),
				intKV("gen_ai.usage.cache_write.input_tokens", 40),
			},
			want: &TokenUsage{Input: 100, Output: 10, CacheWrite: 40},
		},
		{
			name: "cache_write wins over cache_creation",
			attrs: []*commonpb.KeyValue{
				intKV("gen_ai.usage.input_tokens", 100),
				intKV("gen_ai.usage.cache_write.input_tokens", 40),
				intKV("gen_ai.usage.cache_creation.input_tokens", 99),
			},
			want: &TokenUsage{Input: 100, CacheWrite: 40},
		},
		{
			name: "deprecated prompt and completion tokens",
			attrs: []*commonpb.KeyValue{
				intKV("gen_ai.usage.prompt_tokens", 70),
				intKV("gen_ai.usage.completion_tokens", 7),
			},
			want: &TokenUsage{Input: 70, Output: 7},
		},
		{
			name: "current names win over deprecated ones",
			attrs: []*commonpb.KeyValue{
				intKV("gen_ai.usage.input_tokens", 50),
				intKV("gen_ai.usage.prompt_tokens", 500),
				intKV("gen_ai.usage.output_tokens", 5),
				intKV("gen_ai.usage.completion_tokens", 55),
			},
			want: &TokenUsage{Input: 50, Output: 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTestTracker(t)
			attrs := append([]*commonpb.KeyValue{strKV("gen_ai.request.model", "m")}, tt.attrs...)
			otlpPost(t, newOTLPHandler(tr), "/v1/traces", traces(span(1, attrs...)), false)
			want := map[string]*TokenUsage{"m": tt.want}
			if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
				t.Fatalf("tokens = %s, want %s", jsonString(got), jsonString(want))
			}
		})
	}
}

func TestOTLPTraces_SpecJSONEncoding(t *testing.T) {
	// OTLP/JSON as exporters emit it: hex ids, int64 as strings, camelCase keys.
	body := `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"harness"}}]},
	"scopeSpans":[{"scope":{"name":"genai"},"spans":[{"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174",
	"name":"chat gemini-2.5-flash","kind":3,"startTimeUnixNano":"1544712660000000000","endTimeUnixNano":"1544712661000000000",
	"attributes":[{"key":"gen_ai.request.model","value":{"stringValue":"gemini-2.5-flash"}},
	{"key":"gen_ai.usage.input_tokens","value":{"intValue":"42"}},{"key":"gen_ai.usage.output_tokens","value":{"intValue":8}}]}]}]}]}`
	tr := newTestTracker(t)
	h := newOTLPHandler(tr)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/traces", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("POST = %d: %s", rec.Code, rec.Body.String())
		}
	}
	want := map[string]*TokenUsage{"gemini-2.5-flash": {Input: 42, Output: 8}}
	if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
		t.Errorf("tokens = %s, want %s", jsonString(got), jsonString(want))
	}
}

func TestOTLPHandler_EncodingsAndErrors(t *testing.T) {
	tr := newTestTracker(t)
	h := newOTLPHandler(tr)
	raw, _ := proto.Marshal(traces(span(1, strKV("gen_ai.request.model", "m"), intKV("gen_ai.usage.input_tokens", 9))))
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(raw)
	_ = zw.Close()

	tests := []struct {
		name     string
		method   string
		path     string
		ct, ce   string
		body     []byte
		wantCode int
	}{
		{"gzip protobuf", http.MethodPost, "/v1/traces", "application/x-protobuf", "gzip", gz.Bytes(), http.StatusOK},
		{"logs accepted and dropped", http.MethodPost, "/v1/logs", "application/json", "", []byte(`{"resourceLogs":[]}`), http.StatusOK},
		{"garbage protobuf", http.MethodPost, "/v1/traces", "application/x-protobuf", "", []byte{0xff, 0xff, 0xff}, http.StatusBadRequest},
		{"garbage json", http.MethodPost, "/v1/metrics", "application/json", "", []byte(`{"resourceMetrics":`), http.StatusBadRequest},
		{"unknown encoding", http.MethodPost, "/v1/traces", "application/x-protobuf", "br", raw, http.StatusUnsupportedMediaType},
		{"GET not allowed", http.MethodGet, "/v1/traces", "", "", nil, http.StatusMethodNotAllowed},
		{"unknown path", http.MethodPost, "/v1/profiles", "application/x-protobuf", "", raw, http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(tt.body))
			if tt.ct != "" {
				req.Header.Set("Content-Type", tt.ct)
			}
			if tt.ce != "" {
				req.Header.Set("Content-Encoding", tt.ce)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.wantCode {
				t.Errorf("status = %d, want %d (%s)", rec.Code, tt.wantCode, rec.Body.String())
			}
		})
	}
	want := map[string]*TokenUsage{"m": {Input: 9}}
	if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
		t.Errorf("tokens = %s, want %s", jsonString(got), jsonString(want))
	}
}

func TestOTLPMetrics(t *testing.T) {
	const (
		cumulative = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
		delta      = metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	)
	tests := []struct {
		name    string
		exports []*colmetricspb.ExportMetricsServiceRequest
		want    map[string]*TokenUsage
	}{
		{
			name: "cumulative exports are diffed",
			exports: []*colmetricspb.ExportMetricsServiceRequest{
				tokenMetrics(cumulative, tokenPoint{"gemini-2.5-flash", "input", 1, 100}, tokenPoint{"gemini-2.5-flash", "output", 1, 10}),
				tokenMetrics(cumulative, tokenPoint{"gemini-2.5-flash", "input", 1, 250}, tokenPoint{"gemini-2.5-flash", "output", 1, 30}),
				tokenMetrics(cumulative, tokenPoint{"gemini-2.5-flash", "input", 1, 250}, tokenPoint{"gemini-2.5-flash", "output", 1, 30}),
			},
			want: map[string]*TokenUsage{"gemini-2.5-flash": {Input: 250, Output: 30}},
		},
		{
			name: "cumulative stream restart counts the new value in full",
			exports: []*colmetricspb.ExportMetricsServiceRequest{
				tokenMetrics(cumulative, tokenPoint{"gemini-2.5-flash", "input", 1, 100}),
				tokenMetrics(cumulative, tokenPoint{"gemini-2.5-flash", "input", 2, 40}),
			},
			want: map[string]*TokenUsage{"gemini-2.5-flash": {Input: 140}},
		},
		{
			name: "delta exports are summed",
			exports: []*colmetricspb.ExportMetricsServiceRequest{
				tokenMetrics(delta, tokenPoint{"gemini-2.5-pro", "input", 1, 100}),
				tokenMetrics(delta, tokenPoint{"gemini-2.5-pro", "input", 2, 50}, tokenPoint{"gemini-2.5-pro", "output", 2, 5}),
			},
			want: map[string]*TokenUsage{"gemini-2.5-pro": {Input: 150, Output: 5}},
		},
		{
			name: "unknown token types ignored",
			exports: []*colmetricspb.ExportMetricsServiceRequest{
				tokenMetrics(delta, tokenPoint{"m", "input", 1, 7}, tokenPoint{"m", "thoughts", 1, 99}),
			},
			want: map[string]*TokenUsage{"m": {Input: 7}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTestTracker(t)
			h := newOTLPHandler(tr)
			for i, req := range tt.exports {
				otlpPost(t, h, "/v1/metrics", req, i%2 == 1) // alternate protobuf and JSON
			}
			if got := tr.report().Tokens; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("tokens = %s, want %s", jsonString(got), jsonString(tt.want))
			}
		})
	}
}

func TestOTLP_SpansAndMetricsNotDoubleCounted(t *testing.T) {
	tr := newTestTracker(t)
	h := newOTLPHandler(tr)
	cumulative := metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE

	// The harness reports one call as a span and the same call in the token
	// histogram. The span's response model differs from the metric's request model.
	otlpPost(t, h, "/v1/traces", traces(span(1,
		strKV("gen_ai.request.model", "gemini-2.5-flash"),
		strKV("gen_ai.response.model", "gemini-2.5-flash-001"),
		intKV("gen_ai.usage.input_tokens", 100),
		intKV("gen_ai.usage.output_tokens", 10),
	)), false)
	otlpPost(t, h, "/v1/metrics", tokenMetrics(cumulative,
		tokenPoint{"gemini-2.5-flash", "input", 1, 100},
		tokenPoint{"gemini-2.5-flash", "output", 1, 10},
		// A model only ever seen in metrics is still counted.
		tokenPoint{"gemini-2.5-pro", "input", 1, 60},
	), false)

	want := map[string]*TokenUsage{
		"gemini-2.5-flash-001": {Input: 100, Output: 10},
		"gemini-2.5-pro":       {Input: 60},
	}
	if got := tr.report().Tokens; !reflect.DeepEqual(got, want) {
		t.Errorf("tokens = %s, want %s", jsonString(got), jsonString(want))
	}
}

func TestCheckLoopback(t *testing.T) {
	tests := []struct {
		addr    string
		wantErr bool
	}{
		{"127.0.0.1:4318", false},
		{"127.0.0.1:0", false},
		{"[::1]:4318", false},
		{"localhost:4318", false},
		{":4318", true},
		{"0.0.0.0:4318", true},
		{"10.0.0.5:4318", true},
		{"example.com:4318", true},
		{"127.0.0.1", true},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			if err := checkLoopback(tt.addr); (err != nil) != tt.wantErr {
				t.Errorf("checkLoopback(%q) error = %v, wantErr %v", tt.addr, err, tt.wantErr)
			}
		})
	}
}

func TestOTLPReceiver_BindsLoopbackOnly(t *testing.T) {
	tr := newTestTracker(t)
	if _, err := startOTLPReceiver(":0", tr); err == nil {
		t.Fatal("receiver started on all interfaces")
	}
	r, err := startOTLPReceiver("127.0.0.1:0", tr)
	if err != nil {
		t.Fatal(err)
	}
	defer r.stop(context.Background())
	if r.server.ReadHeaderTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout = %v, want a positive timeout", r.server.ReadHeaderTimeout)
	}

	host, _, err := net.SplitHostPort(r.addr)
	if err != nil {
		t.Fatal(err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		t.Errorf("receiver bound to %q, want a loopback address", r.addr)
	}
	if !strings.HasPrefix(r.endpoint(), "http://127.0.0.1:") {
		t.Errorf("endpoint = %q", r.endpoint())
	}

	raw, _ := proto.Marshal(traces(span(1, strKV("gen_ai.request.model", "m"), intKV("gen_ai.usage.input_tokens", 1))))
	resp, err := http.Post(r.endpoint()+"/v1/traces", "application/x-protobuf", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /v1/traces = %d", resp.StatusCode)
	}
}
