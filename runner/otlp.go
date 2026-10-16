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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultOTLPAddr is the loopback address of the OTLP/HTTP receiver that
	// ax-task-runner starts by default.
	DefaultOTLPAddr = "127.0.0.1:4318"

	// maxOTLPBytes caps a decoded OTLP request body.
	maxOTLPBytes = 16 << 20

	tokenUsageMetric = "gen_ai.client.token.usage"

	// otlpReadHeaderTimeout bounds how long a client may take to send request
	// headers to the OTLP receiver.
	otlpReadHeaderTimeout = 10 * time.Second
)

// Span attribute names carrying GenAI token usage. Where semantic conventions
// renamed an attribute, every spelling is accepted; the first present wins, so
// current names take precedence over deprecated ones (for example
// gen_ai.usage.prompt_tokens and gen_ai.usage.completion_tokens, which older
// instrumentations still emit).
var (
	attrInputTokens      = []string{"gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"}
	attrOutputTokens     = []string{"gen_ai.usage.output_tokens", "gen_ai.usage.completion_tokens"}
	attrCacheReadTokens  = []string{"gen_ai.usage.cache_read.input_tokens", "gen_ai.usage.cache_read_input_tokens"}
	attrCacheWriteTokens = []string{"gen_ai.usage.cache_write.input_tokens", "gen_ai.usage.cache_creation.input_tokens", "gen_ai.usage.cache_creation_input_tokens"}
	attrReasoningTokens  = []string{"gen_ai.usage.reasoning.output_tokens", "gen_ai.usage.reasoning_tokens"}
)

// aggregateOperations are gen_ai.operation.name values whose spans summarize
// the usage of child model-call spans. Their usage is not counted, so a
// framework that reports both levels is not counted twice.
var aggregateOperations = map[string]bool{
	"invoke_agent":    true,
	"create_agent":    true,
	"execute_tool":    true,
	"invoke_workflow": true,
}

// otlpReceiver is a loopback-only OTLP/HTTP receiver that turns GenAI spans and
// token-usage metrics into usage counters.
type otlpReceiver struct {
	server *http.Server
	addr   string // bound address, set by start
}

// checkLoopback rejects listen addresses that are not on a loopback interface.
func checkLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("address %q is not a loopback address", addr)
	}
	return nil
}

// startOTLPReceiver binds addr, which must be a loopback address, and serves
// the OTLP/HTTP trace, metric, and log endpoints in the background.
func startOTLPReceiver(addr string, t *usageTracker) (*otlpReceiver, error) {
	if err := checkLoopback(addr); err != nil {
		return nil, fmt.Errorf("OTLP receiver: %w", err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("OTLP receiver listening on %s: %w", addr, err)
	}
	r := &otlpReceiver{
		server: &http.Server{Handler: newOTLPHandler(t), ReadHeaderTimeout: otlpReadHeaderTimeout},
		addr:   lis.Addr().String(),
	}
	slog.Info("OTLP receiver started", "addr", r.addr)
	go func() {
		if err := r.server.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("OTLP receiver error", "error", err)
		}
	}()
	return r, nil
}

// endpoint is the base URL handed to the task command in
// OTEL_EXPORTER_OTLP_ENDPOINT.
func (r *otlpReceiver) endpoint() string { return "http://" + r.addr }

func (r *otlpReceiver) stop(ctx context.Context) error { return r.server.Shutdown(ctx) }

// newOTLPHandler serves POST /v1/traces, /v1/metrics, and /v1/logs with
// protobuf or JSON bodies. Logs are accepted and dropped so harnesses that
// export every signal do not see errors.
func newOTLPHandler(t *usageTracker) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", func(w http.ResponseWriter, r *http.Request) {
		req := &coltracepb.ExportTraceServiceRequest{}
		if !decodeOTLP(w, r, req) {
			return
		}
		t.applyOTLP(spanUsages(req), nil)
		writeOTLP(w, r, &coltracepb.ExportTraceServiceResponse{})
	})
	mux.HandleFunc("/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		req := &colmetricspb.ExportMetricsServiceRequest{}
		if !decodeOTLP(w, r, req) {
			return
		}
		t.applyOTLP(nil, metricPoints(req))
		writeOTLP(w, r, &colmetricspb.ExportMetricsServiceResponse{})
	})
	mux.HandleFunc("/v1/logs", func(w http.ResponseWriter, r *http.Request) {
		req := &collogspb.ExportLogsServiceRequest{}
		if !decodeOTLP(w, r, req) {
			return
		}
		writeOTLP(w, r, &collogspb.ExportLogsServiceResponse{})
	})
	return mux
}

func isJSON(r *http.Request) bool {
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return mt == "application/json"
}

// decodeOTLP reads an OTLP request body into msg, writing an error response
// and returning false when it cannot.
func decodeOTLP(w http.ResponseWriter, r *http.Request, msg proto.Message) bool {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	var body io.Reader = r.Body
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, "invalid gzip body", http.StatusBadRequest)
			return false
		}
		defer gz.Close()
		body = gz
	default:
		http.Error(w, "unsupported content encoding "+enc, http.StatusUnsupportedMediaType)
		return false
	}
	data, err := io.ReadAll(io.LimitReader(body, maxOTLPBytes+1))
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return false
	}
	if len(data) > maxOTLPBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return false
	}
	if isJSON(r) {
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, msg)
	} else {
		err = proto.Unmarshal(data, msg)
	}
	if err != nil {
		http.Error(w, "invalid OTLP body: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// writeOTLP writes an export response in the encoding of the request.
func writeOTLP(w http.ResponseWriter, r *http.Request, msg proto.Message) {
	var (
		data []byte
		err  error
	)
	if isJSON(r) {
		w.Header().Set("Content-Type", "application/json")
		data, err = protojson.Marshal(msg)
	} else {
		w.Header().Set("Content-Type", "application/x-protobuf")
		data, err = proto.Marshal(msg)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(data)
}

// spanUsages extracts token usage from every span that carries any
// gen_ai.usage token attribute.
func spanUsages(req *coltracepb.ExportTraceServiceRequest) []spanUsage {
	var out []spanUsage
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, span := range ss.GetSpans() {
				attrs := attrMap(span.GetAttributes())
				if aggregateOperations[stringAttr(attrs, "gen_ai.operation.name")] {
					continue
				}
				var d tokenDelta
				var found bool
				for _, f := range []struct {
					dst   *int64
					names []string
				}{
					{&d.input, attrInputTokens},
					{&d.output, attrOutputTokens},
					{&d.cacheRead, attrCacheReadTokens},
					{&d.cacheWrite, attrCacheWriteTokens},
					{&d.reasoning, attrReasoningTokens},
				} {
					if v, ok := intAttr(attrs, f.names...); ok {
						*f.dst, found = v, true
					}
				}
				if !found {
					continue
				}
				if v, ok := floatAttr(attrs, "gen_ai.usage.cost.amount"); ok && v > 0 {
					d.costAmount = v
					d.costCurrency = stringAttr(attrs, "gen_ai.usage.cost.currency")
				}
				reqModel := stringAttr(attrs, "gen_ai.request.model")
				respModel := stringAttr(attrs, "gen_ai.response.model")
				out = append(out, spanUsage{
					id:            string(span.GetTraceId()) + string(span.GetSpanId()),
					model:         firstNonEmpty(respModel, reqModel, unknownModel),
					requestModel:  reqModel,
					responseModel: respModel,
					delta:         d,
				})
			}
		}
	}
	return out
}

// metricPoints extracts the data points of the gen_ai.client.token.usage
// histogram (explicit or exponential buckets).
func metricPoints(req *colmetricspb.ExportMetricsServiceRequest) []metricPoint {
	var out []metricPoint
	for _, rm := range req.GetResourceMetrics() {
		resKey := attrKey(rm.GetResource().GetAttributes())
		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope().GetName()
			for _, m := range sm.GetMetrics() {
				if m.GetName() != tokenUsageMetric {
					continue
				}
				type point struct {
					attrs []*commonpb.KeyValue
					start uint64
					sum   float64
				}
				var (
					temporality metricspb.AggregationTemporality
					points      []point
				)
				switch data := m.GetData().(type) {
				case *metricspb.Metric_Histogram:
					temporality = data.Histogram.GetAggregationTemporality()
					for _, p := range data.Histogram.GetDataPoints() {
						points = append(points, point{p.GetAttributes(), p.GetStartTimeUnixNano(), p.GetSum()})
					}
				case *metricspb.Metric_ExponentialHistogram:
					temporality = data.ExponentialHistogram.GetAggregationTemporality()
					for _, p := range data.ExponentialHistogram.GetDataPoints() {
						points = append(points, point{p.GetAttributes(), p.GetStartTimeUnixNano(), p.GetSum()})
					}
				default:
					continue
				}
				for _, p := range points {
					attrs := attrMap(p.attrs)
					reqModel := stringAttr(attrs, "gen_ai.request.model")
					respModel := stringAttr(attrs, "gen_ai.response.model")
					out = append(out, metricPoint{
						stream:        resKey + "|" + scope + "|" + attrKey(p.attrs),
						cumulative:    temporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
						start:         p.start,
						sum:           p.sum,
						tokenType:     stringAttr(attrs, "gen_ai.token.type"),
						model:         firstNonEmpty(respModel, reqModel, unknownModel),
						requestModel:  reqModel,
						responseModel: respModel,
					})
				}
			}
		}
	}
	return out
}

func attrMap(kvs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	m := make(map[string]*commonpb.AnyValue, len(kvs))
	for _, kv := range kvs {
		m[kv.GetKey()] = kv.GetValue()
	}
	return m
}

// attrKey is a stable string identity for a set of attributes.
func attrKey(kvs []*commonpb.KeyValue) string {
	parts := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		parts = append(parts, kv.GetKey()+"="+anyString(kv.GetValue()))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func anyString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	default:
		return fmt.Sprint(v)
	}
}

func stringAttr(attrs map[string]*commonpb.AnyValue, name string) string {
	if v, ok := attrs[name]; ok {
		if s, ok := v.GetValue().(*commonpb.AnyValue_StringValue); ok {
			return s.StringValue
		}
	}
	return ""
}

// floatAttr reads a numeric attribute given as a double, an int, or a
// numeric string.
func floatAttr(attrs map[string]*commonpb.AnyValue, name string) (float64, bool) {
	v, ok := attrs[name]
	if !ok {
		return 0, false
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue, true
	case *commonpb.AnyValue_IntValue:
		return float64(x.IntValue), true
	case *commonpb.AnyValue_StringValue:
		f, err := strconv.ParseFloat(x.StringValue, 64)
		return f, err == nil
	}
	return 0, false
}

// intAttr reads the first present of names as a non-negative token count.
func intAttr(attrs map[string]*commonpb.AnyValue, names ...string) (int64, bool) {
	for _, name := range names {
		f, ok := floatAttr(attrs, name)
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			continue
		}
		if f < 0 {
			f = 0
		}
		return int64(math.Round(f)), true
	}
	return 0, false
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
