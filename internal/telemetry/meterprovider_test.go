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

package telemetry

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestNewMeterProvider_NoEndpointIsNoop(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"unset":         {},
		"blank":         {EnvEndpoint: "  "},
		"exporter none": {EnvEndpoint: "http://collector:4318", EnvMetricsExporter: "none"},
	} {
		t.Run(name, func(t *testing.T) {
			mp, shutdown, err := newMeterProvider(context.Background(), envMap(env))
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := mp.(noop.MeterProvider); !ok {
				t.Errorf("provider = %T, want noop.MeterProvider", mp)
			}
			if shutdown == nil || shutdown(context.Background()) != nil {
				t.Error("shutdown must be non-nil and succeed")
			}
		})
	}
}

func TestExportInterval(t *testing.T) {
	for v, want := range map[string]time.Duration{
		"":      30 * time.Second,
		"5000":  5 * time.Second,
		" 100 ": 100 * time.Millisecond,
		"0":     30 * time.Second,
		"-1":    30 * time.Second,
		"10s":   30 * time.Second,
	} {
		if got := exportInterval(envMap(map[string]string{EnvExportInterval: v})); got != want {
			t.Errorf("exportInterval(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestExportEnabled(t *testing.T) {
	for _, tc := range []struct {
		env  map[string]string
		want bool
	}{
		{map[string]string{}, false},
		{map[string]string{EnvEndpoint: "http://c:4318"}, true},
		{map[string]string{EnvMetricsEndpoint: "http://c:4318/v1/metrics"}, true},
		{map[string]string{EnvEndpoint: "http://c:4318", EnvMetricsExporter: "NONE"}, false},
		{map[string]string{EnvEndpoint: "http://c:4318", EnvMetricsExporter: "otlp"}, true},
	} {
		if got := exportEnabled(envMap(tc.env)); got != tc.want {
			t.Errorf("exportEnabled(%v) = %v, want %v", tc.env, got, tc.want)
		}
	}
}

// TestSetupMeterProvider_ExportsOTLPHTTP checks the enabled path end to end:
// OTLP/HTTP protobuf POST to <endpoint>/v1/metrics, resource
// service.name=ax-server, cumulative monotonic sums with units.
func TestSetupMeterProvider_ExportsOTLPHTTP(t *testing.T) {
	var (
		mu   sync.Mutex
		reqs []*colmetricspb.ExportMetricsServiceRequest
		path string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		req := &colmetricspb.ExportMetricsServiceRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			t.Errorf("unmarshal: %v", err)
		}
		mu.Lock()
		reqs = append(reqs, req)
		path = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		out, _ := proto.Marshal(&colmetricspb.ExportMetricsServiceResponse{})
		_, _ = w.Write(out)
	}))
	defer srv.Close()

	t.Setenv(EnvEndpoint, srv.URL)
	t.Setenv(EnvExportInterval, "3600000") // only the shutdown flush exports
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	t.Setenv("OTEL_SERVICE_NAME", "")

	prev := otel.GetMeterProvider()
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	ctx := context.Background()
	mp, shutdown, err := SetupMeterProvider(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, err := mp.Meter("test").Float64Counter("ax.task.cost")
	if err != nil {
		t.Fatal(err)
	}
	c.Add(ctx, 1.5)
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if otel.GetMeterProvider() != mp {
		t.Error("SetupMeterProvider did not install the global provider")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(reqs) == 0 {
		t.Fatal("collector received no export")
	}
	if path != "/v1/metrics" {
		t.Errorf("path = %q, want /v1/metrics", path)
	}
	var found bool
	for _, req := range reqs {
		for _, rm := range req.GetResourceMetrics() {
			svc := ""
			for _, kv := range rm.GetResource().GetAttributes() {
				if kv.GetKey() == "service.name" {
					svc = kv.GetValue().GetStringValue()
				}
			}
			if svc != ServiceName {
				t.Errorf("service.name = %q, want %q", svc, ServiceName)
			}
			for _, sm := range rm.GetScopeMetrics() {
				for _, m := range sm.GetMetrics() {
					if m.GetName() != "ax.task.cost" {
						continue
					}
					found = true
					sum := m.GetSum()
					if sum == nil || !sum.GetIsMonotonic() ||
						sum.GetAggregationTemporality() != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
						t.Errorf("metric = %v, want cumulative monotonic sum", m)
					}
					if len(sum.GetDataPoints()) != 1 || sum.GetDataPoints()[0].GetAsDouble() != 1.5 {
						t.Errorf("data points = %v, want one point 1.5", sum.GetDataPoints())
					}
				}
			}
		}
	}
	if !found {
		t.Error("ax.task.cost not exported")
	}
}
