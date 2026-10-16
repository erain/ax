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

// Package telemetry sets up OpenTelemetry for ax-server.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// ServiceName is the default service.name resource attribute.
const ServiceName = "ax-server"

// DefaultExportInterval is the metric export interval when
// OTEL_METRIC_EXPORT_INTERVAL is not set.
const DefaultExportInterval = 30 * time.Second

// Environment variables consulted by SetupMeterProvider (the OTLP exporter
// additionally reads the standard OTEL_EXPORTER_OTLP_* variables itself:
// headers, timeout, compression, TLS, ...).
const (
	EnvEndpoint        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	EnvMetricsEndpoint = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
	EnvMetricsExporter = "OTEL_METRICS_EXPORTER"
	EnvExportInterval  = "OTEL_METRIC_EXPORT_INTERVAL"
)

// SetupMeterProvider creates the ax-server MeterProvider and installs it as
// the global provider (otel.SetMeterProvider).
//
// When OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_METRICS_ENDPOINT) is
// set and OTEL_METRICS_EXPORTER is not "none", metrics are exported with the
// OTLP/HTTP exporter (e.g. http://otel-collector.ax-system:4318, path
// /v1/metrics appended per the OTLP spec) by a periodic reader with
// cumulative temporality (what Managed Prometheus expects), every
// OTEL_METRIC_EXPORT_INTERVAL milliseconds (default 30s). The resource has
// service.name=ax-server (OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES
// override it). Otherwise a no-op provider is returned.
//
// shutdown flushes pending metrics and stops the exporter; call it on server
// exit with a bounded context. It is never nil.
func SetupMeterProvider(ctx context.Context) (metric.MeterProvider, func(context.Context) error, error) {
	mp, shutdown, err := newMeterProvider(ctx, os.Getenv)
	if err != nil {
		return nil, nil, err
	}
	otel.SetMeterProvider(mp)
	return mp, shutdown, nil
}

func noShutdown(context.Context) error { return nil }

// newMeterProvider is SetupMeterProvider without the global side effect;
// getenv is consulted for the enable decision and the interval.
func newMeterProvider(ctx context.Context, getenv func(string) string, extra ...sdkmetric.Option) (metric.MeterProvider, func(context.Context) error, error) {
	if !exportEnabled(getenv) {
		return noop.NewMeterProvider(), noShutdown, nil
	}
	interval := exportInterval(getenv)
	exp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithTemporalitySelector(sdkmetric.DefaultTemporalitySelector)) // cumulative
	if err != nil {
		return nil, nil, fmt.Errorf("telemetry: OTLP metric exporter: %w", err)
	}
	res, err := newResource(ctx)
	if err != nil {
		return nil, nil, err
	}
	opts := append([]sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(interval))),
	}, extra...)
	mp := sdkmetric.NewMeterProvider(opts...)
	slog.Info("OTel metrics export enabled", "interval", interval)
	return mp, mp.Shutdown, nil
}

// exportEnabled reports whether an OTLP metrics endpoint is configured.
func exportEnabled(getenv func(string) string) bool {
	if strings.EqualFold(strings.TrimSpace(getenv(EnvMetricsExporter)), "none") {
		return false
	}
	return strings.TrimSpace(getenv(EnvEndpoint)) != "" || strings.TrimSpace(getenv(EnvMetricsEndpoint)) != ""
}

// exportInterval parses OTEL_METRIC_EXPORT_INTERVAL (milliseconds). Unset or
// invalid values give DefaultExportInterval (invalid ones with a warning).
func exportInterval(getenv func(string) string) time.Duration {
	v := strings.TrimSpace(getenv(EnvExportInterval))
	if v == "" {
		return DefaultExportInterval
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil || ms <= 0 {
		slog.Warn("ignoring invalid "+EnvExportInterval+"; want positive milliseconds", "value", v, "default", DefaultExportInterval)
		return DefaultExportInterval
	}
	return time.Duration(ms) * time.Millisecond
}

// newResource returns service.name=ax-server and service.instance.id=<host
// name> (the pod name in Kubernetes, so replicas write distinct series)
// merged with the environment (OTEL_RESOURCE_ATTRIBUTES, OTEL_SERVICE_NAME
// win) and the SDK attributes.
func newResource(ctx context.Context) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{attribute.String("service.name", ServiceName)}
	if h, err := os.Hostname(); err == nil && h != "" {
		attrs = append(attrs, attribute.String("service.instance.id", h))
	}
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}
	return res, nil
}
