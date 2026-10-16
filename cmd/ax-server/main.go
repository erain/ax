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

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/cost"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/store/redis"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/internal/telemetry"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/internal/usage/metrics"
	"github.com/google/ax/internal/usage/puller"
	"github.com/google/ax/pkg/apis/v1alpha1"
	goredis "github.com/redis/go-redis/v9"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "ax-server:", err)
		os.Exit(1)
	}
}

// onListening, when set (tests), is called with the bound API and OTLP
// addresses (otlp is nil when the ingestor is disabled) once both listen.
var onListening func(api, otlp net.Addr)

// shutdownTimeout bounds the graceful shutdown of the API and OTLP listeners;
// metricsFlushTimeout separately bounds the final metrics export, so slow
// in-flight requests cannot eat the flush's budget.
const (
	shutdownTimeout     = 5 * time.Second
	metricsFlushTimeout = 5 * time.Second
)

// zeroRateCard is the built-in rate card used without --rate-card: usage is
// recorded but nothing is priced (tokens are flagged partially unpriced).
const zeroRateCard = "currency: USD\n"

func run(parent context.Context, args []string) error {
	var (
		listenAddr              string
		redisAddr               string
		redisPassword           string
		substrateEndpoint       string
		substrateAuthority      string
		substrateTokenFile      string
		substrateCAFile         string
		substrateInsecureTLS    bool
		substratePlaintext      bool
		defaultTemplate         string
		defaultTemplateAtespace string
		defaultTaskImage        string
		otlpAddr                string
		rateCardPath            string
		usagePullInterval       time.Duration
		runnerPort              int
		summaryInterval         time.Duration
	)

	fs := flag.NewFlagSet("ax-server", flag.ContinueOnError)
	fs.StringVar(&listenAddr, "addr", ":8080", "HTTP listen address")
	fs.StringVar(&redisAddr, "redis-addr", "localhost:6379", "Redis server address (empty: in-memory stores and locks, single replica, for development)")
	fs.StringVar(&redisPassword, "redis-password", "", "Redis password")
	fs.StringVar(&substrateEndpoint, "substrate-endpoint", "api.ate-system.svc.cluster.local:443", "Agent Substrate Control API endpoint")
	fs.StringVar(&substrateAuthority, "substrate-authority", "api.ate-system.svc", "Authority / TLS ServerName for Substrate endpoint")
	fs.StringVar(&substrateTokenFile, "substrate-token-file", "", "Path to bearer token file for Substrate auth")
	fs.StringVar(&substrateCAFile, "substrate-ca-file", "", "Path to CA PEM file for Substrate TLS")
	fs.BoolVar(&substrateInsecureTLS, "substrate-insecure-tls", false, "Skip Substrate TLS verification")
	fs.BoolVar(&substratePlaintext, "substrate-plaintext", false, "Use insecure plaintext gRPC connection to Substrate")
	fs.StringVar(&defaultTemplate, "template", "default-template", "Default Substrate ActorTemplate name")
	fs.StringVar(&defaultTemplateAtespace, "template-atespace", v1alpha1.DefaultSystemNamespace, "Default Substrate ActorTemplate atespace")
	fs.StringVar(&defaultTaskImage, "default-task-image", v1alpha1.DefaultTaskImage, "Task runner image for tasks that do not set spec.image")
	fs.StringVar(&otlpAddr, "otlp-addr", ":4318", "OTLP/HTTP listen address for Substrate usage events (POST /v1/logs); empty disables the ingestor")
	fs.StringVar(&rateCardPath, "rate-card", "", "Path to the rate card YAML (reloaded on change); empty uses a built-in zero-price card")
	fs.DurationVar(&usagePullInterval, "usage-pull-interval", puller.DefaultInterval, "How often runner token usage is pulled from Running tasks; 0 disables periodic pulls (Suspend/Delete still flush)")
	fs.IntVar(&runnerPort, "runner-port", puller.DefaultPort, "Port of the task runner's GET /usage endpoint")
	fs.DurationVar(&summaryInterval, "summary-interval", metrics.DefaultSummaryInterval, "How often ax.task.usage_summary lines are logged for Running tasks (budget/Idle conditions are re-checked too); 0 disables")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if envAddr := os.Getenv("ADDR"); envAddr != "" {
		listenAddr = envAddr
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		redisAddr = envRedis
	}
	if envPass := os.Getenv("REDIS_PASSWORD"); envPass != "" {
		redisPassword = envPass
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	slog.Info("starting ax-server",
		"listenAddr", listenAddr,
		"redisAddr", redisAddr,
		"substrateEndpoint", substrateEndpoint,
		"template", defaultTemplate,
		"otlpAddr", otlpAddr,
		"rateCard", rateCardPath,
	)

	ctx, cancel := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		taskStore  store.Store
		locker     lock.Locker
		usageStore usage.Store
	)
	if redisAddr == "" {
		slog.Warn("no --redis-addr: using in-memory stores (state is lost on restart; run a single replica only)")
		taskStore = memory.NewStore()
		locker = lock.NewMemoryLocker()
		usageStore = usage.NewMemoryStore()
	} else {
		rClient := goredis.NewClient(&goredis.Options{
			Addr:     redisAddr,
			Password: redisPassword,
		})
		defer rClient.Close()
		taskStore = redis.NewStore(rClient, redis.Options{})
		locker = lock.NewRedisLocker(rClient, lock.RedisLockerOptions{})
		usageStore = usage.NewRedisStore(rClient, "")
	}

	var pricer *cost.Pricer
	if rateCardPath == "" {
		card, err := cost.Parse([]byte(zeroRateCard))
		if err != nil {
			return fmt.Errorf("built-in rate card: %w", err)
		}
		pricer = cost.NewPricer(card)
		slog.Warn("no --rate-card: usage is recorded but not priced")
	} else {
		var err error
		if pricer, err = cost.NewFilePricer(rateCardPath); err != nil {
			return fmt.Errorf("loading rate card: %w", err)
		}
	}

	mp, shutdownMetrics, err := telemetry.SetupMeterProvider(ctx)
	if err != nil {
		slog.Warn("OTel metrics disabled", "error", err)
	}
	labels := server.NewLabelCache(taskStore, server.DefaultLabelCacheTTL)
	recorder, err := metrics.New(mp.Meter(metrics.ScopeName),
		func() []string { return pricer.RateCard().MetricLabels },
		labels,
		metrics.WithKnownModels(func(m string) bool { _, ok := pricer.RateCard().ModelRates(m); return ok }))
	if err != nil {
		return fmt.Errorf("creating usage metrics: %w", err)
	}

	var reconciler server.Reconciler
	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:      substrateEndpoint,
		Authority:   substrateAuthority,
		TokenFile:   substrateTokenFile,
		CAFile:      substrateCAFile,
		InsecureTLS: substrateInsecureTLS,
		Plaintext:   substratePlaintext,
	})
	if err != nil {
		slog.Warn("could not initialize substrate client; running without substrate reconciliation", "error", err)
	} else {
		defer subClient.Close()
		tr := controller.NewTaskReconciler(subClient, defaultTemplate, defaultTemplateAtespace)
		tr.DefaultImage = defaultTaskImage
		reconciler = tr
	}

	srv := server.NewServer(taskStore, server.Options{
		Locker:     locker,
		Reconciler: reconciler,
		Usage: &server.UsageOptions{
			Store:    usageStore,
			Pricer:   pricer,
			RateCard: pricer.RateCard,
			Metrics:  recorder,
			Labels:   labels,
			Summary:  metrics.NewSummaryLogger(logger),
			Logger:   logger,
		},
	})
	// The puller also serves Flush (before Suspend/Delete), so it is created
	// even when periodic pulls are disabled.
	usagePuller := srv.NewUsagePuller(puller.Options{Interval: usagePullInterval, Port: runnerPort})

	apiLn, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", listenAddr, err)
	}
	httpServer := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetHTTP1(true)
	httpServer.Protocols.SetUnencryptedHTTP2(true)

	errCh := make(chan error, 2)
	serve := func(name string, hs *http.Server, ln net.Listener) {
		if err := hs.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("%s server: %w", name, err)
		}
	}
	go serve("API", httpServer, apiLn)

	var otlpServer *http.Server
	var otlpBound net.Addr
	if otlpAddr != "" {
		otlpLn, err := net.Listen("tcp", otlpAddr)
		if err != nil {
			_ = httpServer.Close()
			return fmt.Errorf("listening on %s: %w", otlpAddr, err)
		}
		otlpBound = otlpLn.Addr()
		otlpServer = &http.Server{
			Handler:           srv.IngestHandler(),
			ReadHeaderTimeout: 10 * time.Second,
		}
		go serve("OTLP", otlpServer, otlpLn)
		slog.Info("usage ingestor listening", "addr", otlpBound.String())
	} else {
		slog.Warn("usage ingestor disabled (--otlp-addr is empty)")
	}

	var wg sync.WaitGroup
	if usagePullInterval > 0 {
		wg.Go(func() { _ = usagePuller.Run(ctx) })
	}
	if summaryInterval > 0 {
		wg.Go(func() { srv.RunUsageSummaries(ctx, summaryInterval) })
	}
	if onListening != nil {
		onListening(apiLn.Addr(), otlpBound)
	}

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
		slog.Error("ax-server failed", "error", runErr)
	}
	slog.Info("shutting down ax-server")
	cancel()

	// Both listeners drain concurrently under one deadline.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	var swg sync.WaitGroup
	swg.Go(func() { _ = httpServer.Shutdown(shutdownCtx) })
	if otlpServer != nil {
		swg.Go(func() { _ = otlpServer.Shutdown(shutdownCtx) })
	}
	swg.Wait()
	shutdownCancel()
	wg.Wait()

	flushCtx, flushCancel := context.WithTimeout(context.Background(), metricsFlushTimeout)
	defer flushCancel()
	if err := shutdownMetrics(flushCtx); err != nil {
		slog.Warn("flushing metrics on shutdown failed", "error", err)
	}
	return runErr
}
