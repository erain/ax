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

// Package puller periodically pulls cumulative token usage reports from the
// task runners (GET http://<workerIP>:<port>/usage) and folds them into the
// usage store, pricing every increment and notifying hooks.
package puller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

const (
	// DefaultInterval is the sweep period.
	DefaultInterval = 60 * time.Second
	// DefaultPort is the runner probe port serving GET /usage.
	DefaultPort = 80
	// DefaultTimeout bounds one pull (HTTP request and store update).
	DefaultTimeout = 5 * time.Second
	// DefaultConcurrency bounds the number of concurrent pulls in a sweep.
	DefaultConcurrency = 8
	// UsagePath is the runner endpoint serving the cumulative usage report.
	UsagePath = "/usage"
	// PhaseRunning is the only task phase whose runner is contacted.
	PhaseRunning = "Running"

	// maxReportBytes caps the size of a usage report body.
	maxReportBytes = 1 << 20
)

// TaskLister lists the tasks the puller may contact. Implementations should
// return tasks with phase Running; the puller re-checks the phase and the
// worker IP and skips anything else.
type TaskLister interface {
	ListRunningTasks(ctx context.Context) ([]*v1alpha1.Task, error)
}

// Options configures a Puller. Zero values select the defaults.
type Options struct {
	// Tasks lists the running tasks to pull (required for Run/Sweep).
	Tasks TaskLister
	// Store persists usage (required).
	Store usage.Store
	// Pricer prices each increment; nil charges nothing.
	Pricer usage.Pricer
	// Hooks is notified of every non-zero increment; may be nil.
	Hooks usage.Hooks
	// Client performs the HTTP requests; defaults to a fresh http.Client.
	Client *http.Client
	// Interval is the sweep period (default 60s).
	Interval time.Duration
	// Port is the runner probe port (default 80).
	Port int
	// Timeout bounds one pull, including the store update (default 5s).
	Timeout time.Duration
	// Concurrency bounds concurrent pulls per sweep (default 8).
	Concurrency int
	// Now is the clock used for the pull time (default time.Now).
	Now func() time.Time
	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// Stats are cumulative puller counters.
type Stats struct {
	// Sweeps is the number of completed sweeps.
	Sweeps int64
	// Pulls is the number of runner pulls attempted.
	Pulls int64
	// Failures is the number of pulls that failed (HTTP, decode or store).
	Failures int64
	// Applied is the number of pulls that recorded a non-zero increment.
	Applied int64
}

// Puller pulls runner usage reports into the usage store.
type Puller struct {
	o Options

	sweeps, pulls, failures, applied atomic.Int64
}

// New returns a Puller with defaults applied.
func New(o Options) *Puller {
	if o.Client == nil {
		o.Client = &http.Client{}
	}
	if o.Interval <= 0 {
		o.Interval = DefaultInterval
	}
	if o.Port <= 0 {
		o.Port = DefaultPort
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Puller{o: o}
}

// Stats returns a snapshot of the counters.
func (p *Puller) Stats() Stats {
	return Stats{
		Sweeps:   p.sweeps.Load(),
		Pulls:    p.pulls.Load(),
		Failures: p.failures.Load(),
		Applied:  p.applied.Load(),
	}
}

// Run sweeps immediately and then every Interval until ctx is done. Sweeps
// never overlap. It returns ctx.Err().
func (p *Puller) Run(ctx context.Context) error {
	t := time.NewTicker(p.o.Interval)
	defer t.Stop()
	for {
		p.Sweep(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Sweep pulls every Running task with a worker IP once, with bounded
// concurrency, and waits for all pulls to finish. Failures are counted and
// logged at debug level.
func (p *Puller) Sweep(ctx context.Context) {
	if p.o.Tasks == nil {
		return
	}
	tasks, err := p.o.Tasks.ListRunningTasks(ctx)
	if err != nil {
		p.o.Logger.DebugContext(ctx, "usage puller: listing running tasks failed", "error", err)
		return
	}
	sem := make(chan struct{}, p.o.Concurrency)
	var wg sync.WaitGroup
	for _, task := range tasks {
		if !pullable(task) {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			p.pullLogged(ctx, task)
		}()
	}
	wg.Wait()
	p.sweeps.Add(1)
}

// Flush synchronously pulls one task (used before Suspend/Delete). It returns
// within Timeout. A task that is not Running or has no worker IP is not
// contacted and Flush returns nil. Errors are logged; callers should not
// fail their operation on them.
func (p *Puller) Flush(ctx context.Context, task *v1alpha1.Task) error {
	if !pullable(task) {
		return nil
	}
	return p.pullLogged(ctx, task)
}

func (p *Puller) pullLogged(ctx context.Context, task *v1alpha1.Task) error {
	p.pulls.Add(1)
	err := p.pull(ctx, task)
	if err != nil {
		p.failures.Add(1)
		p.o.Logger.DebugContext(ctx, "usage puller: pull failed",
			"atespace", task.GetMetadata().GetAtespace(),
			"task", task.GetMetadata().GetName(),
			"workerIP", task.GetStatus().GetWorkerIp(),
			"error", err)
	}
	return err
}

// pullable reports whether the task's runner may be contacted.
func pullable(task *v1alpha1.Task) bool {
	return task.GetMetadata().GetName() != "" &&
		task.GetStatus().GetPhase() == PhaseRunning &&
		task.GetStatus().GetWorkerIp() != ""
}

func (p *Puller) pull(ctx context.Context, task *v1alpha1.Task) error {
	ctx, cancel := context.WithTimeout(ctx, p.o.Timeout)
	defer cancel()

	r, err := p.fetch(ctx, task.GetStatus().GetWorkerIp())
	if err != nil {
		return err
	}
	now := p.o.Now()
	key := usage.Key{Atespace: task.GetMetadata().GetAtespace(), Name: task.GetMetadata().GetName()}

	var (
		d usage.Delta
		c usage.Cost
		u *usage.TaskUsage
	)
	err = p.o.Store.Update(ctx, key, func(tu *usage.TaskUsage) error {
		d, c, u = usage.Delta{}, usage.Cost{}, nil
		prevReport := tu.State.RunnerReportTime
		d = tu.ApplyRunnerReport(r, now)
		if d.IsZero() && tu.State.RunnerReportTime.Equal(prevReport) {
			return usage.ErrNoChange // stale or duplicate report
		}
		c = tu.ChargeDelta(d, p.o.Pricer, tu.State.Pool)
		u = tu
		return nil
	})
	if err != nil {
		return fmt.Errorf("updating usage: %w", err)
	}
	if d.IsZero() {
		return nil
	}
	p.applied.Add(1)
	if p.o.Hooks != nil && u != nil {
		p.o.Hooks.OnDelta(ctx, key, d, c, u)
	}
	return nil
}

// report mirrors the runner's GET /usage JSON (runner.UsageReport).
type report struct {
	Session        string                  `json:"session"`
	ReportTime     time.Time               `json:"reportTime"`
	CommandExited  bool                    `json:"commandExited"`
	ExitCode       int                     `json:"exitCode"`
	WorkspaceBytes int64                   `json:"workspaceBytes"`
	Tokens         map[string]*tokenReport `json:"tokens"`
}

type tokenReport struct {
	Input        int64   `json:"input"`
	Output       int64   `json:"output"`
	CacheRead    int64   `json:"cacheRead"`
	CacheWrite   int64   `json:"cacheWrite"`
	Reasoning    int64   `json:"reasoning"`
	CostAmount   float64 `json:"costAmount"`
	CostCurrency string  `json:"costCurrency"`
}

var errBadReport = errors.New("invalid usage report")

func (p *Puller) fetch(ctx context.Context, workerIP string) (usage.RunnerReport, error) {
	url := "http://" + net.JoinHostPort(workerIP, strconv.Itoa(p.o.Port)) + UsagePath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return usage.RunnerReport{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.o.Client.Do(req)
	if err != nil {
		return usage.RunnerReport{}, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxReportBytes))
		return usage.RunnerReport{}, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReportBytes+1))
	if err != nil {
		return usage.RunnerReport{}, fmt.Errorf("reading %s: %w", url, err)
	}
	if len(body) > maxReportBytes {
		return usage.RunnerReport{}, fmt.Errorf("%w: body exceeds %d bytes", errBadReport, maxReportBytes)
	}
	return decodeReport(body)
}

// decodeReport parses a runner usage report and converts it to the
// accounting input. Negative counters are rejected.
func decodeReport(body []byte) (usage.RunnerReport, error) {
	var rep report
	if err := json.Unmarshal(body, &rep); err != nil {
		return usage.RunnerReport{}, fmt.Errorf("%w: %v", errBadReport, err)
	}
	if rep.WorkspaceBytes < 0 {
		return usage.RunnerReport{}, fmt.Errorf("%w: negative workspaceBytes", errBadReport)
	}
	out := usage.RunnerReport{
		Session:        rep.Session,
		ReportTime:     rep.ReportTime,
		CommandExited:  rep.CommandExited,
		ExitCode:       rep.ExitCode,
		WorkspaceBytes: rep.WorkspaceBytes,
	}
	if len(rep.Tokens) > 0 {
		out.Tokens = make(map[string]usage.RunnerTokens, len(rep.Tokens))
	}
	for model, t := range rep.Tokens {
		if t == nil {
			continue
		}
		if t.Input < 0 || t.Output < 0 || t.CacheRead < 0 || t.CacheWrite < 0 || t.Reasoning < 0 || t.CostAmount < 0 {
			return usage.RunnerReport{}, fmt.Errorf("%w: negative counter for model %q", errBadReport, model)
		}
		out.Tokens[model] = usage.RunnerTokens{
			Input:        t.Input,
			Output:       t.Output,
			CacheRead:    t.CacheRead,
			CacheWrite:   t.CacheWrite,
			Reasoning:    t.Reasoning,
			CostAmount:   t.CostAmount,
			CostCurrency: t.CostCurrency,
		}
	}
	return out, nil
}
