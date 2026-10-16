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
// task runners (GET http://<workerIP>:<port>/usage, or through the Substrate
// atenet-router with the ate-target-actor header) and folds them into the
// usage store, pricing every increment and notifying hooks.
package puller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
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
	// TargetActorHeader selects the actor when pulling through atenet-router.
	TargetActorHeader = "ate-target-actor"
	// PhaseRunning is the only task phase whose runner is contacted.
	PhaseRunning = "Running"

	// MaxClockSkew is how far in the future a runner reportTime may be; a
	// later one is replaced by the pull time so a skewed or malicious sandbox
	// cannot freeze its runner accounting.
	MaxClockSkew = 2 * time.Minute
	// hookTimeout bounds one Hooks.OnDelta call.
	hookTimeout = 5 * time.Second

	// maxReportBytes caps the size of a usage report body.
	maxReportBytes = 1 << 20
)

// TaskLister lists the tasks the puller may contact. Implementations should
// return tasks with phase Running; the puller re-checks the phase and the
// worker IP and skips anything else.
type TaskLister interface {
	ListRunningTasks(ctx context.Context) ([]*v1alpha1.Task, error)
}

// TaskGetter re-reads one task. In router mode the puller re-reads the task
// immediately before contacting the router (sweep and Flush) and skips the
// pull unless it is still Running on the same worker IP, because a router
// request resumes a suspended actor. A missing task is reported as (nil, nil).
type TaskGetter interface {
	LookupTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
}

// Options configures a Puller. Zero values select the defaults.
type Options struct {
	// Tasks lists the running tasks to pull (required for Run/Sweep).
	Tasks TaskLister
	// Getter re-reads a task before a router pull (see TaskGetter). Nil =
	// Tasks if it implements TaskGetter, else no re-read.
	Getter TaskGetter
	// Store persists usage (required).
	Store usage.Store
	// Pricer prices each increment; nil charges nothing.
	Pricer usage.Pricer
	// Hooks is notified of every non-zero increment; may be nil.
	Hooks usage.Hooks
	// Client performs the HTTP requests. The default uses a dedicated
	// transport without proxy. A supplied client is copied and its
	// CheckRedirect is always replaced so redirects are never followed (a
	// 3xx counts as a failed pull); its Transport is used as is.
	Client *http.Client
	// Interval is the sweep period (default 60s).
	Interval time.Duration
	// Port is the runner probe port (default 80). Ignored with RouterAddr.
	Port int
	// RouterAddr, if set (host:port), sends pulls through the Substrate
	// atenet-router (GET http://<RouterAddr>/usage with header
	// ate-target-actor: <atespace>/<task>) instead of to <workerIP>:<Port>.
	// Substrate v0.5.0 workers do not expose actor ports on the pod IP (the
	// router reaches actors through the worker's atunnel on :443), so this is
	// required there. The router routes plain HTTP to actor port 80 and
	// resumes a suspended actor, so only Running tasks are ever pulled, and
	// right before each request the task is re-read (Getter) and the usage
	// record's last actor state is checked (TaskUsage.ActorStopped).
	RouterAddr string
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
	// Skipped is the number of router pulls dropped by the pre-request
	// re-check (task no longer Running on the same worker, or the actor
	// reported stopped); they are not counted in Pulls.
	Skipped int64
}

// Puller pulls runner usage reports into the usage store.
type Puller struct {
	o Options

	sweeps, pulls, failures, applied, skipped atomic.Int64
}

// New returns a Puller with defaults applied.
func New(o Options) *Puller {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Getter == nil {
		o.Getter, _ = o.Tasks.(TaskGetter)
	}
	if o.Client == nil {
		o.Client = &http.Client{Transport: newTransport(o.Concurrency)}
	} else {
		c := *o.Client
		o.Client = &c
	}
	// The runner lives in the sandbox next to untrusted code: never let it
	// steer ax-server's request elsewhere.
	o.Client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if o.Interval <= 0 {
		o.Interval = DefaultInterval
	}
	if o.Port <= 0 || o.Port > 65535 {
		o.Port = DefaultPort
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return &Puller{o: o}
}

// newTransport returns the default transport: no proxy, bounded handshakes,
// and up to concurrency idle connections per host. Direct pulls reach a
// different host (worker IP) per task; router pulls all go to one host, so
// keeping one idle connection per concurrent pull lets sweeps reuse them.
func newTransport(concurrency int) *http.Transport {
	return &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: DefaultTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          max(64, concurrency),
		MaxIdleConnsPerHost:   concurrency,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: DefaultTimeout,
		DisableCompression:    true,
	}
}

// Stats returns a snapshot of the counters.
func (p *Puller) Stats() Stats {
	return Stats{
		Sweeps:   p.sweeps.Load(),
		Pulls:    p.pulls.Load(),
		Failures: p.failures.Load(),
		Applied:  p.applied.Load(),
		Skipped:  p.skipped.Load(),
	}
}

// Run sweeps immediately and then every Interval until ctx is done. Sweeps
// never overlap. It returns ctx.Err().
func (p *Puller) Run(ctx context.Context) error {
	p.o.Logger.InfoContext(ctx, "usage puller started", "interval", p.o.Interval.String(), "port", p.o.Port, "router", p.o.RouterAddr)
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
// logged at debug level; a sweep that contacted at least one runner logs one
// INFO summary line (pulled/failed/applied and the last error).
func (p *Puller) Sweep(ctx context.Context) {
	if p.o.Tasks == nil {
		return
	}
	tasks, err := p.o.Tasks.ListRunningTasks(ctx)
	if err != nil {
		p.o.Logger.WarnContext(ctx, "usage puller: listing running tasks failed", "error", err)
		return
	}
	sem := make(chan struct{}, p.o.Concurrency)
	var (
		wg                               sync.WaitGroup
		pulled, failed, applied, skipped atomic.Int64
		lastErrMu                        sync.Mutex
		lastErr                          error
		lastErrTask, lastErrIP           string
	)
	defer func() {
		// One line per sweep that contacted runners (or skipped a router
		// pull), so pull failures are visible without debug logging.
		if n, sk := pulled.Load(), skipped.Load(); n > 0 || sk > 0 {
			attrs := []any{"pulled", n, "failed", failed.Load(), "applied", applied.Load()}
			if sk > 0 {
				attrs = append(attrs, "skipped", sk)
			}
			if lastErr != nil {
				attrs = append(attrs, "lastErrorTask", lastErrTask, "lastErrorWorkerIP", lastErrIP, "lastError", lastErr.Error())
			}
			p.o.Logger.InfoContext(ctx, "usage puller sweep", attrs...)
		}
	}()
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
			ok, skip, err := p.pullLogged(ctx, task)
			if skip {
				skipped.Add(1)
				return
			}
			pulled.Add(1)
			if err != nil {
				failed.Add(1)
				lastErrMu.Lock()
				lastErr = err
				lastErrTask = task.GetMetadata().GetAtespace() + "/" + task.GetMetadata().GetName()
				lastErrIP = task.GetStatus().GetWorkerIp()
				lastErrMu.Unlock()
			} else if ok {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	p.sweeps.Add(1)
}

// Flush synchronously pulls one task (used before Suspend/Delete, while the
// task is still Running). It returns within Timeout. A task that is not
// Running or has no worker IP is not contacted and Flush returns nil, as is a
// router pull dropped by the re-check. Errors are logged; callers should not
// fail their operation on them.
func (p *Puller) Flush(ctx context.Context, task *v1alpha1.Task) error {
	if !pullable(task) {
		return nil
	}
	_, _, err := p.pullLogged(ctx, task)
	return err
}

// errSkipped reports a pull dropped by the router-mode re-check.
var errSkipped = errors.New("pull skipped")

func (p *Puller) pullLogged(ctx context.Context, task *v1alpha1.Task) (applied, skipped bool, err error) {
	applied, err = p.pull(ctx, task)
	if errors.Is(err, errSkipped) {
		p.skipped.Add(1)
		p.o.Logger.DebugContext(ctx, "usage puller: pull skipped",
			"atespace", task.GetMetadata().GetAtespace(),
			"task", task.GetMetadata().GetName(),
			"workerIP", task.GetStatus().GetWorkerIp(),
			"reason", err)
		return false, true, nil
	}
	p.pulls.Add(1)
	if err != nil {
		p.failures.Add(1)
		p.o.Logger.DebugContext(ctx, "usage puller: pull failed",
			"atespace", task.GetMetadata().GetAtespace(),
			"task", task.GetMetadata().GetName(),
			"workerIP", task.GetStatus().GetWorkerIp(),
			"error", err)
	}
	return applied, false, err
}

// pullable reports whether the task's runner may be contacted.
func pullable(task *v1alpha1.Task) bool {
	return task.GetMetadata().GetName() != "" &&
		task.GetStatus().GetPhase() == PhaseRunning &&
		task.GetStatus().GetWorkerIp() != ""
}

func (p *Puller) pull(parent context.Context, task *v1alpha1.Task) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, p.o.Timeout)
	defer cancel()

	if p.o.RouterAddr != "" {
		if err := p.recheck(ctx, task); err != nil {
			return false, err
		}
	}
	r, err := p.fetch(ctx, task)
	if err != nil {
		return false, err
	}
	now := p.o.Now()
	if r.ReportTime.After(now.Add(MaxClockSkew)) {
		r.ReportTime = now
	}
	key := usage.Key{Atespace: task.GetMetadata().GetAtespace(), Name: task.GetMetadata().GetName()}

	var (
		d usage.Delta
		c usage.Cost
		u *usage.TaskUsage
	)
	err = p.o.Store.Update(ctx, key, func(tu *usage.TaskUsage) error {
		d, c, u = usage.Delta{}, usage.Cost{}, nil
		before, err := json.Marshal(tu)
		if err != nil {
			return err
		}
		d = tu.ApplyRunnerReport(r, now)
		c = tu.ChargeDelta(d, p.o.Pricer, tu.State.Pool)
		u = tu
		// Apply* may advance accounting state with a zero Delta, so the
		// record is persisted unless it is byte-identical.
		if d.IsZero() {
			after, err := json.Marshal(tu)
			if err != nil {
				return err
			}
			if bytes.Equal(before, after) {
				return usage.ErrNoChange
			}
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("updating usage: %w", err)
	}
	if d.IsZero() {
		return false, nil
	}
	p.applied.Add(1)
	if p.o.Hooks != nil && u != nil {
		// Hooks get their own budget: the pull's deadline may be nearly
		// spent, and a canceled caller must not drop a committed delta.
		hctx, hcancel := context.WithTimeout(context.WithoutCancel(parent), hookTimeout)
		defer hcancel()
		p.o.Hooks.OnDelta(hctx, key, d, c, u)
	}
	return true, nil
}

// recheck runs right before a router request, which would resume a
// suspended actor: the task is re-read (one store GET, if a Getter is
// configured) and must still be Running on the snapshot's worker IP, and the
// usage record's last actor state must not be a closing one. It returns an
// error wrapping errSkipped to drop the pull, or a read error.
func (p *Puller) recheck(ctx context.Context, task *v1alpha1.Task) error {
	atespace, name := task.GetMetadata().GetAtespace(), task.GetMetadata().GetName()
	if p.o.Getter != nil {
		cur, err := p.o.Getter.LookupTask(ctx, atespace, name)
		if err != nil {
			return fmt.Errorf("re-reading task: %w", err)
		}
		switch {
		case cur == nil:
			return fmt.Errorf("%w: task deleted", errSkipped)
		case cur.GetStatus().GetPhase() != PhaseRunning:
			return fmt.Errorf("%w: task phase %q", errSkipped, cur.GetStatus().GetPhase())
		case cur.GetStatus().GetWorkerIp() != task.GetStatus().GetWorkerIp():
			return fmt.Errorf("%w: worker IP changed to %q", errSkipped, cur.GetStatus().GetWorkerIp())
		}
	}
	u, err := p.o.Store.Get(ctx, usage.Key{Atespace: atespace, Name: name})
	switch {
	case errors.Is(err, usage.ErrNotFound):
		// No actor events recorded yet.
	case err != nil:
		return fmt.Errorf("reading usage: %w", err)
	case u.ActorStopped():
		return fmt.Errorf("%w: actor stopped (last state %q)", errSkipped, u.State.Marks[len(u.State.Marks)-1].State)
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

var errBadWorkerIP = errors.New("invalid worker IP")

// checkWorkerIP accepts only literal unicast IPs; hostnames, unspecified,
// link-local (169.254.0.0/16 incl. the metadata server, fe80::/10), multicast
// and zoned addresses are rejected. Loopback is allowed (tests).
func checkWorkerIP(s string) (netip.Addr, error) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("%w %q: %v", errBadWorkerIP, s, err)
	}
	if a.Zone() != "" {
		return netip.Addr{}, fmt.Errorf("%w %q: zoned address", errBadWorkerIP, s)
	}
	a = a.Unmap()
	if a.IsUnspecified() || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() ||
		a.IsInterfaceLocalMulticast() || a.IsMulticast() {
		return netip.Addr{}, fmt.Errorf("%w %q: not a routable unicast address", errBadWorkerIP, s)
	}
	return a, nil
}

func (p *Puller) fetch(ctx context.Context, task *v1alpha1.Task) (usage.RunnerReport, error) {
	// The worker IP is validated in both modes: it marks a placed actor.
	addr, err := checkWorkerIP(task.GetStatus().GetWorkerIp())
	if err != nil {
		return usage.RunnerReport{}, err
	}
	url := "http://" + netip.AddrPortFrom(addr, uint16(p.o.Port)).String() + UsagePath
	target := ""
	if p.o.RouterAddr != "" {
		atespace := task.GetMetadata().GetAtespace()
		if atespace == "" {
			atespace = "default"
		}
		// The actor is named after the task.
		target = atespace + "/" + task.GetMetadata().GetName()
		url = "http://" + p.o.RouterAddr + UsagePath
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return usage.RunnerReport{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if target != "" {
		req.Header.Set(TargetActorHeader, target)
		url += " (actor " + target + ")"
	}
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
