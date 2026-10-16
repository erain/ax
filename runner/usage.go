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
	"container/list"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// UsagePath is the probe-listener path serving the cumulative UsageReport.
	UsagePath = "/usage"
	// UsageRecordPath is the loopback metadata path the task command POSTs
	// UsageRecord bodies to.
	UsageRecordPath = "/metadata/v1alpha1/ax/usage"

	// usageFileRel is where the counters file lives relative to the workspace root.
	usageFileRel = ".ax/usage.json"
	// workspaceBytesTTL bounds how often the workspace tree is walked.
	workspaceBytesTTL = 60 * time.Second
	// dedupeCapacity bounds the span and request id dedupe sets.
	dedupeCapacity = 10000
	// maxRecordBytes caps a JSON usage POST body.
	maxRecordBytes = 1 << 20
	// unknownModel is used when a usage record does not name its model.
	unknownModel = "unknown"
)

// TokenUsage is the cumulative token count for one model, as served in
// UsageReport.Tokens and persisted in the counters file. CostAmount and
// CostCurrency are set only when at least one record for the model carried a
// provider-reported cost.
type TokenUsage struct {
	Input        int64   `json:"input"`
	Output       int64   `json:"output"`
	CacheRead    int64   `json:"cacheRead"`
	CacheWrite   int64   `json:"cacheWrite"`
	Reasoning    int64   `json:"reasoning"`
	CostAmount   float64 `json:"costAmount,omitempty"`
	CostCurrency string  `json:"costCurrency,omitempty"`
}

// UsageReport is the body of GET /usage on the probe listener. Token counts are
// cumulative since the counters file was created and survive runner restarts;
// Session changes every time the runner process starts.
type UsageReport struct {
	Session        string                 `json:"session"`
	ReportTime     time.Time              `json:"reportTime"`
	CommandExited  bool                   `json:"commandExited"`
	ExitCode       int                    `json:"exitCode"`
	WorkspaceBytes int64                  `json:"workspaceBytes"`
	Tokens         map[string]*TokenUsage `json:"tokens"`
}

// UsageCost is a provider-reported cost attached to a UsageRecord.
type UsageCost struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// UsageRecord is the body the task command POSTs to UsageRecordPath to report
// the tokens of one model call. RequestID is optional; a record whose
// RequestID was already seen by this runner process is ignored.
type UsageRecord struct {
	Model            string     `json:"model"`
	InputTokens      int64      `json:"inputTokens"`
	OutputTokens     int64      `json:"outputTokens"`
	CacheReadTokens  int64      `json:"cacheReadTokens"`
	CacheWriteTokens int64      `json:"cacheWriteTokens"`
	ReasoningTokens  int64      `json:"reasoningTokens"`
	Cost             *UsageCost `json:"cost,omitempty"`
	RequestID        string     `json:"requestId,omitempty"`
}

// tokenDelta is one increment of token usage for a model.
type tokenDelta struct {
	input, output, cacheRead, cacheWrite, reasoning int64
	costAmount                                      float64
	costCurrency                                    string
}

func (d tokenDelta) empty() bool {
	return d.input == 0 && d.output == 0 && d.cacheRead == 0 && d.cacheWrite == 0 && d.reasoning == 0 && d.costAmount == 0
}

// usageFile is the on-disk form of the counters.
type usageFile struct {
	CreateTime time.Time              `json:"createTime"`
	Tokens     map[string]*TokenUsage `json:"tokens"`
}

// usageTracker keeps the runner's cumulative token counters, persists them to
// the workspace, and remembers how the task command exited.
type usageTracker struct {
	path   string // counters file; empty keeps counters in memory only
	wsRoot string // tree walked for workspaceBytes; empty reports zero

	mu            sync.Mutex
	session       string
	file          usageFile
	spans         *lru // trace+span ids already counted
	requests      *lru // UsageRecord request ids already counted
	spanModels    map[string]bool
	metricStreams map[string]metricStream
	commandExited bool
	exitCode      int

	wsMu      sync.Mutex
	wsBytes   int64
	wsBytesAt time.Time

	now func() time.Time
}

// metricStream remembers the last cumulative value of one OTLP metric stream.
type metricStream struct {
	start uint64
	sum   float64
}

// newUsageTracker loads the counters at path, or starts empty counters when
// the file does not exist or cannot be read, and opens a new session.
func newUsageTracker(path, wsRoot string) *usageTracker {
	t := &usageTracker{
		path:          path,
		wsRoot:        wsRoot,
		session:       newSessionID(),
		spans:         newLRU(dedupeCapacity),
		requests:      newLRU(dedupeCapacity),
		spanModels:    map[string]bool{},
		metricStreams: map[string]metricStream{},
		now:           time.Now,
	}
	t.file = t.load()
	slog.Info("usage counters loaded", "path", path, "session", t.session, "models", len(t.file.Tokens))
	return t
}

func newSessionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// load reads the counters file. A missing file starts fresh counters; an
// unreadable one is moved aside so it is not overwritten silently.
func (t *usageTracker) load() usageFile {
	fresh := usageFile{CreateTime: time.Now().UTC(), Tokens: map[string]*TokenUsage{}}
	if t.path == "" {
		return fresh
	}
	data, err := os.ReadFile(t.path)
	if errors.Is(err, fs.ErrNotExist) {
		return fresh
	}
	var f usageFile
	if err == nil {
		err = json.Unmarshal(data, &f)
	}
	if err != nil {
		aside := t.path + ".corrupt"
		slog.Error("usage counters unreadable; starting fresh", "path", t.path, "movedTo", aside, "error", err)
		_ = os.Rename(t.path, aside)
		return fresh
	}
	if f.Tokens == nil {
		f.Tokens = map[string]*TokenUsage{}
	}
	if f.CreateTime.IsZero() {
		f.CreateTime = fresh.CreateTime
	}
	return f
}

// saveLocked writes the counters atomically: a temp file in the same
// directory, synced, then renamed over the old one. Errors are logged; the
// counters stay correct in memory either way.
func (t *usageTracker) saveLocked() {
	if t.path == "" {
		return
	}
	if err := writeFileAtomic(t.path, t.file); err != nil {
		slog.Error("persisting usage counters failed", "path", t.path, "error", err)
	}
}

func writeFileAtomic(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// addLocked adds d to model's counters. It does not persist.
func (t *usageTracker) addLocked(model string, d tokenDelta) {
	if model == "" {
		model = unknownModel
	}
	c := t.file.Tokens[model]
	if c == nil {
		c = &TokenUsage{}
		t.file.Tokens[model] = c
	}
	c.Input += d.input
	c.Output += d.output
	c.CacheRead += d.cacheRead
	c.CacheWrite += d.cacheWrite
	c.Reasoning += d.reasoning
	if d.costAmount != 0 {
		switch {
		case c.CostCurrency == "" || c.CostCurrency == d.costCurrency:
			c.CostAmount += d.costAmount
			c.CostCurrency = d.costCurrency
		default:
			slog.Warn("ignoring usage cost in a different currency", "model", model, "have", c.CostCurrency, "got", d.costCurrency)
		}
	}
}

// record adds one JSON usage record. It reports false when the record's
// request id was already counted.
func (t *usageTracker) record(r UsageRecord) bool {
	d := tokenDelta{
		input:      r.InputTokens,
		output:     r.OutputTokens,
		cacheRead:  r.CacheReadTokens,
		cacheWrite: r.CacheWriteTokens,
		reasoning:  r.ReasoningTokens,
	}
	if r.Cost != nil {
		d.costAmount, d.costCurrency = r.Cost.Amount, r.Cost.Currency
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if r.RequestID != "" && !t.requests.add(r.RequestID) {
		return false
	}
	t.addLocked(r.Model, d)
	t.saveLocked()
	return true
}

// spanUsage is the token usage carried by one span.
type spanUsage struct {
	id            string // trace id + span id
	model         string
	requestModel  string
	responseModel string
	delta         tokenDelta
}

// metricPoint is one data point of the gen_ai.client.token.usage histogram.
type metricPoint struct {
	stream        string // identity of the point's time series
	cumulative    bool
	start         uint64
	sum           float64
	tokenType     string
	model         string
	requestModel  string
	responseModel string
}

// applyOTLP adds the usage decoded from one OTLP export request. Spans are
// counted once per trace+span id. Metric points are counted only for models
// that no span has reported usage for in this session, so a harness that
// emits both signals is not counted twice.
func (t *usageTracker) applyOTLP(spans []spanUsage, points []metricPoint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	changed := false
	for _, s := range spans {
		if s.id != "" && !t.spans.add(s.id) {
			continue
		}
		t.spanModels[s.model] = true
		if s.requestModel != "" {
			t.spanModels[s.requestModel] = true
		}
		if s.responseModel != "" {
			t.spanModels[s.responseModel] = true
		}
		t.addLocked(s.model, s.delta)
		changed = true
	}
	for _, p := range points {
		inc := p.sum
		if p.cumulative {
			last, seen := t.metricStreams[p.stream]
			t.metricStreams[p.stream] = metricStream{start: p.start, sum: p.sum}
			if seen && last.start == p.start && p.sum >= last.sum {
				inc = p.sum - last.sum
			}
		}
		if t.spanModels[p.model] || t.spanModels[p.requestModel] || t.spanModels[p.responseModel] {
			continue
		}
		n := int64(inc + 0.5)
		if n <= 0 {
			continue
		}
		var d tokenDelta
		switch p.tokenType {
		case "input":
			d.input = n
		case "output":
			d.output = n
		default:
			continue
		}
		t.addLocked(p.model, d)
		changed = true
	}
	if changed {
		t.saveLocked()
	}
}

// commandDone records how the task command exited.
func (t *usageTracker) commandDone(exitCode int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commandExited = true
	t.exitCode = exitCode
}

// report returns a snapshot of the counters.
func (t *usageTracker) report() UsageReport {
	wsBytes := t.workspaceBytes()
	t.mu.Lock()
	defer t.mu.Unlock()
	tokens := make(map[string]*TokenUsage, len(t.file.Tokens))
	for m, c := range t.file.Tokens {
		cp := *c
		tokens[m] = &cp
	}
	return UsageReport{
		Session:        t.session,
		ReportTime:     t.now().UTC(),
		CommandExited:  t.commandExited,
		ExitCode:       t.exitCode,
		WorkspaceBytes: wsBytes,
		Tokens:         tokens,
	}
}

// workspaceBytes returns the total size of regular files under the workspace
// root, walking the tree at most once per workspaceBytesTTL.
func (t *usageTracker) workspaceBytes() int64 {
	if t.wsRoot == "" {
		return 0
	}
	t.wsMu.Lock()
	defer t.wsMu.Unlock()
	if !t.wsBytesAt.IsZero() && t.now().Sub(t.wsBytesAt) < workspaceBytesTTL {
		return t.wsBytes
	}
	var total int64
	_ = filepath.WalkDir(t.wsRoot, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries, keep walking
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	t.wsBytes, t.wsBytesAt = total, t.now()
	return total
}

// handleReport serves GET /usage.
func (t *usageTracker) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(t.report())
}

// handleRecord serves POST /metadata/v1alpha1/ax/usage.
func (t *usageTracker) handleRecord(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var rec UsageRecord
	dec := json.NewDecoder(io.LimitReader(r.Body, maxRecordBytes))
	if err := dec.Decode(&rec); err != nil {
		http.Error(w, fmt.Sprintf("invalid usage record: %v", err), http.StatusBadRequest)
		return
	}
	if rec.InputTokens < 0 || rec.OutputTokens < 0 || rec.CacheReadTokens < 0 || rec.CacheWriteTokens < 0 || rec.ReasoningTokens < 0 ||
		(rec.Cost != nil && rec.Cost.Amount < 0) {
		http.Error(w, "invalid usage record: token counts and cost must not be negative", http.StatusBadRequest)
		return
	}
	recorded := t.record(rec)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"recorded": recorded})
}

// lru is a bounded set that evicts the least recently added or seen key.
type lru struct {
	cap   int
	order *list.List
	items map[string]*list.Element
}

func newLRU(capacity int) *lru {
	return &lru{cap: capacity, order: list.New(), items: map[string]*list.Element{}}
}

// add inserts key and reports whether it was new. A key already present is
// refreshed and add returns false.
func (l *lru) add(key string) bool {
	if e, ok := l.items[key]; ok {
		l.order.MoveToFront(e)
		return false
	}
	l.items[key] = l.order.PushFront(key)
	if l.order.Len() > l.cap {
		oldest := l.order.Back()
		l.order.Remove(oldest)
		delete(l.items, oldest.Value.(string))
	}
	return true
}
