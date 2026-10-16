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

// Package ingest is the OTLP/HTTP logs receiver for Agent Substrate actor
// events. It decodes ate.actor.usage_sampled, ate.actor.state_changed and
// ate.actor.crashed log records (forwarded by the sidecar Collector), maps each
// actor to its AX task (the actor is named after the task, in the task's
// atespace), and folds the events into the task's usage record.
//
// Schema (Agent Substrate v0.5.0, internal/actorevent and internal/ateattr):
// the event name is the LogRecord's own event_name field (the body is the
// display string, e.g. "Actor usage sampled"); attributes are flat ate.* keys;
// ate.actor.epoch is an int (unix nanos); ate.stats.cpu.time is a double
// (seconds); ate.stats.memory.* are ints (bytes); ate.actor.state is a
// lowercase string (running, suspended, crashed, ...). The decoder is lenient
// about value types (int, double and numeric strings are all accepted) so a
// collector transform cannot silently break attribution.
package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/usage"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// MaxBodyBytes bounds the (decompressed) request body.
const MaxBodyBytes = 8 << 20

// CreationSkew is how much earlier than the task's creation timestamp an
// event may be and still be attributed to the task (clock skew between
// ax-server and Substrate).
const CreationSkew = 2 * time.Second

// Event names (LogRecord event_name) and their body-text fallbacks.
const (
	EventUsageSampled = "ate.actor.usage_sampled"
	EventStateChanged = "ate.actor.state_changed"
	EventCrashed      = "ate.actor.crashed"
)

var bodyEvents = map[string]string{
	"Actor usage sampled": EventUsageSampled,
	"Actor state changed": EventStateChanged,
	"Actor crashed":       EventCrashed,
}

// Attribute keys read from the records.
const (
	AttrAtespace        = "ate.atespace"
	AttrActorName       = "ate.actor.name"
	AttrActorUID        = "ate.actor.uid"
	AttrWorkerPoolName  = "ate.workerpool.name"
	AttrEpoch           = "ate.actor.epoch"
	AttrStatsKind       = "ate.stats.kind"
	AttrCPUTime         = "ate.stats.cpu.time"
	AttrMemWorkingSet   = "ate.stats.memory.working_set"
	AttrActorState      = "ate.actor.state"
	AttrActorOperation  = "ate.actor.operation.name"
	attrEventNameLegacy = "event.name"
)

// TaskLookup resolves an actor to its AX task. A missing task is reported as
// (nil, nil), an error wrapping store.ErrNotFound, or a gRPC NotFound status.
// internal/store.Store satisfies it.
type TaskLookup interface {
	GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)
}

// Options configures the handler.
type Options struct {
	// Store persists usage records (required).
	Store usage.Store
	// Tasks resolves actors to AX tasks (required). Events for actors without
	// a task are ignored.
	Tasks TaskLookup
	// Pricer prices each recorded delta (nil = record usage without cost).
	Pricer usage.Pricer
	// Hooks is called after each committed non-empty delta (may be nil).
	Hooks usage.Hooks
	// Accounting returns the accounting config (idle threshold etc.) at the
	// time of each request; nil = usage defaults.
	Accounting func() usage.AccountingConfig
	// Clock is the time source for records without any timestamp (nil =
	// time.Now).
	Clock func() time.Time
	// Logger receives debug counts and errors (nil = slog.Default()).
	Logger *slog.Logger
}

// Handler serves OTLP/HTTP log exports (POST, application/x-protobuf or
// application/json, optionally gzip-encoded). Mount it at /v1/logs.
type Handler struct {
	opts Options
}

// NewHandler returns a Handler for opts.
func NewHandler(opts Options) *Handler {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Handler{opts: opts}
}

// event is one decoded actor event.
type event struct {
	name     string
	time     time.Time
	atespace string
	actor    string
	sample   usage.UsageSample
	change   usage.StateChange
}

// Stats are the counts of one Ingest call.
type Stats struct {
	Records int // log records seen
	Events  int // ate.actor.* events decoded
	NoTask  int // events dropped because no AX task matches the actor
	UIDSkip int // events dropped because the task records a different actor uid
	Stale   int // events dropped because they predate the task's creation
	Resets  int // usage records reset because a newer actor replaced the recorded one
	Tasks   int // task usage records updated
	Deltas  int // non-empty deltas recorded (= Hooks calls)
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	jsonBody := false
	switch mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt {
	case "application/x-protobuf", "application/protobuf", "":
	case "application/json":
		jsonBody = true
	default:
		http.Error(w, "unsupported content type "+mt, http.StatusUnsupportedMediaType)
		return
	}
	if r.ContentLength > MaxBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	var body io.Reader = r.Body
	raw := &io.LimitedReader{R: r.Body, N: MaxBodyBytes + 1}
	switch enc := r.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(raw)
		if err != nil {
			http.Error(w, "invalid gzip body", http.StatusBadRequest)
			return
		}
		defer gz.Close()
		body = gz
	default:
		http.Error(w, "unsupported content encoding "+enc, http.StatusUnsupportedMediaType)
		return
	}
	data, err := io.ReadAll(io.LimitReader(body, MaxBodyBytes+1))
	if err != nil && raw.N <= 0 {
		// The compressed body itself exceeded the limit.
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(data) > MaxBodyBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	req := &collogspb.ExportLogsServiceRequest{}
	if jsonBody {
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, req)
	} else {
		err = proto.Unmarshal(data, req)
	}
	if err != nil {
		http.Error(w, "invalid OTLP body: "+err.Error(), http.StatusBadRequest)
		return
	}

	st, err := h.Ingest(r.Context(), req)
	if err != nil {
		// Retryable for OTLP exporters; accounting is replay-safe.
		h.opts.Logger.ErrorContext(r.Context(), "usage ingest failed", "error", err)
		http.Error(w, "ingest failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	h.opts.Logger.DebugContext(r.Context(), "usage ingest",
		"records", st.Records, "events", st.Events, "noTask", st.NoTask,
		"uidSkip", st.UIDSkip, "stale", st.Stale, "resets", st.Resets, "tasks", st.Tasks, "deltas", st.Deltas)

	var out []byte
	if jsonBody {
		w.Header().Set("Content-Type", "application/json")
		out, err = protojson.Marshal(&collogspb.ExportLogsServiceResponse{})
	} else {
		w.Header().Set("Content-Type", "application/x-protobuf")
		out, err = proto.Marshal(&collogspb.ExportLogsServiceResponse{})
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(out)
}

// Ingest applies every actor event in req. Events are grouped per task and
// applied in timestamp order inside one Store.Update per task. It returns an
// error only for task-lookup or store failures (the caller should retry; the
// accounting ignores replays).
func (h *Handler) Ingest(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (Stats, error) {
	var st Stats
	type actorRef struct{ atespace, name string }
	byActor := map[actorRef][]event{}
	var order []actorRef
	for _, rl := range req.GetResourceLogs() {
		resAttrs := rl.GetResource().GetAttributes()
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				st.Records++
				ev, ok := h.decode(rec, resAttrs)
				if !ok {
					continue
				}
				st.Events++
				ref := actorRef{ev.atespace, ev.actor}
				if _, seen := byActor[ref]; !seen {
					order = append(order, ref)
				}
				byActor[ref] = append(byActor[ref], ev)
			}
		}
	}

	cfg := usage.AccountingConfig{}
	if h.opts.Accounting != nil {
		cfg = h.opts.Accounting()
	}
	var errs []error
	for _, ref := range order {
		events := byActor[ref]
		task, err := h.opts.Tasks.GetTask(ctx, ref.atespace, ref.name)
		if isNotFound(task, err) {
			st.NoTask += len(events)
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("looking up task %s/%s: %w", ref.atespace, ref.name, err))
			continue
		}
		// A task re-created with the same name maps to the same actor name:
		// events of the previous actor (its late deleting/deleted events, a
		// final sample) predate the new task and must not be attributed to
		// it (they would pin the record to the old actor uid).
		if ct := task.GetMetadata().GetCreationTimestamp(); ct != nil && ct.IsValid() {
			notBefore := ct.AsTime().Add(-CreationSkew)
			events = slices.DeleteFunc(events, func(ev event) bool {
				if ev.time.Before(notBefore) {
					st.Stale++
					return true
				}
				return false
			})
		}
		taskUID := task.GetStatus().GetActorUid()
		if taskUID != "" {
			events = slices.DeleteFunc(events, func(ev event) bool {
				if evUID := ev.uid(); evUID != "" && evUID != taskUID {
					st.UIDSkip++
					return true
				}
				return false
			})
		}
		if len(events) == 0 {
			continue
		}
		// Apply in timestamp order (cheaper and exact for the per-epoch
		// sample integration; the accounting is order-robust regardless).
		slices.SortStableFunc(events, func(a, b event) int { return a.time.Compare(b.time) })

		atespace := task.GetMetadata().GetAtespace()
		if atespace == "" {
			atespace = ref.atespace
		}
		key := usage.Key{Atespace: atespace, Name: task.GetMetadata().GetName()}
		recorded, resets, err := h.apply(ctx, key, taskUID, events, cfg)
		st.Resets += resets
		if err != nil {
			errs = append(errs, fmt.Errorf("updating usage of %s/%s: %w", key.Atespace, key.Name, err))
			continue
		}
		if recorded {
			st.Deltas++
		}
		st.Tasks++
	}
	return st, errors.Join(errs...)
}

// apply folds events into the usage record of key, prices the combined delta,
// and calls Hooks when it is non-empty. It reports whether a non-empty delta
// was recorded and how many times the record was reset for a new actor.
//
// Actor replacement (task deleted and re-created with the same name): the
// record is reset to a fresh TaskUsage when
//   - taskUID (task.status.actorUid) is set and the record holds another uid;
//   - taskUID is unknown and an event carries a uid other than the recorded
//     one, is newer than every recorded event of the recorded actor, and
//     either is the new actor's creation (state event with operation
//     "create") or the recorded actor is gone (last state deleting/deleted).
//     Substrate deletes the old actor before it creates the new one with the
//     same name, so such an event belongs to the newer actor; a late event of
//     an old actor is older and stays rejected by the uid guard, and a stray
//     foreign event cannot flip the record of a live actor.
func (h *Handler) apply(ctx context.Context, key usage.Key, taskUID string, events []event, cfg usage.AccountingConfig) (bool, int, error) {
	var (
		delta  usage.Delta
		cost   usage.Cost
		snap   *usage.TaskUsage
		resets int
	)
	err := h.opts.Store.Update(ctx, key, func(u *usage.TaskUsage) error {
		// fn may be retried: (re)assign every captured result.
		delta, cost, snap, resets = usage.Delta{}, usage.Cost{}, nil, 0
		before, err := json.Marshal(u)
		if err != nil {
			return err
		}
		reset := func() {
			*u = *usage.NewTaskUsage()
			delta = usage.Delta{}
			resets++
		}
		if rec := u.State.ActorUID; taskUID != "" && rec != "" && rec != taskUID {
			reset()
		}
		for _, ev := range events {
			if taskUID == "" && replacesActor(u, ev) {
				reset()
			}
			switch ev.name {
			case EventUsageSampled:
				delta.Add(u.ApplyUsageSample(ev.sample, cfg))
			default:
				delta.Add(u.ApplyStateChange(ev.change))
			}
		}
		if !delta.IsZero() {
			cost = u.ChargeDelta(delta, h.opts.Pricer, u.State.Pool)
		}
		after, err := json.Marshal(u)
		if err != nil {
			return err
		}
		if bytes.Equal(before, after) {
			// Replays, stale events, uid mismatches: nothing to write.
			return usage.ErrNoChange
		}
		snap = u
		return nil
	})
	if err != nil {
		return false, 0, err
	}
	if delta.IsZero() {
		return false, resets, nil
	}
	if h.opts.Hooks != nil && snap != nil {
		h.opts.Hooks.OnDelta(ctx, key, delta, cost, snap)
	}
	return true, resets, nil
}

// replacesActor reports whether ev comes from an actor that replaced the one
// recorded in u (see apply).
func replacesActor(u *usage.TaskUsage, ev event) bool {
	uid, rec := ev.uid(), u.State.ActorUID
	if uid == "" || rec == "" || uid == rec || !ev.time.After(u.ActorLastSeen()) {
		return false
	}
	if ev.name != EventUsageSampled && ev.change.Operation == "create" {
		return true
	}
	switch u.State.State {
	case "deleting", "deleted":
		return true
	}
	return false
}

func (ev event) uid() string {
	if ev.name == EventUsageSampled {
		return ev.sample.ActorUID
	}
	return ev.change.ActorUID
}

func isNotFound(task *v1alpha1.Task, err error) bool {
	if err == nil {
		return task == nil
	}
	return errors.Is(err, store.ErrNotFound) || status.Code(err) == codes.NotFound
}

// decode turns one log record into an actor event; ok is false for records
// that are not (complete) ate.actor.* events.
func (h *Handler) decode(rec *logspb.LogRecord, resAttrs []*commonpb.KeyValue) (event, bool) {
	attrs := attrMap(resAttrs)
	for _, kv := range rec.GetAttributes() {
		attrs[kv.GetKey()] = kv.GetValue()
	}
	name := rec.GetEventName()
	if name == "" {
		name, _ = stringAttr(attrs, attrEventNameLegacy)
	}
	if name == "" {
		name = bodyEvents[rec.GetBody().GetStringValue()]
	}
	switch name {
	case EventUsageSampled, EventStateChanged, EventCrashed:
	default:
		return event{}, false
	}
	ev := event{name: name}
	ev.atespace, _ = stringAttr(attrs, AttrAtespace)
	ev.actor, _ = stringAttr(attrs, AttrActorName)
	if ev.actor == "" {
		return event{}, false
	}
	if ev.atespace == "" {
		ev.atespace = "default"
	}
	switch {
	case rec.GetTimeUnixNano() != 0:
		ev.time = time.Unix(0, int64(rec.GetTimeUnixNano())).UTC()
	case rec.GetObservedTimeUnixNano() != 0:
		ev.time = time.Unix(0, int64(rec.GetObservedTimeUnixNano())).UTC()
	default:
		ev.time = h.opts.Clock().UTC()
	}
	uid, _ := stringAttr(attrs, AttrActorUID)

	if name == EventUsageSampled {
		s := usage.UsageSample{Time: ev.time, ActorUID: uid}
		s.Pool, _ = stringAttr(attrs, AttrWorkerPoolName)
		s.Kind, _ = stringAttr(attrs, AttrStatsKind)
		s.Epoch, _ = intAttr(attrs, AttrEpoch)
		if v, ok := floatAttr(attrs, AttrCPUTime); ok {
			s.CPUSeconds = &v
		}
		if v, ok := intAttr(attrs, AttrMemWorkingSet); ok {
			s.WorkingSetBytes = &v
		}
		ev.sample = s
		return ev, true
	}
	c := usage.StateChange{Time: ev.time, ActorUID: uid}
	c.State, _ = stringAttr(attrs, AttrActorState)
	c.Operation, _ = stringAttr(attrs, AttrActorOperation)
	if c.State == "" && name == EventCrashed {
		c.State = usage.StateCrashed
	}
	if c.State == "" {
		return event{}, false
	}
	ev.change = c
	return ev, true
}

func attrMap(kvs []*commonpb.KeyValue) map[string]*commonpb.AnyValue {
	m := make(map[string]*commonpb.AnyValue, len(kvs)+16)
	for _, kv := range kvs {
		m[kv.GetKey()] = kv.GetValue()
	}
	return m
}

func stringAttr(m map[string]*commonpb.AnyValue, key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue, true
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10), true
	}
	return "", false
}

func floatAttr(m map[string]*commonpb.AnyValue, key string) (float64, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, false
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue, !math.IsNaN(x.DoubleValue) && !math.IsInf(x.DoubleValue, 0)
	case *commonpb.AnyValue_IntValue:
		return float64(x.IntValue), true
	case *commonpb.AnyValue_StringValue:
		f, err := strconv.ParseFloat(x.StringValue, 64)
		return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
	}
	return 0, false
}

func intAttr(m map[string]*commonpb.AnyValue, key string) (int64, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, false
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_IntValue:
		return x.IntValue, true
	case *commonpb.AnyValue_DoubleValue:
		if math.IsNaN(x.DoubleValue) || math.IsInf(x.DoubleValue, 0) {
			return 0, false
		}
		return int64(x.DoubleValue), true
	case *commonpb.AnyValue_StringValue:
		if i, err := strconv.ParseInt(x.StringValue, 10, 64); err == nil {
			return i, true
		}
		if f, err := strconv.ParseFloat(x.StringValue, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return int64(f), true
		}
	}
	return 0, false
}
