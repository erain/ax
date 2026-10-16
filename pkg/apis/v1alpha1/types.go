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

package v1alpha1

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v3"
)

const (
	APIVersion    = "ax.io/v1alpha1"
	KindTask      = "Task"
	KindWorkspace = "Workspace"
	KindModel     = "Model"

	DefaultTaskImage       = "gcr.io/ax-substrate/ate-images/ax-task-runner"
	DefaultSystemNamespace = "ax-system"

	// PhaseTerminating marks a task whose deletion has been requested and whose
	// actor is being torn down. The record disappears once cleanup completes.
	PhaseTerminating = "Terminating"

	// PhaseCompleted marks a task whose command exited and which was suspended
	// because of spec.onCompletion SUSPEND_ON_EXIT.
	PhaseCompleted = "Completed"
)

// Task condition types.
const (
	// ConditionBudgetWarning is True while some budget limit is at or above its
	// warning threshold but below the limit.
	ConditionBudgetWarning = "BudgetWarning"
	// ConditionBudgetExceeded is True once some budget limit has been reached.
	ConditionBudgetExceeded = "BudgetExceeded"
	// ConditionIdle is True while a running task is idle.
	ConditionIdle = "Idle"
)

// YAML encoding.
//
// The API kinds are protobuf messages, so protojson is the single source of
// truth for their wire names (lowerCamelCase), timestamp format (RFC 3339), and
// which fields are omitted when unset. YAML is bridged through it: manifests are
// decoded generically, converted to JSON, and then decoded strictly by protojson,
// so unknown or misspelled fields are rejected. Output is protojson rendered as
// a yaml.Node so field order follows the proto definition instead of being
// sorted.
//
// A few legacy manifest shapes are still accepted. They are rewritten on the
// generic document before strict decoding by the per-kind normalizers below.

var (
	protoMarshal   = protojson.MarshalOptions{}
	protoUnmarshal = protojson.UnmarshalOptions{} // unknown fields are errors
)

// normalizer rewrites a generically decoded document in place so legacy shapes
// match the current schema before strict decoding.
type normalizer func(doc map[string]any)

func marshalYAML(m proto.Message) (any, error) {
	data, err := protoMarshal.Marshal(m)
	if err != nil {
		return nil, err
	}
	n, err := jsonToYAMLNode(data)
	if err != nil {
		return nil, err
	}
	tagInt64s(n, m.ProtoReflect().Descriptor())
	return n, nil
}

// tagInt64s retags the 64-bit integer fields of a protojson-derived node as
// YAML integers. protojson renders them as strings, which would otherwise be
// written quoted (maxTokens: "1000").
func tagInt64s(n *yaml.Node, md protoreflect.MessageDescriptor) {
	if n.Kind != yaml.MappingNode || strings.HasPrefix(string(md.FullName()), "google.protobuf.") {
		return
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		fd := md.Fields().ByJSONName(n.Content[i].Value)
		if fd == nil {
			continue
		}
		val := n.Content[i+1]
		switch {
		case fd.IsMap():
			if val.Kind != yaml.MappingNode {
				continue
			}
			for j := 1; j < len(val.Content); j += 2 {
				tagInt64Value(val.Content[j], fd.MapValue())
			}
		case fd.IsList():
			for _, item := range val.Content {
				tagInt64Value(item, fd)
			}
		default:
			tagInt64Value(val, fd)
		}
	}
}

func tagInt64Value(n *yaml.Node, fd protoreflect.FieldDescriptor) {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		tagInt64s(n, fd.Message())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
			if _, err := strconv.ParseInt(n.Value, 10, 64); err == nil {
				n.Tag = "!!int"
			} else if _, err := strconv.ParseUint(n.Value, 10, 64); err == nil {
				n.Tag = "!!int"
			}
		}
	}
}

func unmarshalYAML(value *yaml.Node, m proto.Message, normalize normalizer) error {
	var doc any
	if err := value.Decode(&doc); err != nil {
		return err
	}
	if obj, ok := doc.(map[string]any); ok && normalize != nil {
		normalize(obj)
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("converting yaml to json: %w", err)
	}
	return protoUnmarshal.Unmarshal(data, m)
}

// jsonToYAMLNode converts a JSON document into a yaml.Node tree, preserving
// object key order and tagging scalars so the YAML encoder quotes strings that
// would otherwise read as another type (for example a status of "True").
func jsonToYAMLNode(data []byte) (*yaml.Node, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return readJSONValue(dec)
}

func readJSONValue(dec *json.Decoder) (*yaml.Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			n := &yaml.Node{Kind: yaml.MappingNode}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyTok.(string)
				val, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, val)
			}
			_, err = dec.Token() // closing '}'
			return n, err
		case '[':
			n := &yaml.Node{Kind: yaml.SequenceNode}
			for dec.More() {
				val, err := readJSONValue(dec)
				if err != nil {
					return nil, err
				}
				n.Content = append(n.Content, val)
			}
			_, err = dec.Token() // closing ']'
			return n, err
		}
		return nil, fmt.Errorf("unexpected json delimiter %q", v)
	case string:
		// protojson renders timestamps as RFC 3339 strings. Let those stay plain
		// scalars, which YAML reads as timestamps; every other string is tagged so
		// values like "True" or "1" are quoted and read back as strings.
		tag := "!!str"
		if _, err := time.Parse(time.RFC3339Nano, v); err == nil {
			tag = "!!timestamp"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v}, nil
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(v.String(), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v.String()}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(v)}, nil
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	}
	return nil, fmt.Errorf("unexpected json token %v", tok)
}

// Legacy manifest shapes.

// normalizeTask accepts spec.budget.activeDeadline written as a Go duration
// string such as "30m" or "1h30m"; protojson only accepts seconds ("1800s").
func normalizeTask(doc map[string]any) {
	spec, _ := doc["spec"].(map[string]any)
	budget, _ := spec["budget"].(map[string]any)
	if s, ok := budget["activeDeadline"].(string); ok {
		if d, err := time.ParseDuration(s); err == nil {
			budget["activeDeadline"] = protoDurationString(d)
		}
	}
}

// protoDurationString formats d the way protojson writes a
// google.protobuf.Duration: seconds with an "s" suffix.
func protoDurationString(d time.Duration) string {
	sign := ""
	if d < 0 {
		sign = "-"
		d = -d
	}
	secs, nanos := int64(d/time.Second), int64(d%time.Second)
	if nanos == 0 {
		return fmt.Sprintf("%s%ds", sign, secs)
	}
	return fmt.Sprintf("%s%d.%09ds", sign, secs, nanos)
}

// normalizeWorkspace accepts spec.mcp written as a bare list of servers.
func normalizeWorkspace(doc map[string]any) {
	spec, _ := doc["spec"].(map[string]any)
	if list, ok := spec["mcp"].([]any); ok {
		spec["mcp"] = map[string]any{"servers": list}
	}
}

// normalizeModel accepts older spellings of the secret reference: spec.secretKeyRef,
// spec.apiKey.secretKeyRef, and a bare string meaning the secret name and key are
// the same.
func normalizeModel(doc map[string]any) {
	spec, ok := doc["spec"].(map[string]any)
	if !ok {
		return
	}
	if _, has := spec["secretKey"]; !has {
		if ref, ok := spec["secretKeyRef"]; ok {
			spec["secretKey"] = ref
		} else if apiKey, ok := spec["apiKey"].(map[string]any); ok {
			if ref, ok := apiKey["secretKeyRef"]; ok {
				spec["secretKey"] = ref
			}
		}
	}
	delete(spec, "secretKeyRef")
	delete(spec, "apiKey")
	if s, ok := spec["secretKey"].(string); ok {
		spec["secretKey"] = map[string]any{"name": s, "key": s}
	}
}

// Per-kind YAML methods. Nested messages are handled by protojson as part of
// their parent, so only the kinds that are encoded on their own need these.

func (t *Task) MarshalYAML() (any, error)        { return marshalYAML(t) }
func (t *Task) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, t, normalizeTask) }

func (w *Workspace) MarshalYAML() (any, error)        { return marshalYAML(w) }
func (w *Workspace) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, w, normalizeWorkspace) }

func (m *Model) MarshalYAML() (any, error)        { return marshalYAML(m) }
func (m *Model) UnmarshalYAML(n *yaml.Node) error { return unmarshalYAML(n, m, normalizeModel) }

// Workspace bindings.
//
// A task binds one or more workspaces through spec.workspaces. Every entry is
// treated the same way regardless of how many there are.

// DefaultWorkspacePath is the durable volume every workspace lives under. A
// binding without a path is mounted at DefaultWorkspacePath/<name>, and a task
// with no bindings gets an empty working directory at DefaultWorkspacePath.
const DefaultWorkspacePath = "/workspace"

// WorkspaceRefs returns the workspaces the task binds, in declaration order,
// with nil entries dropped.
func (s *TaskSpec) WorkspaceRefs() []*WorkspaceRef {
	if s == nil {
		return nil
	}
	out := make([]*WorkspaceRef, 0, len(s.GetWorkspaces()))
	for _, r := range s.GetWorkspaces() {
		if r != nil {
			out = append(out, r)
		}
	}
	return out
}

// WorkspacePaths returns the mount path of each workspace in WorkspaceRefs, in
// the same order. An explicit path wins; otherwise the workspace lands at
// DefaultWorkspacePath/<name>.
func (s *TaskSpec) WorkspacePaths() []string {
	refs := s.WorkspaceRefs()
	paths := make([]string, len(refs))
	for i, r := range refs {
		if r.GetPath() != "" {
			paths[i] = r.GetPath()
		} else {
			paths[i] = DefaultWorkspacePath + "/" + r.GetName()
		}
	}
	return paths
}

// Resource names.
//
// A resource's name and atespace become Substrate resource names (the task's
// actor and ActorTemplate, and the atespace itself), which must be lowercase
// RFC 1123 labels. Checking them at apply time turns an asynchronous
// ActorCreationFailed condition into an immediate error.

// MaxNameLength is the longest name or atespace a resource may have.
const MaxNameLength = 63

var nameRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// ValidateName reports whether s can be used as a resource name or atespace: a
// lowercase RFC 1123 label of at most MaxNameLength characters.
func ValidateName(s string) error {
	if s == "" {
		return errors.New("must not be empty")
	}
	if len(s) > MaxNameLength || !nameRegexp.MatchString(s) {
		return fmt.Errorf("must be a lowercase RFC 1123 label: at most %d lowercase alphanumeric characters or '-', starting and ending with an alphanumeric character", MaxNameLength)
	}
	return nil
}

// ValidateObjectMeta checks a resource's name, its labels and, when set, its
// atespace. An empty atespace is fine here; the API server defaults it.
func ValidateObjectMeta(meta *ObjectMeta) error {
	if err := ValidateName(meta.GetName()); err != nil {
		return fmt.Errorf("metadata.name: invalid value %q: %w", meta.GetName(), err)
	}
	if atespace := meta.GetAtespace(); atespace != "" {
		if err := ValidateName(atespace); err != nil {
			return fmt.Errorf("metadata.atespace: invalid value %q: %w", atespace, err)
		}
	}
	if err := ValidateLabels(meta.GetLabels()); err != nil {
		return fmt.Errorf("metadata.labels: %w", err)
	}
	return nil
}

// Labels.
//
// Labels follow Kubernetes label syntax so selectors and tooling behave the
// same way they do there.

const (
	// MaxLabelNameLength is the longest label value, and the longest label key
	// name (the part after an optional prefix).
	MaxLabelNameLength = 63
	// MaxLabelPrefixLength is the longest label key prefix.
	MaxLabelPrefixLength = 253
)

var (
	labelNameRegexp   = regexp.MustCompile(`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`)
	labelPrefixRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
)

// ValidateLabelKey reports whether k is a valid label key: a name of at most
// MaxLabelNameLength alphanumeric characters, '-', '_' or '.', starting and
// ending with an alphanumeric character, optionally preceded by a DNS
// subdomain prefix of at most MaxLabelPrefixLength characters and a '/'.
func ValidateLabelKey(k string) error {
	name := k
	if prefix, rest, ok := strings.Cut(k, "/"); ok {
		if prefix == "" || len(prefix) > MaxLabelPrefixLength || !labelPrefixRegexp.MatchString(prefix) {
			return fmt.Errorf("invalid key %q: prefix must be a lowercase DNS subdomain of at most %d characters", k, MaxLabelPrefixLength)
		}
		name = rest
	}
	if name == "" || len(name) > MaxLabelNameLength || !labelNameRegexp.MatchString(name) {
		return fmt.Errorf("invalid key %q: name must be at most %d alphanumeric characters, '-', '_' or '.', starting and ending with an alphanumeric character", k, MaxLabelNameLength)
	}
	return nil
}

// ValidateLabelValue reports whether v is a valid label value: empty, or at
// most MaxLabelNameLength alphanumeric characters, '-', '_' or '.', starting
// and ending with an alphanumeric character.
func ValidateLabelValue(v string) error {
	if v == "" {
		return nil
	}
	if len(v) > MaxLabelNameLength || !labelNameRegexp.MatchString(v) {
		return fmt.Errorf("invalid value %q: must be at most %d alphanumeric characters, '-', '_' or '.', starting and ending with an alphanumeric character", v, MaxLabelNameLength)
	}
	return nil
}

// ValidateLabels checks every key and value of labels, reporting the first
// problem in key order.
func ValidateLabels(labels map[string]string) error {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		if err := ValidateLabelKey(k); err != nil {
			return err
		}
		if err := ValidateLabelValue(labels[k]); err != nil {
			return fmt.Errorf("label %q: %w", k, err)
		}
	}
	return nil
}

// LabelRequirement is one term of a LabelSelector: the label Key must be
// present with exactly Value.
type LabelRequirement struct {
	Key   string
	Value string
}

// LabelSelector selects objects by label equality. It is the conjunction of
// its requirements, so an object matches only if it satisfies every term. The
// zero value has no requirements and matches everything.
type LabelSelector struct {
	reqs []LabelRequirement // sorted by key, keys unique
}

// ParseLabelSelector parses a comma-separated list of equality terms such as
// "agent=foo,customer=bar" (Kubernetes equality-based syntax; "k==v" is also
// accepted). Whitespace around terms, keys and values is ignored. An empty or
// all-whitespace string yields the empty selector, which matches everything.
// Keys and values must be valid label keys and values; a key repeated with a
// different value is an error.
func ParseLabelSelector(s string) (LabelSelector, error) {
	if strings.TrimSpace(s) == "" {
		return LabelSelector{}, nil
	}
	byKey := make(map[string]string)
	for _, term := range strings.Split(s, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			return LabelSelector{}, fmt.Errorf("invalid label selector %q: empty term", s)
		}
		if strings.Contains(term, "!=") {
			return LabelSelector{}, fmt.Errorf("invalid label selector %q: term %q: only equality (key=value) terms are supported", s, term)
		}
		k, v, ok := strings.Cut(term, "==")
		if !ok {
			k, v, ok = strings.Cut(term, "=")
		}
		if !ok {
			return LabelSelector{}, fmt.Errorf("invalid label selector %q: term %q: expected key=value", s, term)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if err := ValidateLabelKey(k); err != nil {
			return LabelSelector{}, fmt.Errorf("invalid label selector %q: %w", s, err)
		}
		if err := ValidateLabelValue(v); err != nil {
			return LabelSelector{}, fmt.Errorf("invalid label selector %q: key %q: %w", s, k, err)
		}
		if prev, dup := byKey[k]; dup && prev != v {
			return LabelSelector{}, fmt.Errorf("invalid label selector %q: key %q has conflicting values %q and %q", s, k, prev, v)
		}
		byKey[k] = v
	}
	sel := LabelSelector{reqs: make([]LabelRequirement, 0, len(byKey))}
	for k, v := range byKey {
		sel.reqs = append(sel.reqs, LabelRequirement{Key: k, Value: v})
	}
	slices.SortFunc(sel.reqs, func(a, b LabelRequirement) int { return strings.Compare(a.Key, b.Key) })
	return sel, nil
}

// Empty reports whether the selector has no requirements (matches everything).
func (s LabelSelector) Empty() bool { return len(s.reqs) == 0 }

// Requirements returns a copy of the selector's terms, sorted by key.
func (s LabelSelector) Requirements() []LabelRequirement { return slices.Clone(s.reqs) }

// Matches reports whether labels satisfy every requirement. A nil map is
// treated as having no labels.
func (s LabelSelector) Matches(labels map[string]string) bool {
	for _, r := range s.reqs {
		if v, ok := labels[r.Key]; !ok || v != r.Value {
			return false
		}
	}
	return true
}

// String renders the selector in canonical form ("k1=v1,k2=v2", keys sorted).
// Parsing the result yields an equal selector.
func (s LabelSelector) String() string {
	terms := make([]string, len(s.reqs))
	for i, r := range s.reqs {
		terms[i] = r.Key + "=" + r.Value
	}
	return strings.Join(terms, ",")
}

// Budgets.

// BudgetCurrency is the only currency budgets and rate cards support.
const BudgetCurrency = "USD"

// ValidateBudget reports the first problem with a task budget. A nil budget is
// valid.
func ValidateBudget(b *BudgetSpec) error {
	if b == nil {
		return nil
	}
	if d := b.GetActiveDeadline(); d != nil {
		if err := d.CheckValid(); err != nil {
			return fmt.Errorf("activeDeadline: %w", err)
		}
		if d.AsDuration() < 0 {
			return fmt.Errorf("activeDeadline: must not be negative, got %s", d.AsDuration())
		}
	}
	if b.GetMaxTokens() < 0 {
		return fmt.Errorf("maxTokens: must not be negative, got %d", b.GetMaxTokens())
	}
	if c := b.GetMaxCost(); c != nil {
		if a := c.GetAmount(); a < 0 || math.IsNaN(a) || math.IsInf(a, 0) {
			return fmt.Errorf("maxCost.amount: must be a non-negative number, got %v", a)
		}
		if cur := c.GetCurrency(); cur != "" && cur != BudgetCurrency {
			return fmt.Errorf("maxCost.currency: only %s is supported, got %q", BudgetCurrency, cur)
		}
	}
	if p := b.GetWarnAtPercent(); p < 0 || p > 100 {
		return fmt.Errorf("warnAtPercent: must be between 0 and 100, got %d", p)
	}
	switch b.GetOnExceeded() {
	case ExceededAction_EXCEEDED_ACTION_UNSPECIFIED, ExceededAction_WARN:
	case ExceededAction_SUSPEND:
		return errors.New("onExceeded: SUSPEND is not supported yet")
	default:
		return fmt.Errorf("onExceeded: unknown action %d", b.GetOnExceeded())
	}
	return nil
}

// ValidateWorkspace reports the first problem with a workspace that would make
// it unusable. It is called by the API server before saving.
func ValidateWorkspace(w *Workspace) error {
	if err := ValidateObjectMeta(w.GetMetadata()); err != nil {
		return err
	}
	if w.GetSpec() != nil {
		for i, f := range w.GetSpec().GetFiles() {
			field := fmt.Sprintf("spec.files[%d]", i)
			if f == nil || f.GetPath() == "" {
				return fmt.Errorf("%s: path is required", field)
			}
		}
	}
	return nil
}

// ValidateModel reports the first problem with a model that would make it
// unusable. It is called by the API server before saving.
func ValidateModel(m *Model) error {
	return ValidateObjectMeta(m.GetMetadata())
}

// ValidateTask reports the first problem with a task that would make it
// impossible to run correctly. It is called by the API server before saving.
func ValidateTask(t *Task) error {
	if err := ValidateObjectMeta(t.GetMetadata()); err != nil {
		return err
	}
	spec := t.GetSpec()
	if spec == nil {
		return nil
	}
	if err := ValidateBudget(spec.GetBudget()); err != nil {
		return fmt.Errorf("spec.budget.%w", err)
	}
	switch spec.GetOnCompletion() {
	case CompletionPolicy_COMPLETION_POLICY_UNSPECIFIED, CompletionPolicy_KEEP, CompletionPolicy_SUSPEND_ON_EXIT:
	default:
		return fmt.Errorf("spec.onCompletion: unknown policy %d", spec.GetOnCompletion())
	}
	refs := spec.WorkspaceRefs()
	paths := spec.WorkspacePaths()
	names := make(map[string]bool, len(refs))
	seen := make(map[string]string, len(refs))
	for i, r := range refs {
		field := fmt.Sprintf("spec.workspaces[%d]", i)
		if r.GetName() == "" {
			return fmt.Errorf("%s: name is required", field)
		}
		if names[r.GetName()] {
			return fmt.Errorf("%s: workspace %q is bound more than once", field, r.GetName())
		}
		names[r.GetName()] = true
		if r.GetPath() != "" && !strings.HasPrefix(r.GetPath(), "/") {
			return fmt.Errorf("%s: path %q must be absolute", field, r.GetPath())
		}
		p := strings.TrimRight(paths[i], "/")
		if p == "" {
			p = "/"
		}
		if other, dup := seen[p]; dup {
			return fmt.Errorf("%s: path %q is already used by workspace %q", field, paths[i], other)
		}
		seen[p] = r.GetName()
	}
	return nil
}
