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
	"fmt"
	"io"
	"log/slog"
)

// Log formats accepted by --log-format.
const (
	logFormatText = "text"
	logFormatJSON = "json"
)

// newLogHandler returns the slog handler for --log-format.
//
// "json" writes one JSON object per line. Cloud Logging (GKE) parses such
// lines into jsonPayload, so structured lines like ax.task.usage_summary can
// be filtered by field (jsonPayload.msg="ax.task.usage_summary",
// jsonPayload.labels.agent="..."). The top-level level key is renamed to
// "severity" (WARN -> WARNING) so Cloud Logging picks up the severity; msg and
// all other fields are unchanged.
func newLogHandler(format string, w io.Writer, level slog.Leveler) (slog.Handler, error) {
	switch format {
	case logFormatText, "":
		return slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}), nil
	case logFormatJSON:
		return slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: cloudLoggingSeverity}), nil
	default:
		return nil, fmt.Errorf("invalid --log-format %q (want %s or %s)", format, logFormatText, logFormatJSON)
	}
}

// cloudLoggingSeverity renames the record's built-in level to "severity".
// Only the built-in level attr (a slog.Level value) is renamed: a top-level
// user attr that happens to be called "level" (e.g. the budget crossing's
// level=warning|exceeded) is kept as is, so a line never gets two "severity"
// keys.
func cloudLoggingSeverity(groups []string, a slog.Attr) slog.Attr {
	if len(groups) != 0 || a.Key != slog.LevelKey {
		return a
	}
	lvl, ok := a.Value.Any().(slog.Level)
	if !ok {
		return a
	}
	var s string
	switch {
	case lvl < slog.LevelInfo:
		s = "DEBUG"
	case lvl < slog.LevelWarn:
		s = "INFO"
	case lvl < slog.LevelError:
		s = "WARNING"
	default:
		s = "ERROR"
	}
	return slog.String("severity", s)
}
