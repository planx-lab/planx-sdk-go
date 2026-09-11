package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

// TestLogger_NeverNil asserts the package logger is always non-nil, including
// before any explicit configuration (init() runs it).
func TestLogger_NeverNil(t *testing.T) {
	if Logger() == nil {
		t.Fatal("Logger() returned nil")
	}
}

// TestSetLogger_EmitsJSON captures structured output through a custom handler
// to verify the package-level logger is actually swapped and emits JSON.
func TestSetLogger_EmitsJSON(t *testing.T) {
	var buf bytes.Buffer
	custom := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	prev := Logger()
	SetLogger(custom)
	defer SetLogger(prev) // restore for other tests in the package

	Logger().Info("hello", "component", "test")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("output is not JSON: %v\nraw: %s", err, buf.String())
	}
	if entry["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", entry["msg"])
	}
	if entry["component"] != "test" {
		t.Errorf("component = %v, want test", entry["component"])
	}
}

// TestSetLogger_NilRestoresDefault confirms passing nil is safe and restores
// the JSON-to-stderr default rather than panicking.
func TestSetLogger_NilRestoresDefault(t *testing.T) {
	SetLogger(nil)
	l := Logger()
	if l == nil {
		t.Fatal("Logger() nil after SetLogger(nil)")
	}
}
