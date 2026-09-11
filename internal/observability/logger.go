// Package observability provides the runtime-owned metrics and logging
// machinery for the SDK. It is internal on purpose: plugins must remain
// unaware of metrics/logging, and all instrumentation is wired by the
// runtime. Tracing is out of scope.
package observability

import (
	"log/slog"
	"os"
	"sync/atomic"
)

// defaultLogger is the package-level logger used by the runtime and any
// internal package that imports observability. It is configured once (JSON
// to stderr, info level) and may be swapped via SetLogger. Atomic pointer
// swaps keep concurrent reads safe without a mutex on the hot path.
var defaultLogger atomic.Pointer[slog.Logger]

func init() {
	l := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	defaultLogger.Store(l)
}

// Logger returns the package-level logger. It is never nil.
func Logger() *slog.Logger {
	if l := defaultLogger.Load(); l != nil {
		return l
	}
	// Defensive fallback; init() makes this unreachable in practice.
	return slog.Default()
}

// SetLogger replaces the package-level logger. Pass nil to restore the JSON
// default. Calling this concurrently with Logger() reads is safe; calling it
// concurrently with itself is also safe.
func SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	defaultLogger.Store(l)
}
