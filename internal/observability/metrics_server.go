package observability

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsAddrEnv is the environment variable that, when set to a non-empty
// listen address (e.g. "127.0.0.1:9100" or ":9100"), enables an HTTP server
// that serves the Prometheus default registry at /metrics. When unset or
// empty the server is NOT started — preserving the SDK's default behavior of
// zero extra listening sockets inside the plugin process.
const MetricsAddrEnv = "PLANX_SDK_METRICS_ADDR"

// MetricsServer is an optional, opt-in HTTP server exposing /metrics. A nil
// *MetricsServer is the disabled state; StartMetricsServer returns (nil, nil)
// when the env var is unset so callers can simply call Stop() on the result.
type MetricsServer struct {
	server *http.Server
	addr   string
}

// readMetricsAddr resolves the configured listen address. Returns "" when
// disabled (the env var is unset or explicitly empty).
func readMetricsAddr() string {
	return os.Getenv(MetricsAddrEnv)
}

// StartMetricsServer starts an HTTP server serving /metrics from the default
// Prometheus registry, iff MetricsAddrEnv is set to a non-empty address.
// When the env var is empty it returns (nil, nil) without doing anything.
// The server is started in a background goroutine; the caller should defer
// Stop() to shut it down cleanly.
func StartMetricsServer() (*MetricsServer, error) {
	addr := readMetricsAddr()
	if addr == "" {
		return nil, nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			Logger().Error("planx-sdk metrics server stopped", "err", err, "addr", addr)
		}
	}()

	Logger().Info("planx-sdk metrics server listening", "addr", ln.Addr().String())
	return &MetricsServer{server: srv, addr: ln.Addr().String()}, nil
}

// Stop gracefully shuts the metrics server down. Safe to call on a nil
// receiver (disabled state) — it is a no-op then.
func (m *MetricsServer) Stop() {
	if m == nil || m.server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.server.Shutdown(ctx); err != nil {
		Logger().Error("planx-sdk metrics server shutdown error", "err", err)
	}
}

// Addr returns the actual address the server bound to. Empty for the disabled
// (nil) state.
func (m *MetricsServer) Addr() string {
	if m == nil {
		return ""
	}
	return m.addr
}
