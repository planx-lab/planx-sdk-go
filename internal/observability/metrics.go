package observability

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Metric names are prefixed with planx_sdk_ to distinguish SDK-owned
// (plugin-process) metrics from engine-owned metrics in a shared scrape.
var (
	// rpcRequestsTotal counts completed RPCs by service/method/status.
	// status is the gRPC code string ("OK", "INTERNAL", ...).
	rpcRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "planx_sdk_rpc_requests_total",
			Help: "Total number of RPCs processed by the SDK, partitioned by service, method and gRPC status.",
		},
		[]string{"service", "method", "status"},
	)

	// rpcDurationSeconds is the server-side RPC latency histogram.
	rpcDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "planx_sdk_rpc_duration_seconds",
			Help:    "Server-side RPC latency in seconds, partitioned by service and method.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"service", "method"},
	)

	// sessionsActive is the number of currently-open plugin sessions.
	sessionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "planx_sdk_sessions_active",
			Help: "Number of currently active plugin sessions.",
		},
	)
)

// methodInfo splits a gRPC FullMethod ("/planx.plugin.v4.SessionService/CreateSession")
// into a service name and the trailing method label. It is robust to leading
// slashes and missing slashes; on any degenerate input (empty or bare "/") it
// falls back to a self-describing ("unknown", ...) label so the metric never
// records empty strings.
func methodInfo(fullMethod string) (service, method string) {
	s := fullMethod
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[:i], s[i+1:]
		}
	}
	if s == "" {
		// Empty or slash-only input: avoid ("","") labels.
		if fullMethod == "" {
			return "unknown", "unknown"
		}
		return "unknown", fullMethod
	}
	return s, s
}

// codeLabel renders a gRPC error code as a label value. nil err => "OK".
func codeLabel(err error) string {
	if err == nil {
		return "OK"
	}
	return status.Code(err).String()
}

// MetricsUnaryInterceptor records request count and latency for every unary
// RPC handled by the plugin. Observation is deferred so the histogram sample
// and counter always land, even if the handler panics or returns early. It
// must run BEFORE the recovery interceptor in the chain so that panics
// recovered downstream are still observed here as latency; the recovery
// interceptor supplies the INTERNAL status that this counter labels on.
// (Order: metrics -> recovery -> handler.)
func MetricsUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	start := time.Now()
	defer func() {
		service, method := methodInfo(info.FullMethod)
		rpcDurationSeconds.WithLabelValues(service, method).Observe(time.Since(start).Seconds())
		rpcRequestsTotal.WithLabelValues(service, method, codeLabel(err)).Inc()
	}()
	return handler(ctx, req)
}

// MetricsStreamInterceptor records request count and latency for every
// streaming RPC. Observation is deferred so the sample/counter always land,
// regardless of handler outcome. As with the unary variant, it should run
// before the recovery interceptor. The handler error labels the counter.
func MetricsStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	start := time.Now()
	defer func() {
		service, method := methodInfo(info.FullMethod)
		rpcDurationSeconds.WithLabelValues(service, method).Observe(time.Since(start).Seconds())
		rpcRequestsTotal.WithLabelValues(service, method, codeLabel(err)).Inc()
	}()
	return handler(srv, ss)
}

// IncActiveSession increments the active-sessions gauge. Called by the
// runtime when a session is created.
func IncActiveSession() { sessionsActive.Inc() }

// DecActiveSession decrements the active-sessions gauge. Called by the
// runtime when a session is closed.
func DecActiveSession() { sessionsActive.Dec() }

// SetActiveSessions sets the active-sessions gauge to an absolute value. Used
// by tests to assert gauge state directly without depending on increment order.
func SetActiveSessions(n int) { sessionsActive.Set(float64(n)) }
