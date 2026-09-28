package observability

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMethodInfo(t *testing.T) {
	tests := []struct {
		full        string
		wantService string
		wantMethod  string
	}{
		{"/planx.plugin.v4.SessionService/CreateSession", "planx.plugin.v4.SessionService", "CreateSession"},
		{"planx.plugin.v4.SourceService/Read", "planx.plugin.v4.SourceService", "Read"},
		{"/NoSlash/Handle", "NoSlash", "Handle"},
		{"Bare", "Bare", "Bare"},
		{"/", "unknown", "/"},
		{"", "unknown", "unknown"},
	}
	for _, tc := range tests {
		gotSvc, gotMethod := methodInfo(tc.full)
		if gotSvc != tc.wantService || gotMethod != tc.wantMethod {
			t.Errorf("methodInfo(%q) = (%q,%q), want (%q,%q)", tc.full, gotSvc, gotMethod, tc.wantService, tc.wantMethod)
		}
	}
}

func TestCodeLabel(t *testing.T) {
	if got := codeLabel(nil); got != "OK" {
		t.Errorf("codeLabel(nil) = %q, want OK", got)
	}
	if got := codeLabel(status.Error(codes.Internal, "boom")); got != codes.Internal.String() {
		t.Errorf("codeLabel(internal) = %q, want %q", got, codes.Internal.String())
	}
	// A plain (non-gRPC) error is reported as Unknown, never panics.
	if got := codeLabel(errors.New("plain")); got != codes.Unknown.String() {
		t.Errorf("codeLabel(plain) = %q, want %q", got, codes.Unknown.String())
	}
}

func TestMetricsUnaryInterceptor_SuccessIncrementsCounter(t *testing.T) {
	// The collectors are package-global and shared across tests, so reset the
	// relevant counter labels to a known baseline by ignoring prior state and
	// asserting only on the delta using testutil's CollectAndCount.
	const fullMethod = "/test.svc.ok/Method"
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	before := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues("test.svc.ok", "Method", "OK"))

	_, err := MetricsUnaryInterceptor(
		context.Background(),
		"req",
		&grpc.UnaryServerInfo{FullMethod: fullMethod},
		handler,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	after := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues("test.svc.ok", "Method", "OK"))
	if got := after - before; got != 1 {
		t.Fatalf("counter delta = %v, want 1", got)
	}
}

func TestMetricsUnaryInterceptor_ErrorLabelsByCode(t *testing.T) {
	wantErr := status.Error(codes.NotFound, "nope")
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, wantErr
	}

	before := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues("test.svc.err", "Method", codes.NotFound.String()))

	_, err := MetricsUnaryInterceptor(
		context.Background(),
		"req",
		&grpc.UnaryServerInfo{FullMethod: "/test.svc.err/Method"},
		handler,
	)
	if !errors.Is(err, wantErr) && status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound error from interceptor, got %v", err)
	}

	after := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues("test.svc.err", "Method", codes.NotFound.String()))
	if got := after - before; got != 1 {
		t.Fatalf("counter delta = %v, want 1", got)
	}
}

// TestMetricsUnaryInterceptor_ErrorRecordsHistogram is the review-required
// coverage for the previously-untested error-path histogram. It drives the
// interceptor with a failing handler and asserts that BOTH the request
// counter increments with the correct status label AND the latency histogram
// gains a series (the interceptor only calls Observe, so a fresh series
// proves an observation landed). Histogram series count is asserted via
// CollectAndCount, the same pattern used by TestRegistryExposesAllMetrics;
// ToFloat64 cannot read a histogram Observer.
func TestMetricsUnaryInterceptor_ErrorRecordsHistogram(t *testing.T) {
	const fullMethod = "/test.svc.hist/Method"
	service, method := "test.svc.hist", "Method"
	label := codes.Unavailable.String()

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.Unavailable, "down")
	}

	beforeSeries := testutil.CollectAndCount(rpcDurationSeconds)
	beforeCounter := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues(service, method, label))

	_, err := MetricsUnaryInterceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: fullMethod},
		handler,
	)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}

	afterSeries := testutil.CollectAndCount(rpcDurationSeconds)
	if afterSeries <= beforeSeries {
		t.Fatalf("histogram series count did not grow: before=%d after=%d", beforeSeries, afterSeries)
	}
	afterCounter := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues(service, method, label))
	if got := afterCounter - beforeCounter; got != 1 {
		t.Fatalf("counter delta = %v, want 1", got)
	}
}

// TestMetricsUnaryInterceptor_PanicStillObserves proves the deferred
// observation in I2 survives a handler panic. Without the defer, the counter
// and histogram would be silently dropped. The recovery interceptor normally
// sits downstream of metrics and converts the panic to an INTERNAL error, but
// this test runs without it to prove the defer records regardless: the
// (service,method) histogram series is created and the counter advances.
func TestMetricsUnaryInterceptor_PanicStillObserves(t *testing.T) {
	const fullMethod = "/test.svc.panic/Method"
	service, method := "test.svc.panic", "Method"

	beforeSeries := testutil.CollectAndCount(rpcDurationSeconds)
	beforeCounter := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues(service, method, codes.OK.String()))

	panicky := func(ctx context.Context, req any) (any, error) { panic("boom") }

	func() {
		defer func() { _ = recover() }()
		_, _ = MetricsUnaryInterceptor(
			context.Background(),
			nil,
			&grpc.UnaryServerInfo{FullMethod: fullMethod},
			panicky,
		)
	}()

	afterSeries := testutil.CollectAndCount(rpcDurationSeconds)
	if afterSeries <= beforeSeries {
		t.Fatalf("histogram series count did not grow after panic: before=%d after=%d (defer must observe)", beforeSeries, afterSeries)
	}
	afterCounter := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues(service, method, codes.OK.String()))
	if got := afterCounter - beforeCounter; got != 1 {
		t.Fatalf("counter delta after panic = %v, want 1 (defer must observe)", got)
	}
}

type mockStream struct{ grpc.ServerStream }

func TestMetricsStreamInterceptor_IncrementsCounter(t *testing.T) {
	handler := func(srv any, ss grpc.ServerStream) error { return nil }

	before := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues("test.svc.stream", "Stream", "OK"))

	err := MetricsStreamInterceptor(
		"srv",
		&mockStream{},
		&grpc.StreamServerInfo{FullMethod: "/test.svc.stream/Stream"},
		handler,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	after := testutil.ToFloat64(rpcRequestsTotal.WithLabelValues("test.svc.stream", "Stream", "OK"))
	if got := after - before; got != 1 {
		t.Fatalf("counter delta = %v, want 1", got)
	}
}

func TestActiveSessionGauge(t *testing.T) {
	SetActiveSessions(0)
	IncActiveSession()
	IncActiveSession()
	if got := testutil.ToFloat64(sessionsActive); got != 2 {
		t.Fatalf("after two Inc: gauge = %v, want 2", got)
	}
	DecActiveSession()
	if got := testutil.ToFloat64(sessionsActive); got != 1 {
		t.Fatalf("after Dec: gauge = %v, want 1", got)
	}
	SetActiveSessions(0)
}

// TestRegistryExposesAllMetrics ensures every SDK metric is registered on the
// default registry (so promhttp.Handler() exposes them) and that the counter
// and histogram collectors each carry at least one labelled series after an
// RPC is observed.
func TestRegistryExposesAllMetrics(t *testing.T) {
	// Drive one RPC through the interceptor so the histogram has a sample.
	_, _ = MetricsUnaryInterceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: "/scrape.test/Ping"},
		func(ctx context.Context, req any) (any, error) { return nil, nil },
	)

	if n := testutil.CollectAndCount(rpcRequestsTotal); n < 1 {
		t.Fatalf("rpcRequestsTotal has %d series, want >=1", n)
	}
	if n := testutil.CollectAndCount(rpcDurationSeconds); n < 1 {
		t.Fatalf("rpcDurationSeconds has %d series, want >=1", n)
	}
}

func TestStartMetricsServer_DisabledByDefault(t *testing.T) {
	t.Setenv(MetricsAddrEnv, "")
	ms, err := StartMetricsServer()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ms != nil {
		t.Fatalf("expected nil MetricsServer when env unset, got %v", ms)
		ms.Stop()
	}
	// Calling Stop on a nil receiver must be safe.
	var nilMS *MetricsServer
	nilMS.Stop()
}

func TestStartMetricsServer_ServesMetrics(t *testing.T) {
	t.Setenv(MetricsAddrEnv, "127.0.0.1:0")
	ms, err := StartMetricsServer()
	if err != nil {
		t.Fatalf("StartMetricsServer: %v", err)
	}
	if ms == nil {
		t.Fatal("expected non-nil MetricsServer")
	}
	defer ms.Stop()

	// Reset gauge so the assertion below is deterministic.
	SetActiveSessions(0)
	IncActiveSession()

	resp, err := http.Get("http://" + ms.Addr() + "/metrics")
	if err != nil {
		t.Fatalf("scrape /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "planx_sdk_sessions_active") {
		t.Errorf("metrics body missing planx_sdk_sessions_active:\n%s", body)
	}
	if !strings.Contains(string(body), "planx_sdk_rpc_requests_total") {
		t.Errorf("metrics body missing planx_sdk_rpc_requests_total:\n%s", body)
	}
}
