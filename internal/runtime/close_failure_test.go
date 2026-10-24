package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type closingSource struct {
	mockSourceSPI
	closeFn func() error
}

func (s *closingSource) Close() error { return s.closeFn() }

type closingProcessor struct {
	mockProcessorSPI
	closeFn func() error
}

func (p *closingProcessor) Close() error { return p.closeFn() }

type closingSink struct {
	mockSinkSPI
	closeFn func() error
}

func (s *closingSink) Close() error { return s.closeFn() }

func TestCloseSessionFailureRetainsOutcomeForEveryRole(t *testing.T) {
	for _, role := range []string{"src", "proc", "sink"} {
		t.Run(role, func(t *testing.T) {
			baseline := activeSessions(t)
			var calls atomic.Int32
			fail := func() error { calls.Add(1); return errors.New("private-password-do-not-return") }
			srv, err := NewPluginServer(newDesc("close-failure"), []ComponentRegistration{
				{Descriptor: &pb.ComponentDescriptor{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}, SourceFactory: func() SourceSPI { return &closingSource{closeFn: fail} }},
				{Descriptor: &pb.ComponentDescriptor{Id: "proc", Kind: pb.ComponentKind_COMPONENT_KIND_PROCESSOR}, ProcessorFactory: func() ProcessorSPI { return &closingProcessor{closeFn: fail} }},
				{Descriptor: &pb.ComponentDescriptor{Id: "sink", Kind: pb.ComponentKind_COMPONENT_KIND_SINK}, SinkFactory: func() SinkSPI { return &closingSink{closeFn: fail} }},
			})
			if err != nil {
				t.Fatal(err)
			}
			created, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: role})
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				_, err = srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: created.SessionId})
				if status.Code(err) != codes.Internal || status.Convert(err).Message() != "plugin cleanup failed" {
					t.Errorf("cleanup failure must remain a sanitized failure: %v", err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("SPI.Close called %d times", calls.Load())
			}
			if got := activeSessions(t); got != baseline+1 {
				t.Fatalf("failed session disappeared from resource gauge: got %v want %v", got, baseline+1)
			}
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-planx-session-id", created.SessionId))
			if _, err := srv.sessionFromMetadata(ctx); status.Code(err) != codes.NotFound {
				t.Fatalf("failed session remained usable: %v", err)
			}
			if _, err := srv.Ack(context.Background(), &pb.AckRequest{SessionId: created.SessionId}); status.Code(err) != codes.NotFound {
				t.Fatalf("failed session ACK: %v", err)
			}
			if err := srv.OpenStream(&pb.StreamOpenRequest{SessionId: created.SessionId}, nil); status.Code(err) != codes.NotFound {
				t.Fatalf("failed session stream: %v", err)
			}
		})
	}
}

func TestCloseSessionPendingDoesNotSucceedOrBlockOtherSessions(t *testing.T) {
	baseline := activeSessions(t)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	slow := &closingSource{closeFn: func() error { calls.Add(1); close(entered); <-release; return errors.New("failed close") }}
	srv := closeTestServer(t, slow)
	created, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() {
		_, e := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: created.SessionId})
		results <- e
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not start")
	}
	go func() {
		_, e := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: created.SessionId})
		results <- e
	}()
	if _, err := srv.Ack(context.Background(), &pb.AckRequest{SessionId: created.SessionId}); status.Code(err) != codes.NotFound {
		t.Fatalf("closing session accepted ACK: %v", err)
	}
	other, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "sink"})
	if err != nil {
		t.Fatal(err)
	}
	healthy := make(chan error, 1)
	go func() {
		_, e := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: other.SessionId})
		healthy <- e
	}()
	select {
	case err := <-healthy:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unrelated session blocked by close")
	}
	if got := activeSessions(t); got != baseline+1 {
		t.Errorf("pending cleanup disappeared from resource gauge: got %v want %v", got, baseline+1)
	}
	select {
	case err := <-results:
		t.Errorf("Close returned before SPI completed: %v", err)
		results <- err
	default:
	}
	release <- struct{}{}
	for range 2 {
		select {
		case err := <-results:
			if status.Code(err) != codes.Internal {
				t.Errorf("concurrent close hid failure: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not complete")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent SPI.Close calls=%d", calls.Load())
	}
}

func TestCloseSessionPanicCannotBecomeLaterSuccess(t *testing.T) {
	var calls atomic.Int32
	source := &closingSource{closeFn: func() error { calls.Add(1); panic("private panic detail") }}
	srv := closeTestServer(t, source)
	created, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = recoveryUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "CloseSession"}, func(ctx context.Context, _ any) (any, error) {
		return srv.CloseSession(ctx, &pb.SessionCloseRequest{SessionId: created.SessionId})
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic was not failure: %v", err)
	}
	_, err = srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: created.SessionId})
	if status.Code(err) != codes.Internal || calls.Load() != 1 {
		t.Fatalf("panic outcome lost or close repeated: calls=%d error=%v", calls.Load(), err)
	}
}

func closeTestServer(t *testing.T, source SourceSPI) *PluginServer {
	t.Helper()
	srv, err := NewPluginServer(newDesc("close-isolation"), []ComponentRegistration{
		srcReg("src", func() SourceSPI { return source }),
		{Descriptor: &pb.ComponentDescriptor{Id: "sink", Kind: pb.ComponentKind_COMPONENT_KIND_SINK}, SinkFactory: func() SinkSPI { return &mockSinkSPI{} }},
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func activeSessions(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "planx_sdk_sessions_active" && len(family.Metric) == 1 {
			return family.Metric[0].GetGauge().GetValue()
		}
	}
	t.Fatal("missing SDK sessions gauge")
	return 0
}

func TestCloseSessionSuccessConcurrentCountsOnce(t *testing.T) {
	baseline := activeSessions(t)
	var calls atomic.Int32
	srv := closeTestServer(t, &closingSource{closeFn: func() error { calls.Add(1); return nil }})
	created, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			if _, err := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: created.SessionId}); err != nil {
				t.Errorf("concurrent close: %v", err)
			}
		})
	}
	group.Wait()
	if calls.Load() != 1 || activeSessions(t) != baseline {
		t.Fatalf("successful close not counted once: calls=%d active=%v baseline=%v", calls.Load(), activeSessions(t), baseline)
	}
}
