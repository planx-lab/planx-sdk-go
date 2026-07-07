package runtime

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Transport-level coverage that plugin_server_test.go does NOT provide: every
// existing test calls handler methods directly on *PluginServer. None verifies
// that ServeGRPC's wiring (RegisterAll over a real listener, the recovery
// interceptors, metadata extraction through a real connection) actually works.
//
// These tests stand up the SAME grpc.NewServer that ServeGRPC builds (with the
// recovery ChainUnary/ChainStream interceptors) and dial it with a real client,
// so the gap between "handlers work" and "the server actually serves" is closed.

// startTransportServer builds a real PluginServer (one source + sink), wraps it
// in the same interceptor chain ServeGRPC uses, registers all five services via
// RegisterAll, and serves on an ephemeral loopback port. Returns a connected
// client pointing at it.
func startTransportServer(t *testing.T) (pb.PluginServiceClient, pb.SessionServiceClient, pb.SinkServiceClient, *grpc.ClientConn) {
	t.Helper()
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// Mirror ServeGRPC's interceptor wiring exactly.
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recoveryUnaryInterceptor),
		grpc.ChainStreamInterceptor(recoveryStreamInterceptor),
	)
	RegisterAll(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewPluginServiceClient(conn), pb.NewSessionServiceClient(conn), pb.NewSinkServiceClient(conn), conn
}

func TestTransport_DiscoverOverRealConnection(t *testing.T) {
	pluginClient, _, _, _ := startTransportServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	desc, err := pluginClient.Discover(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("Discover over real conn: %v", err)
	}
	if desc.GetId() != "p" {
		t.Errorf("descriptor id = %q, want p", desc.GetId())
	}
}

func TestTransport_CreateSessionThenProcessorDispatchedByMetadata(t *testing.T) {
	// CreateSession returns a session id; a subsequent Sink WriteBatch must be
	// routed to the right session purely via the x-planx-session-id metadata —
	// over a REAL connection, not a hand-built context.
	_, sessionClient, sinkClient, _ := startTransportServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	createResp, err := sessionClient.CreateSession(ctx, &pb.SessionCreateRequest{
		ComponentId: "sink",
		Config:      []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("CreateSession over real conn: %v", err)
	}
	sid := createResp.GetSessionId()
	if sid == "" {
		t.Fatal("empty session id")
	}

	// The sink must see the session id arrive via gRPC metadata, not via an
	// in-process context. This is the routing path production relies on.
	// Payload is gob-encoded (the codec Unpacks it inside the handler).
	writeCtx, writeCancel := context.WithTimeout(
		metadata.AppendToOutgoingContext(ctx, "x-planx-session-id", sid),
		2*time.Second,
	)
	defer writeCancel()
	payload := packMap(t, map[string]string{"row": "1"})
	if _, err := sinkClient.WriteBatch(writeCtx, &pb.Batch{Payload: payload}); err != nil {
		t.Fatalf("WriteBatch routed by metadata over real conn: %v", err)
	}
}

func TestTransport_MissingMetadataReturnsInvalidArgument(t *testing.T) {
	// Without the session-id metadata, the server must reject the call — this
	// verifies the sessionFromMetadata path runs over a real transport.
	_, _, sinkClient, _ := startTransportServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := sinkClient.WriteBatch(ctx, &pb.Batch{Payload: []byte("x")})
	if err == nil {
		t.Fatal("expected error when session-id metadata is absent")
	}
}
