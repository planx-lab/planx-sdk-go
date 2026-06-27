package runtime

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func newDesc(id string, comps ...*pb.ComponentDescriptor) *pb.PluginDescriptor {
	return &pb.PluginDescriptor{Id: id, Components: comps}
}

func srcReg(id string, factory func() SourceSPI) ComponentRegistration {
	return ComponentRegistration{
		Descriptor:    &pb.ComponentDescriptor{Id: id, Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE},
		SourceFactory: factory,
	}
}

func TestNewPluginServer_NilDescriptor(t *testing.T) {
	_, err := NewPluginServer(nil, []ComponentRegistration{srcReg("s", func() SourceSPI { return &mockSourceSPI{} })})
	if err == nil {
		t.Fatal("expected error for nil descriptor")
	}
}

func TestNewPluginServer_NoComponents(t *testing.T) {
	_, err := NewPluginServer(newDesc("p"), nil)
	if err == nil {
		t.Fatal("expected error for zero components")
	}
}

func TestNewPluginServer_DuplicateComponentID(t *testing.T) {
	comps := []ComponentRegistration{
		srcReg("dup", func() SourceSPI { return &mockSourceSPI{} }),
		srcReg("dup", func() SourceSPI { return &mockSourceSPI{} }),
	}
	_, err := NewPluginServer(newDesc("p"), comps)
	if err == nil {
		t.Fatal("expected error for duplicate component id")
	}
}

func TestNewPluginServer_EmptyComponentID(t *testing.T) {
	comps := []ComponentRegistration{
		srcReg("", func() SourceSPI { return &mockSourceSPI{} }),
	}
	_, err := NewPluginServer(newDesc("p"), comps)
	if err == nil {
		t.Fatal("expected error for empty component id")
	}
}

func TestNewPluginServer_Success(t *testing.T) {
	srv, err := NewPluginServer(newDesc("p"), []ComponentRegistration{
		srcReg("s", func() SourceSPI { return &mockSourceSPI{} }),
	})
	if err != nil {
		t.Fatalf("NewPluginServer: %v", err)
	}
	if srv == nil || len(srv.components) != 1 {
		t.Fatalf("expected 1 registered component, got %v", srv)
	}
}

func TestCreateSession_UnknownComponent(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "nope"})
	assertCode(t, err, codes.NotFound)
}

func TestCreateSession_Source_InitsAndReturnsID(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{
		ComponentId: "src", Config: []byte(`{"k":"v"}`),
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if resp.GetSessionId() == "" {
		t.Fatal("expected non-empty session id")
	}
}

func TestCreateSession_Source_InitFailure(t *testing.T) {
	wantErr := errors.New("init boom")
	srv, _ := NewPluginServer(newDesc("p"), []ComponentRegistration{{
		Descriptor:    &pb.ComponentDescriptor{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE},
		SourceFactory: func() SourceSPI { return &mockSourceSPI{initErr: wantErr} },
	}})
	_, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped init error, got: %v", err)
	}
}

func TestCloseSession_CallsSPICloseAndRemoves(t *testing.T) {
	src := &mockSourceSPI{}
	srv := buildTestServer(t, src, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	sid := resp.GetSessionId()

	if _, err := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: sid}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if !src.closeCalled {
		t.Fatal("expected spi.Close to be called")
	}
	// Session gone -> Ack NotFound.
	_, err := srv.Ack(context.Background(), &pb.AckRequest{SessionId: sid})
	assertCode(t, err, codes.NotFound)
}

func TestCloseSession_UnknownSession_NoError(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: "ghost"})
	if err != nil {
		t.Fatalf("expected nil error for unknown session, got: %v", err)
	}
}

func TestAck_Source_ReleasesWindow(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	sid := resp.GetSessionId()

	for _, w := range []int32{5, 3, 2} {
		if _, err := srv.Ack(context.Background(), &pb.AckRequest{SessionId: sid, NewWindow: w}); err != nil {
			t.Fatalf("Ack(%d): %v", w, err)
		}
	}
}

func TestAck_UnknownSession_NotFound(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.Ack(context.Background(), &pb.AckRequest{SessionId: "ghost"})
	assertCode(t, err, codes.NotFound)
}

func TestDiscover_ReturnsDescriptor(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	desc, err := srv.Discover(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if desc.GetId() != "p" {
		t.Fatalf("expected descriptor id 'p', got %q", desc.GetId())
	}
	if len(desc.GetComponents()) != 3 {
		t.Fatalf("expected 3 components, got %d", len(desc.GetComponents()))
	}
}

func TestHealth_DefaultReady(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	h, err := srv.Health(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if h.GetState() != pb.HealthStatus_STATE_READY {
		t.Fatalf("expected READY, got %v", h.GetState())
	}
}

func TestValidateConfig_UnknownComponent(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.ValidateConfig(context.Background(), &pb.ConfigValidationRequest{ComponentId: "nope"})
	assertCode(t, err, codes.NotFound)
}

func TestValidateConfig_NoHook_DefaultsOk(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	res, err := srv.ValidateConfig(context.Background(), &pb.ConfigValidationRequest{ComponentId: "src"})
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	if !res.GetOk() {
		t.Fatal("expected ok=true when no hook registered")
	}
}

func TestValidateConfig_WithHook(t *testing.T) {
	srv, _ := NewPluginServer(newDesc("p"), []ComponentRegistration{{
		Descriptor:    &pb.ComponentDescriptor{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE},
		SourceFactory: func() SourceSPI { return &mockSourceSPI{} },
		Validate: func(_ context.Context, _ []byte) (bool, string) {
			return false, "cannot reach upstream"
		},
	}})
	res, err := srv.ValidateConfig(context.Background(), &pb.ConfigValidationRequest{ComponentId: "src"})
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	if res.GetOk() {
		t.Fatal("expected ok=false from hook")
	}
	if res.GetMessage() != "cannot reach upstream" {
		t.Fatalf("expected hook message, got %q", res.GetMessage())
	}
}

// mockSourceStream implements grpc.ServerStreamingServer[*pb.Batch] for tests.
type mockSourceStream struct {
	ctx     context.Context
	sent    []*pb.Batch
	sendErr error
}

func (m *mockSourceStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockSourceStream) SendHeader(metadata.MD) error { return nil }
func (m *mockSourceStream) SetTrailer(metadata.MD)       {}
func (m *mockSourceStream) Context() context.Context     { return m.ctx }
func (m *mockSourceStream) SendMsg(any) error            { return nil }
func (m *mockSourceStream) RecvMsg(any) error            { return nil }
func (m *mockSourceStream) Send(b *pb.Batch) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, b)
	return nil
}

func TestOpenStream_SourceEOF_ClosesCleanly(t *testing.T) {
	src := &mockSourceSPI{batchErr: io.EOF}
	srv := buildTestServer(t, src, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})

	err := srv.OpenStream(&pb.StreamOpenRequest{SessionId: resp.GetSessionId(), InitialWindow: 10},
		&mockSourceStream{ctx: context.Background()})
	if err != nil {
		t.Fatalf("OpenStream on io.EOF: want nil (clean close), got %v", err)
	}
}

func TestOpenStream_RealError_Propagates(t *testing.T) {
	wantErr := errors.New("source boom")
	src := &mockSourceSPI{batchErr: wantErr}
	srv := buildTestServer(t, src, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})

	err := srv.OpenStream(&pb.StreamOpenRequest{SessionId: resp.GetSessionId(), InitialWindow: 10},
		&mockSourceStream{ctx: context.Background()})
	if !errors.Is(err, wantErr) {
		t.Fatalf("OpenStream: want %v, got %v", wantErr, err)
	}
}

func TestOpenStream_UnknownSession_NotFound(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	err := srv.OpenStream(&pb.StreamOpenRequest{SessionId: "ghost"},
		&mockSourceStream{ctx: context.Background()})
	assertCode(t, err, codes.NotFound)
}

func TestOpenStream_NonSourceSession_FailedPrecondition(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	// Create a PROCESSOR session, then try to OpenStream on it.
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "proc"})
	err := srv.OpenStream(&pb.StreamOpenRequest{SessionId: resp.GetSessionId()},
		&mockSourceStream{ctx: context.Background()})
	assertCode(t, err, codes.FailedPrecondition)
}

func TestOpenStream_SendsBatches(t *testing.T) {
	src := &mockSourceSPI{batches: []any{map[string]string{"x": "1"}}, errs: []error{nil, io.EOF}}
	srv := buildTestServer(t, src, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	st := &mockSourceStream{ctx: context.Background()}
	if err := srv.OpenStream(&pb.StreamOpenRequest{SessionId: resp.GetSessionId(), InitialWindow: 4}, st); err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if len(st.sent) != 1 {
		t.Fatalf("expected 1 sent batch, got %d", len(st.sent))
	}
}

func TestProcess_MissingMetadata(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.Process(ctxNoMetadata(), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.InvalidArgument)
}

func TestProcess_MissingSessionID(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	_, err := srv.Process(ctx, &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.InvalidArgument)
}

func TestProcess_UnknownSession(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.Process(ctxWithSessionID("ghost"), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.NotFound)
}

func TestProcess_ValidSession(t *testing.T) {
	proc := &mockProcessorSPI{out: map[string]string{"r": "ok"}}
	srv := buildTestServer(t, &mockSourceSPI{}, proc, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "proc"})

	payload := packMap(t, map[string]string{"input": "data"})
	out, err := srv.Process(ctxWithSessionID(resp.GetSessionId()), &pb.Batch{Payload: payload})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(out.GetPayload()) == 0 {
		t.Fatal("expected non-empty payload")
	}
}

func TestProcess_NonProcessorSession_FailedPrecondition(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	_, err := srv.Process(ctxWithSessionID(resp.GetSessionId()), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.FailedPrecondition)
}

func TestWriteBatch_MissingMetadata(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.WriteBatch(ctxNoMetadata(), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.InvalidArgument)
}

func TestWriteBatch_UnknownSession(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	_, err := srv.WriteBatch(ctxWithSessionID("ghost"), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.NotFound)
}

func TestWriteBatch_ValidSession(t *testing.T) {
	sink := &mockSinkSPI{}
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, sink)
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "sink"})

	payload := packMap(t, map[string]string{"row": "1"})
	if _, err := srv.WriteBatch(ctxWithSessionID(resp.GetSessionId()), &pb.Batch{Payload: payload}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if sink.lastWrite == nil {
		t.Fatal("expected WriteBatch to deliver unpacked data to SPI")
	}
}

func TestWriteBatch_NonSinkSession_FailedPrecondition(t *testing.T) {
	srv := buildTestServer(t, &mockSourceSPI{}, &mockProcessorSPI{}, &mockSinkSPI{})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{ComponentId: "src"})
	_, err := srv.WriteBatch(ctxWithSessionID(resp.GetSessionId()), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.FailedPrecondition)
}
