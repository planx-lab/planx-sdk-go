package runtime

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Mock SPI implementations
// ---------------------------------------------------------------------------

type mockSourceSPI struct {
	initErr     error
	batch       any
	batchErr    error
	closeErr    error
	closeCalled bool
}

func (m *mockSourceSPI) Init(_ context.Context, _ []byte) error { return m.initErr }
func (m *mockSourceSPI) ReadBatch() (any, error)                { return m.batch, m.batchErr }
func (m *mockSourceSPI) Close() error                           { m.closeCalled = true; return m.closeErr }

type mockProcessorSPI struct {
	initErr  error
	out      any
	procErr  error
	closeErr error
}

func (m *mockProcessorSPI) Init(_ context.Context, _ []byte) error { return m.initErr }
func (m *mockProcessorSPI) Process(in any) (any, error)            { return m.out, m.procErr }
func (m *mockProcessorSPI) Close() error                           { return m.closeErr }

type mockSinkSPI struct {
	initErr       error
	writeErr      error
	closeErr      error
	closeCalled   bool
	lastWriteData any
}

func (m *mockSinkSPI) Init(_ context.Context, _ []byte) error { return m.initErr }
func (m *mockSinkSPI) WriteBatch(batch any) error             { m.lastWriteData = batch; return m.writeErr }
func (m *mockSinkSPI) Close() error                           { m.closeCalled = true; return m.closeErr }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ctxWithSessionID returns a context with gRPC metadata containing the given
// session ID under the "x-planx-session-id" key.
func ctxWithSessionID(sid string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-planx-session-id", sid))
}

// ctxNoMetadata returns a bare context with no incoming metadata.
func ctxNoMetadata() context.Context {
	return context.Background()
}

// packMap uses the real gob codec to produce a valid payload from a
// map[string]string, which is pre-registered in batch.init().
func packMap(t *testing.T, m map[string]string) []byte {
	t.Helper()
	codec := newGobCodec()
	packed, err := codec.Pack(m)
	if err != nil {
		t.Fatalf("packMap: %v", err)
	}
	return packed
}

// newGobCodec exposes the internal gobCodec for test helpers.
// We cannot import internal/batch from outside the runtime package
// for packing convenience data, so we reuse the server's own codec field.
// Instead, we just create a server and borrow its codec.
func newGobCodec() interface {
	Pack(batch any) ([]byte, error)
	Unpack(p []byte) (any, error)
} {
	srv := NewSourceServer(func() SourceSPI { return &mockSourceSPI{} })
	return srv.codec
}

// assertCode is a test helper that checks a gRPC status code.
func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		if want != codes.OK {
			t.Fatalf("expected error with code %s, got nil", want)
		}
		return
	}
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if s.Code() != want {
		t.Fatalf("expected code %s, got %s: %v", want, s.Code(), err)
	}
}

// ===========================================================================
// SourceServer tests
// ===========================================================================

func TestSourceServer_CreateSession_Success(t *testing.T) {
	srv := NewSourceServer(func() SourceSPI {
		return &mockSourceSPI{}
	})

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{
		Config: []byte(`{"key":"val"}`),
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("expected non-empty session ID")
	}
}

func TestSourceServer_CreateSession_InitFailure(t *testing.T) {
	wantErr := errors.New("init boom")
	srv := NewSourceServer(func() SourceSPI {
		return &mockSourceSPI{initErr: wantErr}
	})

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{
		Config: []byte("{}"),
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped init error, got: %v", err)
	}
	if resp != nil {
		t.Fatal("expected nil response on error")
	}
}

func TestSourceServer_CloseSession_RemovesAndCallsClose(t *testing.T) {
	spi := &mockSourceSPI{}
	srv := NewSourceServer(func() SourceSPI { return spi })

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := resp.SessionId

	_, err = srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: sid})
	if err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if !spi.closeCalled {
		t.Fatal("expected spi.Close to be called")
	}

	// Session should be gone — Ack should return NotFound
	_, err = srv.Ack(context.Background(), &pb.AckRequest{SessionId: sid})
	assertCode(t, err, codes.NotFound)
}

func TestSourceServer_CloseSession_UnknownSession_NoError(t *testing.T) {
	srv := NewSourceServer(func() SourceSPI { return &mockSourceSPI{} })

	_, err := srv.CloseSession(context.Background(), &pb.SessionCloseRequest{
		SessionId: "nonexistent",
	})
	if err != nil {
		t.Fatalf("expected nil error for unknown session, got: %v", err)
	}
}

func TestSourceServer_Ack_UnknownSession_ReturnsNotFound(t *testing.T) {
	srv := NewSourceServer(func() SourceSPI { return &mockSourceSPI{} })

	_, err := srv.Ack(context.Background(), &pb.AckRequest{
		SessionId: "does-not-exist",
		NewWindow: 10,
	})
	assertCode(t, err, codes.NotFound)
}

func TestSourceServer_Ack_ValidSession_ReleasesWindow(t *testing.T) {
	srv := NewSourceServer(func() SourceSPI { return &mockSourceSPI{} })

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := resp.SessionId

	_, err = srv.Ack(context.Background(), &pb.AckRequest{
		SessionId: sid,
		NewWindow: 5,
	})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Second Ack should also succeed.
	_, err = srv.Ack(context.Background(), &pb.AckRequest{
		SessionId: sid,
		NewWindow: 3,
	})
	if err != nil {
		t.Fatalf("second Ack: %v", err)
	}
}

// ===========================================================================
// ProcessorServer tests
// ===========================================================================

func TestProcessorServer_CreateSession_Success(t *testing.T) {
	srv := NewProcessorServer(func() ProcessorSPI {
		return &mockProcessorSPI{}
	})

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{
		Config: []byte(`{"key":"val"}`),
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("expected non-empty session ID")
	}
}

func TestProcessorServer_Process_MissingMetadata(t *testing.T) {
	srv := NewProcessorServer(func() ProcessorSPI { return &mockProcessorSPI{} })

	_, err := srv.Process(ctxNoMetadata(), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.InvalidArgument)
}

func TestProcessorServer_Process_MissingSessionID(t *testing.T) {
	srv := NewProcessorServer(func() ProcessorSPI { return &mockProcessorSPI{} })

	// Metadata present but no x-planx-session-id key.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	_, err := srv.Process(ctx, &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.InvalidArgument)
}

func TestProcessorServer_Process_UnknownSession(t *testing.T) {
	srv := NewProcessorServer(func() ProcessorSPI { return &mockProcessorSPI{} })

	ctx := ctxWithSessionID("nonexistent")
	_, err := srv.Process(ctx, &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.NotFound)
}

func TestProcessorServer_Process_ValidSession(t *testing.T) {
	srv := NewProcessorServer(func() ProcessorSPI {
		return &mockProcessorSPI{
			out: map[string]string{"result": "ok"},
		}
	})

	// Create a session first.
	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := resp.SessionId

	// Build a valid packed payload.
	payload := packMap(t, map[string]string{"input": "data"})

	ctx := ctxWithSessionID(sid)
	out, err := srv.Process(ctx, &pb.Batch{Payload: payload})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(out.Payload) == 0 {
		t.Fatal("expected non-empty payload in response")
	}
}

func TestProcessorServer_CloseSession(t *testing.T) {
	spi := &mockProcessorSPI{}
	srv := NewProcessorServer(func() ProcessorSPI { return spi })

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := resp.SessionId

	_, err = srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: sid})
	if err != nil {
		t.Fatalf("CloseSession: %v", err)
	}

	// Session should be gone.
	ctx := ctxWithSessionID(sid)
	_, err = srv.Process(ctx, &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.NotFound)
}

// ===========================================================================
// SinkServer tests
// ===========================================================================

func TestSinkServer_CreateSession_Success(t *testing.T) {
	srv := NewSinkServer(func() SinkSPI {
		return &mockSinkSPI{}
	})

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{
		Config: []byte(`{"key":"val"}`),
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if resp.SessionId == "" {
		t.Fatal("expected non-empty session ID")
	}
}

func TestSinkServer_WriteBatch_MissingMetadata(t *testing.T) {
	srv := NewSinkServer(func() SinkSPI { return &mockSinkSPI{} })

	_, err := srv.WriteBatch(ctxNoMetadata(), &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.InvalidArgument)
}

func TestSinkServer_WriteBatch_UnknownSession(t *testing.T) {
	srv := NewSinkServer(func() SinkSPI { return &mockSinkSPI{} })

	ctx := ctxWithSessionID("nonexistent")
	_, err := srv.WriteBatch(ctx, &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.NotFound)
}

func TestSinkServer_WriteBatch_ValidSession(t *testing.T) {
	spi := &mockSinkSPI{}
	srv := NewSinkServer(func() SinkSPI { return spi })

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := resp.SessionId

	payload := packMap(t, map[string]string{"row": "1"})

	ctx := ctxWithSessionID(sid)
	_, err = srv.WriteBatch(ctx, &pb.Batch{Payload: payload})
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if spi.lastWriteData == nil {
		t.Fatal("expected WriteBatch to be called with unpacked data")
	}
}

func TestSinkServer_CloseSession(t *testing.T) {
	spi := &mockSinkSPI{}
	srv := NewSinkServer(func() SinkSPI { return spi })

	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sid := resp.SessionId

	_, err = srv.CloseSession(context.Background(), &pb.SessionCloseRequest{SessionId: sid})
	if err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if !spi.closeCalled {
		t.Fatal("expected spi.Close to be called")
	}

	// Session should be gone.
	ctx := ctxWithSessionID(sid)
	_, err = srv.WriteBatch(ctx, &pb.Batch{Payload: []byte("x")})
	assertCode(t, err, codes.NotFound)
}

// ===========================================================================
// SourceServer.OpenStream tests
// ===========================================================================

// mockSourceStream implements grpc.ServerStream plus the generated Send(*pb.Batch),
// so OpenStream can be driven without a real gRPC connection.
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

// When a source signals exhaustion via io.EOF, OpenStream must close the gRPC
// stream cleanly (return nil). gRPC then surfaces io.EOF to the engine's Recv(),
// which classifies it as CodeEOF -> pipeline SUCCEEDED. Returning io.EOF would
// be wrapped by gRPC as code=Unknown and misread as a plugin failure.
func TestSourceServer_OpenStream_SourceEOF_ClosesCleanly(t *testing.T) {
	srv := NewSourceServer(func() SourceSPI {
		return &mockSourceSPI{batchErr: io.EOF}
	})
	resp, err := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	stream := &mockSourceStream{ctx: context.Background()}
	err = srv.OpenStream(&pb.StreamOpenRequest{
		SessionId:     resp.SessionId,
		InitialWindow: 10,
	}, stream)
	if err != nil {
		t.Fatalf("OpenStream on source io.EOF: want nil (clean close -> engine CodeEOF), got %v", err)
	}
}

// A real (non-EOF) source error must still propagate, not be swallowed.
func TestSourceServer_OpenStream_RealError_Propagates(t *testing.T) {
	wantErr := errors.New("source boom")
	srv := NewSourceServer(func() SourceSPI {
		return &mockSourceSPI{batchErr: wantErr}
	})
	resp, _ := srv.CreateSession(context.Background(), &pb.SessionCreateRequest{Config: []byte("{}")})

	stream := &mockSourceStream{ctx: context.Background()}
	err := srv.OpenStream(&pb.StreamOpenRequest{
		SessionId:     resp.SessionId,
		InitialWindow: 10,
	}, stream)
	if !errors.Is(err, wantErr) {
		t.Fatalf("OpenStream on real error: want %v, got %v", wantErr, err)
	}
}
