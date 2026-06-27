package runtime

import (
	"context"
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// --- mock SPIs ---

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
	initErr     error
	writeErr    error
	closeErr    error
	closeCalled bool
	lastWrite   any
}

func (m *mockSinkSPI) Init(_ context.Context, _ []byte) error { return m.initErr }
func (m *mockSinkSPI) WriteBatch(b any) error                 { m.lastWrite = b; return m.writeErr }
func (m *mockSinkSPI) Close() error                           { m.closeCalled = true; return m.closeErr }

// --- helpers ---

func ctxWithSessionID(sid string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-planx-session-id", sid))
}

func ctxNoMetadata() context.Context { return context.Background() }

// packMap builds a gob payload from map[string]string (pre-registered in batch.init).
func packMap(t *testing.T, m map[string]string) []byte {
	t.Helper()
	srv, err := NewPluginServer(newDesc("p"), []ComponentRegistration{srcReg("s", func() SourceSPI { return &mockSourceSPI{} })})
	if err != nil {
		t.Fatalf("packMap server: %v", err)
	}
	packed, err := srv.codec.Pack(m)
	if err != nil {
		t.Fatalf("packMap: %v", err)
	}
	return packed
}

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

// buildTestServer builds a PluginServer with one source, one processor, one sink
// component. SPI pointers are returned so individual tests can assert on them.
func buildTestServer(t *testing.T, src *mockSourceSPI, proc *mockProcessorSPI, sink *mockSinkSPI) *PluginServer {
	t.Helper()
	srv, err := NewPluginServer(newDesc("p",
		&pb.ComponentDescriptor{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE},
		&pb.ComponentDescriptor{Id: "proc", Kind: pb.ComponentKind_COMPONENT_KIND_PROCESSOR},
		&pb.ComponentDescriptor{Id: "sink", Kind: pb.ComponentKind_COMPONENT_KIND_SINK},
	), []ComponentRegistration{
		{Descriptor: &pb.ComponentDescriptor{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}, SourceFactory: func() SourceSPI { return src }},
		{Descriptor: &pb.ComponentDescriptor{Id: "proc", Kind: pb.ComponentKind_COMPONENT_KIND_PROCESSOR}, ProcessorFactory: func() ProcessorSPI { return proc }},
		{Descriptor: &pb.ComponentDescriptor{Id: "sink", Kind: pb.ComponentKind_COMPONENT_KIND_SINK}, SinkFactory: func() SinkSPI { return sink }},
	})
	if err != nil {
		t.Fatalf("buildTestServer: %v", err)
	}
	return srv
}
