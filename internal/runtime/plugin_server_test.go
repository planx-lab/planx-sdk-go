package runtime

import (
	"context"
	"errors"
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
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
