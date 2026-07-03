package runtime_test

import (
	"context"
	"errors"
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These tests exercise the DiscoverSchema handler. They live in an external
// test package for symmetry with the other PluginServer end-to-end tests; the
// Discover hook signature references only proto types (*pb.DiscoverSchemaResponse),
// so the runtime package itself has no sdk dependency.

// discoverReg builds a SOURCE ComponentRegistration carrying an optional
// Discover hook returning *pb.DiscoverSchemaResponse, mirroring how real DB
// sources are wired by sdk.buildRegistration.
func discoverReg(id string, discover func(ctx context.Context, config []byte) (*pb.DiscoverSchemaResponse, error)) runtime.ComponentRegistration {
	return runtime.ComponentRegistration{
		Descriptor:    &pb.ComponentDescriptor{Id: id, Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE},
		SourceFactory: func() runtime.SourceSPI { return &stubSourceSPI{} },
		Discover:      discover,
	}
}

func TestDiscoverSchema_Implemented(t *testing.T) {
	srv, err := runtime.NewPluginServer(
		&pb.PluginDescriptor{Id: "p", Components: []*pb.ComponentDescriptor{{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}}},
		[]runtime.ComponentRegistration{
			discoverReg("src", func(_ context.Context, _ []byte) (*pb.DiscoverSchemaResponse, error) {
				return &pb.DiscoverSchemaResponse{
					Tables: []*pb.TableInfo{{Schema: "public", Name: "users_src"}},
					Columns: []*pb.ColumnInfo{
						{Name: "id", Type: "integer", Nullable: false},
						{Name: "name", Type: "text", Nullable: true},
					},
				}, nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("NewPluginServer: %v", err)
	}
	resp, err := srv.DiscoverSchema(context.Background(), &pb.DiscoverSchemaRequest{
		ComponentId: "src", Config: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("DiscoverSchema: %v", err)
	}
	if len(resp.GetTables()) != 1 {
		t.Fatalf("expected 1 table, got %d", len(resp.GetTables()))
	}
	if got := resp.GetTables()[0]; got.GetSchema() != "public" || got.GetName() != "users_src" {
		t.Fatalf("unexpected table: %+v", got)
	}
	if len(resp.GetColumns()) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(resp.GetColumns()))
	}
	c := resp.GetColumns()[0]
	if c.GetName() != "id" || c.GetType() != "integer" || c.GetNullable() {
		t.Fatalf("unexpected column[0]: %+v", c)
	}
}

func TestDiscoverSchema_NotImplemented(t *testing.T) {
	// Component without a Discover hook -> empty response, no error.
	srv, err := runtime.NewPluginServer(
		&pb.PluginDescriptor{Id: "p", Components: []*pb.ComponentDescriptor{{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}}},
		[]runtime.ComponentRegistration{discoverReg("src", nil)},
	)
	if err != nil {
		t.Fatalf("NewPluginServer: %v", err)
	}
	resp, err := srv.DiscoverSchema(context.Background(), &pb.DiscoverSchemaRequest{
		ComponentId: "src", Config: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("DiscoverSchema on non-DB source: expected nil error, got: %v", err)
	}
	if len(resp.GetTables()) != 0 || len(resp.GetColumns()) != 0 {
		t.Fatalf("expected empty response, got tables=%d columns=%d",
			len(resp.GetTables()), len(resp.GetColumns()))
	}
}

func TestDiscoverSchema_UnknownComponent(t *testing.T) {
	srv, err := runtime.NewPluginServer(
		&pb.PluginDescriptor{Id: "p", Components: []*pb.ComponentDescriptor{{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}}},
		[]runtime.ComponentRegistration{discoverReg("src", nil)},
	)
	if err != nil {
		t.Fatalf("NewPluginServer: %v", err)
	}
	_, err = srv.DiscoverSchema(context.Background(), &pb.DiscoverSchemaRequest{
		ComponentId: "ghost", Config: []byte(`{}`),
	})
	assertCode(t, err, codes.NotFound)
}

func TestDiscoverSchema_DiscoverError(t *testing.T) {
	srv, err := runtime.NewPluginServer(
		&pb.PluginDescriptor{Id: "p", Components: []*pb.ComponentDescriptor{{Id: "src", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}}},
		[]runtime.ComponentRegistration{
			discoverReg("src", func(_ context.Context, _ []byte) (*pb.DiscoverSchemaResponse, error) {
				return nil, status.Error(codes.Internal, "db unreachable")
			}),
		},
	)
	if err != nil {
		t.Fatalf("NewPluginServer: %v", err)
	}
	_, err = srv.DiscoverSchema(context.Background(), &pb.DiscoverSchemaRequest{
		ComponentId: "src", Config: []byte(`{}`),
	})
	assertCode(t, err, codes.Internal)
}

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if s.Code() != want {
		t.Fatalf("expected code %s, got %s: %v", want, s.Code(), err)
	}
}

// stubSourceSPI is a no-op SourceSPI for the external test package (the
// internal mock lives in an inaccessible _test.go file).
type stubSourceSPI struct{}

func (s *stubSourceSPI) Init(context.Context, []byte) error { return nil }
func (s *stubSourceSPI) ReadBatch() (any, error)           { return nil, errors.New("not used") }
func (s *stubSourceSPI) Close() error                       { return nil }
