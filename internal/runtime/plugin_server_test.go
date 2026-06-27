package runtime

import (
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
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
	srv, err := NewPluginServer(newDesc("p"),
		srcReg("s", func() SourceSPI { return &mockSourceSPI{} }),
	)
	if err != nil {
		t.Fatalf("NewPluginServer: %v", err)
	}
	if srv == nil || len(srv.components) != 1 {
		t.Fatalf("expected 1 registered component, got %v", srv)
	}
}
