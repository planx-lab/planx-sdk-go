package sdk

import (
	"context"
	"testing"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
)

type stubSource struct{ initErr error }

func (s *stubSource) Init(_ context.Context, _ []byte) error { return s.initErr }
func (s *stubSource) ReadBatch() (Batch, error)              { return nil, nil }
func (s *stubSource) Close() error                           { return nil }

func TestBuildRegistration_DescriptorAndComponents(t *testing.T) {
	p := Plugin{
		ID:          "hello",
		Version:     "1.2.3",
		DisplayName: "Hello",
		Summary:     "say hi",
		Components: []ComponentSpec{
			{
				ID:          "source",
				Kind:        KindSource,
				DisplayName: "Hello Source",
				Source:      func() SourceSPI { return &stubSource{} },
			},
		},
	}

	desc, comps := buildRegistration(p)
	if desc.GetId() != "hello" {
		t.Fatalf("id: %q", desc.GetId())
	}
	if desc.GetRuntimeVersion() != "v4" {
		t.Fatalf("runtime version: %q", desc.GetRuntimeVersion())
	}
	if desc.GetDocumentation().GetSummary() != "say hi" {
		t.Fatalf("summary: %q", desc.GetDocumentation().GetSummary())
	}
	if len(comps) != 1 {
		t.Fatalf("expected 1 comp reg, got %d", len(comps))
	}
	if comps[0].SourceFactory == nil {
		t.Fatal("expected source factory wired")
	}
	if len(desc.GetComponents()) != 1 || desc.GetComponents()[0].GetId() != "source" {
		t.Fatalf("descriptor components: %v", desc.GetComponents())
	}
	if desc.GetComponents()[0].GetKind() != pb.ComponentKind_COMPONENT_KIND_SOURCE {
		t.Fatalf("kind: %v", desc.GetComponents()[0].GetKind())
	}
}

func TestSchema_BuilderAssemblesFields(t *testing.T) {
	s := Schema(
		StringField("host", Required(), WithDescription("upstream host"), WithDefault(StringValue("localhost"))),
		IntegerField("port", WithDefault(IntValue(5432))),
		SecretField("api_key", Required()),
		EnumField("mode", []string{"fast", "safe"}, WithDefault(StringValue("safe"))),
		BooleanField("tls", WithDefault(BoolValue(false))),
	)
	if len(s.GetFields()) != 5 {
		t.Fatalf("expected 5 fields, got %d", len(s.GetFields()))
	}
	host := s.GetFields()[0]
	if host.GetName() != "host" || host.GetType() != pb.FieldType_FIELD_TYPE_STRING {
		t.Fatalf("host field: %+v", host)
	}
	if !host.GetRequired() {
		t.Fatal("host should be required")
	}
	if host.GetDefault().GetStringValue() != "localhost" {
		t.Fatalf("host default: %v", host.GetDefault())
	}
	key := s.GetFields()[2]
	if key.GetType() != pb.FieldType_FIELD_TYPE_SECRET {
		t.Fatalf("api_key type: %v", key.GetType())
	}
	mode := s.GetFields()[3]
	if mode.GetType() != pb.FieldType_FIELD_TYPE_ENUM || len(mode.GetEnumValues()) != 2 {
		t.Fatalf("mode field: %+v", mode)
	}
}
