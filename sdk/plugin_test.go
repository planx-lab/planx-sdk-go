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

type stubProcessor struct{}

func (p *stubProcessor) Init(_ context.Context, _ []byte) error { return nil }
func (p *stubProcessor) Process(_ Batch) (Batch, error)         { return nil, nil }
func (p *stubProcessor) Close() error                           { return nil }

type stubSink struct{}

func (s *stubSink) Init(_ context.Context, _ []byte) error { return nil }
func (s *stubSink) WriteBatch(_ Batch) error                    { return nil }
func (s *stubSink) Close() error                           { return nil }

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

func TestBuildRegistration_MultiComponent_AllWired(t *testing.T) {
	validate := func(_ context.Context, _ []byte) (bool, string) { return true, "" }
	p := Plugin{
		ID:      "multi",
		Version: "0.1.0",
		Components: []ComponentSpec{
			{
				ID:          "src",
				Kind:        KindSource,
				DisplayName: "My Source",
				Source:      func() SourceSPI { return &stubSource{} },
				Validate:    validate,
			},
			{
				ID:          "proc",
				Kind:        KindProcessor,
				DisplayName: "My Processor",
				Processor:   func() ProcessorSPI { return &stubProcessor{} },
			},
			{
				ID:          "snk",
				Kind:        KindSink,
				DisplayName: "My Sink",
				Sink:        func() SinkSPI { return &stubSink{} },
			},
		},
	}

	desc, comps := buildRegistration(p)
	if len(comps) != 3 {
		t.Fatalf("expected 3 comp regs, got %d", len(comps))
	}
	// Source
	if comps[0].SourceFactory == nil || comps[0].ProcessorFactory != nil || comps[0].SinkFactory != nil {
		t.Fatal("source: expected only SourceFactory wired")
	}
	if comps[0].Validate == nil {
		t.Fatal("source: expected Validate hook")
	}
	// Processor
	if comps[1].ProcessorFactory == nil || comps[1].SourceFactory != nil || comps[1].SinkFactory != nil {
		t.Fatal("processor: expected only ProcessorFactory wired")
	}
	// Sink
	if comps[2].SinkFactory == nil || comps[2].SourceFactory != nil || comps[2].ProcessorFactory != nil {
		t.Fatal("sink: expected only SinkFactory wired")
	}
	// Descriptor
	descComps := desc.GetComponents()
	if len(descComps) != 3 {
		t.Fatalf("expected 3 descriptor components, got %d", len(descComps))
	}
	if descComps[0].GetKind() != pb.ComponentKind_COMPONENT_KIND_SOURCE {
		t.Fatalf("desc[0] kind: %v", descComps[0].GetKind())
	}
	if descComps[1].GetKind() != pb.ComponentKind_COMPONENT_KIND_PROCESSOR {
		t.Fatalf("desc[1] kind: %v", descComps[1].GetKind())
	}
	if descComps[2].GetKind() != pb.ComponentKind_COMPONENT_KIND_SINK {
		t.Fatalf("desc[2] kind: %v", descComps[2].GetKind())
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
