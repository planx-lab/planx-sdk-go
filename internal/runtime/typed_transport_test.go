package runtime

import (
	"context"
	"io"
	"math"
	"net"
	"reflect"
	"testing"
	"time"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/observability"
	"github.com/planx-lab/planx-sdk-go/record"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type typedSource struct {
	batch record.Batch
	sent  bool
}

func (*typedSource) Init(context.Context, []byte) error { return nil }
func (s *typedSource) ReadBatch() (any, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return s.batch, nil
}
func (*typedSource) Close() error { return nil }

type typedProcessor struct{}

func (*typedProcessor) Init(context.Context, []byte) error { return nil }
func (*typedProcessor) Process(input any) (any, error)     { return input.(record.Batch).Clone(), nil }
func (*typedProcessor) Close() error                       { return nil }

type typedSink struct{ received chan record.Batch }

func (*typedSink) Init(context.Context, []byte) error { return nil }
func (s *typedSink) WriteBatch(input any) error       { s.received <- input.(record.Batch); return nil }
func (*typedSink) Close() error                       { return nil }

func typedClient(t *testing.T, registration ComponentRegistration) *grpc.ClientConn {
	t.Helper()
	srv, err := NewPluginServer(&pb.PluginDescriptor{Id: "typed-test", RuntimeVersion: "v4", Components: []*pb.ComponentDescriptor{registration.Descriptor}}, []ComponentRegistration{registration})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(observability.MetricsUnaryInterceptor, recoveryUnaryInterceptor),
		grpc.ChainStreamInterceptor(observability.MetricsStreamInterceptor, recoveryStreamInterceptor),
	)
	RegisterAll(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///typed-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestTypedTransportSourceProcessorSink(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "records", true: "empty"}[empty], func(t *testing.T) {
			d := decimal.RequireFromString("123456789012345678901234567890123456.78")
			original := record.Batch{
				Schema:  record.Schema{Fields: []record.Field{{Name: "d", Kind: record.Decimal, Precision: 38, Scale: 2}, {Name: "i", Kind: record.Int64}, {Name: "u", Kind: record.Uint64}, {Name: "n", Kind: record.String, Nullable: true}, {Name: "missing", Kind: record.String}}},
				Records: []record.Record{{"d": {Kind: record.Decimal, Present: true, Data: d}, "i": {Kind: record.Int64, Present: true, Data: int64(math.MinInt64)}, "u": {Kind: record.Uint64, Present: true, Data: uint64(math.MaxUint64)}, "n": record.NullValue(record.String), "missing": record.MissingValue(record.String)}},
			}
			if empty {
				original.Records = nil
			}
			sink := &typedSink{received: make(chan record.Batch, 1)}
			sourceConn := typedClient(t, ComponentRegistration{Descriptor: &pb.ComponentDescriptor{Id: "source", Kind: pb.ComponentKind_COMPONENT_KIND_SOURCE}, SourceFactory: func() SourceSPI { return &typedSource{batch: original} }})
			processorConn := typedClient(t, ComponentRegistration{Descriptor: &pb.ComponentDescriptor{Id: "processor", Kind: pb.ComponentKind_COMPONENT_KIND_PROCESSOR}, ProcessorFactory: func() ProcessorSPI { return &typedProcessor{} }})
			sinkConn := typedClient(t, ComponentRegistration{Descriptor: &pb.ComponentDescriptor{Id: "sink", Kind: pb.ComponentKind_COMPONENT_KIND_SINK}, SinkFactory: func() SinkSPI { return sink }})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			create := func(conn *grpc.ClientConn, component string) string {
				resp, err := pb.NewSessionServiceClient(conn).CreateSession(ctx, &pb.SessionCreateRequest{ComponentId: component})
				if err != nil {
					t.Fatal(err)
				}
				return resp.GetSessionId()
			}
			srcID, procID, sinkID := create(sourceConn, "source"), create(processorConn, "processor"), create(sinkConn, "sink")
			stream, err := pb.NewSourceServiceClient(sourceConn).OpenStream(ctx, &pb.StreamOpenRequest{SessionId: srcID, InitialWindow: 2})
			if err != nil {
				t.Fatal(err)
			}
			wire, err := stream.Recv()
			if err != nil {
				t.Fatalf("empty batch must not be EOF: %v", err)
			}
			wire.Lineage = []byte("runtime-owned-lineage")
			out, err := pb.NewProcessorServiceClient(processorConn).Process(metadata.AppendToOutgoingContext(ctx, "x-planx-session-id", procID), wire)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(out.Lineage, wire.Lineage) {
				t.Fatal("processor dropped wire lineage")
			}
			if _, err := pb.NewSinkServiceClient(sinkConn).WriteBatch(metadata.AppendToOutgoingContext(ctx, "x-planx-session-id", sinkID), out); err != nil {
				t.Fatal(err)
			}
			got := <-sink.received
			if !reflect.DeepEqual(original.Schema, got.Schema) || len(got.Records) != len(original.Records) {
				t.Fatal("schema or record count changed")
			}
			if !empty {
				if !got.Records[0]["d"].Data.(decimal.Decimal).Equal(d) || got.Records[0]["i"].Data != int64(math.MinInt64) || got.Records[0]["u"].Data != uint64(math.MaxUint64) {
					t.Fatal("exact numbers changed")
				}
				if !got.Records[0]["n"].Null || got.Records[0]["missing"].Present {
					t.Fatal("presence changed")
				}
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatalf("expected EOF after batch, got %v", err)
			}
			for conn, id := range map[*grpc.ClientConn]string{sourceConn: srcID, processorConn: procID, sinkConn: sinkID} {
				if _, err := pb.NewSessionServiceClient(conn).CloseSession(ctx, &pb.SessionCloseRequest{SessionId: id}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
