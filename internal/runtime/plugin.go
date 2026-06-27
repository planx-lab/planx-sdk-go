package runtime

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/batch"
	"github.com/planx-lab/planx-sdk-go/internal/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ComponentRegistration binds a ComponentDescriptor to its SPI factory and an
// optional config-validation hook. The sdk package builds these from a Plugin
// declaration; exactly one factory is non-nil, matching Descriptor.Kind.
type ComponentRegistration struct {
	Descriptor       *pb.ComponentDescriptor
	SourceFactory    func() SourceSPI
	ProcessorFactory func() ProcessorSPI
	SinkFactory      func() SinkSPI
	// Validate is the optional live config check (Designer connectivity).
	// nil => ValidateConfig responds ok=true (schema-only validation).
	Validate func(ctx context.Context, config []byte) (bool, string)
}

// PluginServer implements all five protocol services for one plugin binary.
// It owns the declaration-time component registry and the runtime session
// manager. One *PluginServer is registered for all five gRPC services.
type PluginServer struct {
	pb.UnimplementedPluginServiceServer
	pb.UnimplementedSessionServiceServer
	pb.UnimplementedSourceServiceServer
	pb.UnimplementedProcessorServiceServer
	pb.UnimplementedSinkServiceServer

	descriptor *pb.PluginDescriptor
	components map[string]*ComponentRegistration
	sessions   *session.Manager[*liveSession]
	codec      batch.Codec
}

// NewPluginServer validates and assembles a PluginServer. It requires a
// non-nil descriptor and at least one component with unique, non-empty ids.
func NewPluginServer(desc *pb.PluginDescriptor, comps []ComponentRegistration) (*PluginServer, error) {
	if desc == nil {
		return nil, errors.New("plugin descriptor is required")
	}
	if len(comps) == 0 {
		return nil, errors.New("at least one component is required")
	}
	m := make(map[string]*ComponentRegistration, len(comps))
	for i := range comps {
		c := &comps[i]
		id := c.Descriptor.GetId()
		if id == "" {
			return nil, fmt.Errorf("component %d: missing id", i)
		}
		if _, dup := m[id]; dup {
			return nil, fmt.Errorf("duplicate component id %q", id)
		}
		m[id] = c
	}
	return &PluginServer{
		descriptor: desc,
		components: m,
		sessions:   session.NewManager[*liveSession](),
		codec:      batch.NewCodec(),
	}, nil
}

// sessionFromMetadata resolves the session id carried in the
// "x-planx-session-id" gRPC metadata key (Processor/Sink dispatch).
func (s *PluginServer) sessionFromMetadata(ctx context.Context) (*liveSession, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing metadata")
	}
	ids := md.Get("x-planx-session-id")
	if len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing session id in metadata")
	}
	sess, ok := s.sessions.Get(ids[0])
	if !ok {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	return sess, nil
}
