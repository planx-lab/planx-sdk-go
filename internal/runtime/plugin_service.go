package runtime

import (
	"context"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Discover returns the plugin's self-description (the declaration-time
// descriptor). Stateless; cached by the Engine.
func (s *PluginServer) Discover(context.Context, *pb.Empty) (*pb.PluginDescriptor, error) {
	return s.descriptor, nil
}

// Health is the Engine-pulled readiness probe. It aggregates every component's
// optional Health hook: if any registered hook reports unhealthy, the plugin is
// NOT_READY. Components without a hook are considered healthy. This is a REAL
// probe — a plugin whose dependency (e.g. its DB) is down reports NOT_READY so
// the Engine can stop routing traffic to it, rather than always claiming READY.
func (s *PluginServer) Health(ctx context.Context, _ *pb.Empty) (*pb.HealthStatus, error) {
	for _, comp := range s.components {
		if comp.Health == nil {
			continue
		}
		ok, msg := comp.Health(ctx)
		if !ok {
			return &pb.HealthStatus{
				State:   pb.HealthStatus_STATE_NOT_READY,
				Message: comp.Descriptor.GetId() + ": " + msg,
			}, nil
		}
	}
	return &pb.HealthStatus{State: pb.HealthStatus_STATE_READY}, nil
}

// ValidateConfig forwards to the component's optional Validate hook. With no
// hook it responds ok=true (schema-only validation) so every component answers.
func (s *PluginServer) ValidateConfig(
	ctx context.Context,
	req *pb.ConfigValidationRequest,
) (*pb.ConfigValidationResult, error) {

	comp, ok := s.components[req.GetComponentId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown component %q", req.GetComponentId())
	}
	if comp.Validate == nil {
		return &pb.ConfigValidationResult{Ok: true, Message: "schema-only validation"}, nil
	}
	ok2, msg := comp.Validate(ctx, req.GetConfig())
	return &pb.ConfigValidationResult{Ok: ok2, Message: msg}, nil
}

// DiscoverSchema forwards to the component's optional Discover hook (DB
// sources). With no hook it returns an empty response so non-DB sources
// answer without error; the Designer hides the discovery UI in that case.
// The hook returns *pb.DiscoverSchemaResponse directly; the sdk package
// performs the *SchemaDiscovery->proto conversion in buildRegistration.
func (s *PluginServer) DiscoverSchema(
	ctx context.Context,
	req *pb.DiscoverSchemaRequest,
) (*pb.DiscoverSchemaResponse, error) {
	comp, ok := s.components[req.GetComponentId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown component %q", req.GetComponentId())
	}
	if comp.Discover == nil {
		return &pb.DiscoverSchemaResponse{}, nil
	}
	return comp.Discover(ctx, req.GetConfig())
}
