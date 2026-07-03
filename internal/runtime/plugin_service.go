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

// Health is the Engine-pulled readiness probe. Default: READY. (Plugins that
// need a richer probe will get a hook in a later plan — YAGNI for now.)
func (s *PluginServer) Health(context.Context, *pb.Empty) (*pb.HealthStatus, error) {
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
