package runtime

import (
	"context"
	"fmt"
	"os"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/flow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateSession instantiates the SPI for the named component, applies config,
// and stores a kind-tagged liveSession under a fresh session id.
func (s *PluginServer) CreateSession(
	ctx context.Context,
	req *pb.SessionCreateRequest,
) (*pb.SessionCreateResponse, error) {

	comp, ok := s.components[req.GetComponentId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "unknown component %q", req.GetComponentId())
	}

	sess := &liveSession{}
	switch comp.Descriptor.GetKind() {
	case pb.ComponentKind_COMPONENT_KIND_SOURCE:
		if comp.SourceFactory == nil {
			return nil, status.Error(codes.FailedPrecondition, "source factory not configured")
		}
		src := comp.SourceFactory()
		if err := src.Init(ctx, req.GetConfig()); err != nil {
			return nil, err
		}
		sess.source = src
		sess.window = flow.NewWindow(0)
	case pb.ComponentKind_COMPONENT_KIND_PROCESSOR:
		if comp.ProcessorFactory == nil {
			return nil, status.Error(codes.FailedPrecondition, "processor factory not configured")
		}
		proc := comp.ProcessorFactory()
		if err := proc.Init(ctx, req.GetConfig()); err != nil {
			return nil, err
		}
		sess.processor = proc
	case pb.ComponentKind_COMPONENT_KIND_SINK:
		if comp.SinkFactory == nil {
			return nil, status.Error(codes.FailedPrecondition, "sink factory not configured")
		}
		sink := comp.SinkFactory()
		if err := sink.Init(ctx, req.GetConfig()); err != nil {
			return nil, err
		}
		sess.sink = sink
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unsupported component kind %v", comp.Descriptor.GetKind())
	}

	id := generateSessionID()
	s.sessions.Add(id, sess)
	return &pb.SessionCreateResponse{SessionId: id}, nil
}

// CloseSession tears down the SPI for the session (no-op if unknown).
func (s *PluginServer) CloseSession(
	_ context.Context,
	req *pb.SessionCloseRequest,
) (*pb.SessionCloseResponse, error) {

	sess, ok := s.sessions.DeleteAndGet(req.GetSessionId())
	if ok {
		if err := sess.close(); err != nil {
			fmt.Fprintf(os.Stderr, "[planx-sdk] spi.Close error: %v\n", err)
		}
	}
	return &pb.SessionCloseResponse{}, nil
}

// Ack is session-scoped flow control. Only SOURCE sessions carry a window;
// for other sessions it is a no-op (the protocol reserves Ack for future
// windowed processors/sinks).
func (s *PluginServer) Ack(
	_ context.Context,
	req *pb.AckRequest,
) (*pb.AckResponse, error) {

	sess, ok := s.sessions.Get(req.GetSessionId())
	if !ok {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	if sess.window != nil {
		sess.window.Release(int(req.GetNewWindow()))
	}
	return &pb.AckResponse{}, nil
}
