package runtime

import (
	"context"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WriteBatch writes one batch to a SINK session. The session id travels in the
// "x-planx-session-id" gRPC metadata key. Returns a per-batch Ack.
func (s *PluginServer) WriteBatch(
	ctx context.Context,
	batchMsg *pb.Batch,
) (*pb.AckResponse, error) {

	sess, err := s.sessionFromMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if sess.sink == nil {
		return nil, status.Error(codes.FailedPrecondition, "session is not a sink")
	}

	b, err := s.codec.Unpack(batchMsg.GetPayload())
	if err != nil {
		return nil, err
	}
	if err := sess.sink.WriteBatch(b); err != nil {
		return nil, err
	}
	return &pb.AckResponse{}, nil
}
