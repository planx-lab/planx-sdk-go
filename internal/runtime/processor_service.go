package runtime

import (
	"context"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Process is a stateless per-batch transform. The session id travels in the
// "x-planx-session-id" gRPC metadata key.
func (s *PluginServer) Process(
	ctx context.Context,
	batchMsg *pb.Batch,
) (*pb.Batch, error) {

	sess, err := s.sessionFromMetadata(ctx)
	if err != nil {
		return nil, err
	}
	if sess.processor == nil {
		return nil, status.Error(codes.FailedPrecondition, "session is not a processor")
	}

	in, err := s.codec.Unpack(batchMsg.GetPayload())
	if err != nil {
		return nil, err
	}
	out, err := sess.processor.Process(in)
	if err != nil {
		return nil, err
	}
	packed, err := s.codec.Pack(out)
	if err != nil {
		return nil, err
	}
	return &pb.Batch{Payload: packed}, nil
}
