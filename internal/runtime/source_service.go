package runtime

import (
	"io"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OpenStream drives a server-streaming read for an already-created SOURCE
// session. A clean stream end (return nil) is the exhaustion signal: gRPC
// closes the stream so the engine's Recv() gets a real io.EOF (CodeEOF).
func (s *PluginServer) OpenStream(
	req *pb.StreamOpenRequest,
	stream pb.SourceService_OpenStreamServer,
) error {

	sess, ok := s.sessions.Get(req.GetSessionId())
	if !ok {
		return status.Error(codes.NotFound, "session not found")
	}
	if sess.source == nil {
		return status.Error(codes.FailedPrecondition, "session is not a source")
	}

	sess.window.Release(int(req.GetInitialWindow()))

	for {
		if err := sess.window.AcquireContext(stream.Context()); err != nil {
			return err
		}

		b, err := sess.source.ReadBatch()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		packed, err := s.codec.Pack(b)
		if err != nil {
			return err
		}
		if err := stream.Send(&pb.Batch{Payload: packed}); err != nil {
			return err
		}
	}
}
