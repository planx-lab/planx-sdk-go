package runtime

import (
	"context"
	"fmt"
	"io"
	"os"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/batch"
	"github.com/planx-lab/planx-sdk-go/internal/flow"
	"github.com/planx-lab/planx-sdk-go/internal/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SourceSPI interface {
	Init(ctx context.Context, config []byte) error
	ReadBatch() (any, error)
	Close() error
}

type SourceServer struct {
	pb.UnimplementedSourcePluginServer

	factory  func() SourceSPI
	sessions *session.Manager[*sourceSession]
	codec    batch.Codec
}

type sourceSession struct {
	spi    SourceSPI
	window *flow.Window
}

func NewSourceServer(factory func() SourceSPI) *SourceServer {
	return &SourceServer{
		factory:  factory,
		sessions: session.NewManager[*sourceSession](),
		codec:    batch.NewCodec(),
	}
}

func (s *SourceServer) CreateSession(
	ctx context.Context,
	req *pb.SessionCreateRequest,
) (*pb.SessionCreateResponse, error) {

	spi := s.factory()
	if err := spi.Init(ctx, req.Config); err != nil {
		return nil, err
	}

	id := generateSessionID()

	s.sessions.Add(id, &sourceSession{
		spi:    spi,
		window: flow.NewWindow(0),
	})

	return &pb.SessionCreateResponse{
		SessionId: id,
	}, nil
}

func (s *SourceServer) OpenStream(
	req *pb.StreamOpenRequest,
	stream pb.SourcePlugin_OpenStreamServer,
) error {

	sess, ok := s.sessions.Get(req.SessionId)
	if !ok {
		return status.Error(codes.NotFound, "session not found")
	}

	sess.window.Release(int(req.InitialWindow))

	for {
		if err := sess.window.AcquireContext(stream.Context()); err != nil {
			return err
		}

		b, err := sess.spi.ReadBatch()
		if err != nil {
			// io.EOF means the source is exhausted — close the stream cleanly so
			// the engine's stream.Recv() gets a real io.EOF (classified as
			// CodeEOF -> pipeline SUCCEEDED). Returning io.EOF directly makes
			// gRPC wrap it as code=Unknown, which the engine misreads as a
			// plugin failure.
			if err == io.EOF {
				return nil
			}
			return err
		}

		packed, err := s.codec.Pack(b)
		if err != nil {
			return err
		}

		if err := stream.Send(&pb.Batch{
			Payload: packed,
		}); err != nil {
			return err
		}
	}
}

func (s *SourceServer) Ack(
	ctx context.Context,
	req *pb.AckRequest,
) (*pb.AckResponse, error) {

	sess, ok := s.sessions.Get(req.SessionId)
	if !ok {
		return nil, status.Error(codes.NotFound, "session not found")
	}

	sess.window.Release(int(req.NewWindow))

	return &pb.AckResponse{}, nil
}

func (s *SourceServer) CloseSession(
	ctx context.Context,
	req *pb.SessionCloseRequest,
) (*pb.Empty, error) {

	sess, ok := s.sessions.DeleteAndGet(req.SessionId)
	if ok {
		if err := sess.spi.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "[planx-sdk] source spi.Close error: %v\n", err)
		}
	}

	return &pb.Empty{}, nil
}
