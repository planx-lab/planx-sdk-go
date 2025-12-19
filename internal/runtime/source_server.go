package runtime

import (
	"context"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/batch"
	"github.com/planx-lab/planx-sdk-go/internal/flow"
	"github.com/planx-lab/planx-sdk-go/internal/session"
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
		return nil
	}

	sess.window.Release(int(req.InitialWindow))

	for {
		sess.window.Acquire()

		b, err := sess.spi.ReadBatch()
		if err != nil {
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
	if ok {
		sess.window.Release(int(req.NewWindow))
	}

	return &pb.AckResponse{}, nil
}

func (s *SourceServer) CloseSession(
	ctx context.Context,
	req *pb.SessionCloseRequest,
) (*pb.Empty, error) {

	sess, ok := s.sessions.Get(req.SessionId)
	if ok {
		_ = sess.spi.Close()
		s.sessions.Remove(req.SessionId)
	}

	return &pb.Empty{}, nil
}
