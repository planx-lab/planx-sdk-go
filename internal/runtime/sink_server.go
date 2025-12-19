package runtime

import (
	"context"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/batch"
	"github.com/planx-lab/planx-sdk-go/internal/session"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type SinkSPI interface {
	Init(ctx context.Context, config []byte) error
	WriteBatch(batch any) error
	Close() error
}

type SinkServer struct {
	pb.UnimplementedSinkPluginServer

	factory  func() SinkSPI
	sessions *session.Manager[*sinkSession]
	codec    batch.Codec
}

type sinkSession struct {
	spi SinkSPI
}

func NewSinkServer(factory func() SinkSPI) *SinkServer {
	return &SinkServer{
		factory:  factory,
		sessions: session.NewManager[*sinkSession](),
		codec:    batch.NewCodec(),
	}
}

func (s *SinkServer) CreateSession(
	ctx context.Context,
	req *pb.SessionCreateRequest,
) (*pb.SessionCreateResponse, error) {

	spi := s.factory()
	if err := spi.Init(ctx, req.Config); err != nil {
		return nil, err
	}

	id := generateSessionID()
	s.sessions.Add(id, &sinkSession{spi: spi})

	return &pb.SessionCreateResponse{
		SessionId: id,
	}, nil
}

func (s *SinkServer) WriteBatch(
	ctx context.Context,
	batchMsg *pb.Batch,
) (*pb.AckResponse, error) {

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

	b, err := s.codec.Unpack(batchMsg.Payload)
	if err != nil {
		return nil, err
	}

	if err := sess.spi.WriteBatch(b); err != nil {
		return nil, err
	}

	return &pb.AckResponse{}, nil
}

func (s *SinkServer) CloseSession(
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
