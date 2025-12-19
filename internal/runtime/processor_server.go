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

type ProcessorSPI interface {
	Init(ctx context.Context, config []byte) error
	Process(batch any) (any, error)
	Close() error
}

type ProcessorServer struct {
	pb.UnimplementedProcessorPluginServer

	factory  func() ProcessorSPI
	sessions *session.Manager[*processorSession]
	codec    batch.Codec
}

type processorSession struct {
	spi ProcessorSPI
}

func NewProcessorServer(factory func() ProcessorSPI) *ProcessorServer {
	return &ProcessorServer{
		factory:  factory,
		sessions: session.NewManager[*processorSession](),
		codec:    batch.NewCodec(),
	}
}

func (p *ProcessorServer) CreateSession(
	ctx context.Context,
	req *pb.SessionCreateRequest,
) (*pb.SessionCreateResponse, error) {

	spi := p.factory()
	if err := spi.Init(ctx, req.Config); err != nil {
		return nil, err
	}

	id := generateSessionID()
	p.sessions.Add(id, &processorSession{spi: spi})

	return &pb.SessionCreateResponse{
		SessionId: id,
	}, nil
}

func (p *ProcessorServer) Process(
	ctx context.Context,
	batchMsg *pb.Batch,
) (*pb.Batch, error) {

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing metadata")
	}

	ids := md.Get("x-planx-session-id")
	if len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing session id in metadata")
	}

	sess, ok := p.sessions.Get(ids[0])
	if !ok {
		return nil, status.Error(codes.NotFound, "session not found")
	}

	in, err := p.codec.Unpack(batchMsg.Payload)
	if err != nil {
		return nil, err
	}

	out, err := sess.spi.Process(in)
	if err != nil {
		return nil, err
	}

	packed, err := p.codec.Pack(out)
	if err != nil {
		return nil, err
	}

	return &pb.Batch{Payload: packed}, nil
}

func (p *ProcessorServer) CloseSession(
	ctx context.Context,
	req *pb.SessionCloseRequest,
) (*pb.Empty, error) {

	sess, ok := p.sessions.Get(req.SessionId)
	if ok {
		_ = sess.spi.Close()
		p.sessions.Remove(req.SessionId)
	}

	return &pb.Empty{}, nil
}
