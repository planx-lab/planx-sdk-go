// Package processor provides a gRPC server wrapper for processor plugins.
// SDK manages the gRPC transport and session map.
// Plugin user only implements the simple ProcessorSPI interface.
package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/planx-lab/planx-common/logger"
	planxv1 "github.com/planx-lab/planx-proto/gen/go/planx/v1"
	"github.com/planx-lab/planx-sdk-go/batch"
	"github.com/planx-lab/planx-sdk-go/session"
)

// SPI is the simple interface that processor plugins must implement.
type SPI interface {
	// Init initializes the processor with configuration.
	Init(ctx context.Context, config []byte) error

	// Process transforms a batch.
	// Input batch is passed in, return transformed batch.
	Process(ctx context.Context, input batch.Batch) (batch.Batch, error)

	// Close releases resources.
	Close() error
}

// Factory creates a new SPI instance for each session.
type Factory func() SPI

// Server implements the gRPC ProcessorPlugin service.
type Server struct {
	planxv1.UnimplementedProcessorPluginServer
	factory   Factory
	sessions  *session.Manager
	instances sync.Map
}

// NewServer creates a new processor server with the given factory.
func NewServer(factory Factory) *Server {
	return &Server{
		factory:  factory,
		sessions: session.NewManager(),
	}
}

// CreateSession implements ProcessorPlugin.CreateSession.
func (s *Server) CreateSession(ctx context.Context, req *planxv1.SessionCreateRequest) (*planxv1.SessionCreateResponse, error) {
	spi := s.factory()

	if err := spi.Init(ctx, req.ConfigJson); err != nil {
		return nil, fmt.Errorf("SPI init failed: %w", err)
	}

	sess := s.sessions.Create(req.TenantId, req.ConfigJson)
	s.instances.Store(sess.ID, spi)

	logger.Info().
		Str("session_id", sess.ID).
		Str("tenant_id", req.TenantId).
		Msg("Processor session created")

	return &planxv1.SessionCreateResponse{SessionId: sess.ID}, nil
}

// Process implements ProcessorPlugin.Process.
func (s *Server) Process(stream planxv1.ProcessorPlugin_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil // stream closed
		}

		spiVal, ok := s.instances.Load(req.SessionId)
		if !ok {
			continue
		}
		spi := spiVal.(SPI)

		// Unpack input batch
		input, err := batch.UnpackBatch(req.PackedBatch)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to unpack batch")
			continue
		}

		// Call user's Process
		output, err := spi.Process(stream.Context(), input)
		if err != nil {
			logger.Error().Err(err).Msg("Process error")
			continue
		}

		// Pack and send output
		packed, err := batch.PackBatch(output)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to pack batch")
			continue
		}

		if err := stream.Send(&planxv1.BatchResponse{PackedBatch: packed}); err != nil {
			return err
		}
	}
}

// Ack implements ProcessorPlugin.Ack.
func (s *Server) Ack(ctx context.Context, req *planxv1.AckRequest) (*planxv1.AckResponse, error) {
	return &planxv1.AckResponse{Success: true}, nil
}

// CloseSession implements ProcessorPlugin.CloseSession.
func (s *Server) CloseSession(ctx context.Context, req *planxv1.SessionCloseRequest) (*planxv1.Empty, error) {
	if spiVal, ok := s.instances.LoadAndDelete(req.SessionId); ok {
		spi := spiVal.(SPI)
		if err := spi.Close(); err != nil {
			logger.Warn().Err(err).Str("session_id", req.SessionId).Msg("SPI close error")
		}
	}

	s.sessions.Close(req.SessionId)
	logger.Info().Str("session_id", req.SessionId).Msg("Processor session closed")

	return &planxv1.Empty{}, nil
}
