// Package sink provides a gRPC server wrapper for sink plugins.
// SDK manages the gRPC transport and session map.
// Plugin user only implements the simple SinkSPI interface.
package sink

import (
	"context"
	"fmt"
	"sync"

	"github.com/planx-lab/planx-common/logger"
	planxv1 "github.com/planx-lab/planx-proto/gen/go/planx/v1"
	"github.com/planx-lab/planx-sdk-go/batch"
	"github.com/planx-lab/planx-sdk-go/session"
)

// SPI is the simple interface that sink plugins must implement.
type SPI interface {
	// Init initializes the sink with configuration.
	Init(ctx context.Context, config []byte) error

	// Write writes a batch to the destination.
	// Return number of records successfully written.
	Write(ctx context.Context, b batch.Batch) (int, error)

	// Close releases resources.
	Close() error
}

// Factory creates a new SPI instance for each session.
type Factory func() SPI

// Server implements the gRPC SinkPlugin service.
type Server struct {
	planxv1.UnimplementedSinkPluginServer
	factory   Factory
	sessions  *session.Manager
	instances sync.Map
}

// NewServer creates a new sink server with the given factory.
func NewServer(factory Factory) *Server {
	return &Server{
		factory:  factory,
		sessions: session.NewManager(),
	}
}

// CreateSession implements SinkPlugin.CreateSession.
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
		Msg("Sink session created")

	return &planxv1.SessionCreateResponse{SessionId: sess.ID}, nil
}

// Write implements SinkPlugin.Write.
func (s *Server) Write(stream planxv1.SinkPlugin_WriteServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}

		spiVal, ok := s.instances.Load(req.SessionId)
		if !ok {
			continue
		}
		spi := spiVal.(SPI)

		// Unpack batch
		b, err := batch.UnpackBatch(req.PackedBatch)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to unpack batch")
			continue
		}

		// Call user's Write
		_, err = spi.Write(stream.Context(), b)
		if err != nil {
			logger.Error().Err(err).Msg("Write error")
			stream.Send(&planxv1.AckResponse{Success: false, Error: err.Error()})
			continue
		}

		if err := stream.Send(&planxv1.AckResponse{
			Success: true,
		}); err != nil {
			return err
		}
	}
}

// CloseSession implements SinkPlugin.CloseSession.
func (s *Server) CloseSession(ctx context.Context, req *planxv1.SessionCloseRequest) (*planxv1.Empty, error) {
	if spiVal, ok := s.instances.LoadAndDelete(req.SessionId); ok {
		spi := spiVal.(SPI)
		if err := spi.Close(); err != nil {
			logger.Warn().Err(err).Str("session_id", req.SessionId).Msg("SPI close error")
		}
	}

	s.sessions.Close(req.SessionId)
	logger.Info().Str("session_id", req.SessionId).Msg("Sink session closed")

	return &planxv1.Empty{}, nil
}
