// Package source provides a gRPC server wrapper for source plugins.
// SDK manages the gRPC transport, session map, and flow control.
// Plugin user only implements the simple SourceSPI interface.
package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/planx-lab/planx-common/logger"
	planxv1 "github.com/planx-lab/planx-proto/gen/go/planx/v1"
	"github.com/planx-lab/planx-sdk-go/batch"
	"github.com/planx-lab/planx-sdk-go/session"
)

// SPI is the simple interface that source plugins must implement.
// SDK handles all gRPC, session management, and flow control complexity.
type SPI interface {
	// Init initializes the source with configuration.
	// Called once per session when CreateSession is received.
	Init(ctx context.Context, config []byte) error

	// ReadBatch reads the next batch of records.
	// SDK handles flow control - this will only be called when window allows.
	// Return io.EOF when no more data is available.
	ReadBatch(ctx context.Context) (batch.Batch, error)

	// Close releases resources.
	// Called when session is closed.
	Close() error
}

// Factory creates a new SPI instance for each session.
// This is the recommended way to ensure each session has isolated state.
type Factory func() SPI

// Server implements the gRPC SourcePlugin service.
// It wraps user-defined SPI logic with session management and flow control.
type Server struct {
	planxv1.UnimplementedSourcePluginServer
	factory   Factory
	sessions  *session.Manager
	instances sync.Map // sessionID -> SPI
}

// NewServer creates a new source server with the given factory.
func NewServer(factory Factory) *Server {
	return &Server{
		factory:  factory,
		sessions: session.NewManager(),
	}
}

// CreateSession implements SourcePlugin.CreateSession.
// SDK handles session creation, user's SPI.Init is called with config.
func (s *Server) CreateSession(ctx context.Context, req *planxv1.SessionCreateRequest) (*planxv1.SessionCreateResponse, error) {
	// Create SPI instance via factory
	spi := s.factory()

	// Initialize with config
	if err := spi.Init(ctx, req.ConfigJson); err != nil {
		return nil, fmt.Errorf("SPI init failed: %w", err)
	}

	// Create session in manager
	sess := s.sessions.Create(req.TenantId, req.ConfigJson)
	s.instances.Store(sess.ID, spi)

	logger.Info().
		Str("session_id", sess.ID).
		Str("tenant_id", req.TenantId).
		Msg("Source session created")

	return &planxv1.SessionCreateResponse{SessionId: sess.ID}, nil
}

// OpenStream implements SourcePlugin.OpenStream.
// SDK handles streaming loop and flow control, calls SPI.ReadBatch.
func (s *Server) OpenStream(req *planxv1.StreamOpenRequest, stream planxv1.SourcePlugin_OpenStreamServer) error {
	sess, err := s.sessions.Get(req.SessionId)
	if err != nil {
		return err
	}

	spiVal, ok := s.instances.Load(req.SessionId)
	if !ok {
		return fmt.Errorf("SPI instance not found for session %s", req.SessionId)
	}
	spi := spiVal.(SPI)

	sess.UpdateWindow(req.WindowSize)

	logger.Info().
		Str("session_id", req.SessionId).
		Int32("window_size", req.WindowSize).
		Msg("Source stream opened")

	for {
		select {
		case <-sess.Context().Done():
			return nil
		case <-stream.Context().Done():
			return nil
		default:
		}

		// Flow control: wait for window
		if !sess.ConsumeWindow() {
			continue // wait for ACK
		}

		// Call user's ReadBatch
		b, err := spi.ReadBatch(stream.Context())
		if err != nil {
			// io.EOF means end of data
			if err.Error() == "EOF" {
				return nil
			}
			logger.Error().Err(err).Str("session_id", sess.ID).Msg("ReadBatch error")
			continue
		}

		if len(b.Records) == 0 {
			continue
		}

		packed, err := batch.PackBatch(b)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to pack batch")
			continue
		}

		if err := stream.Send(&planxv1.BatchResponse{PackedBatch: packed}); err != nil {
			return err
		}

		logger.Debug().
			Str("session_id", sess.ID).
			Int("records", len(b.Records)).
			Msg("Sent batch")
	}
}

// Ack implements SourcePlugin.Ack.
// SDK handles window update automatically.
func (s *Server) Ack(ctx context.Context, req *planxv1.AckRequest) (*planxv1.AckResponse, error) {
	sess, err := s.sessions.Get(req.SessionId)
	if err != nil {
		return &planxv1.AckResponse{Success: false, Error: err.Error()}, nil
	}

	sess.UpdateWindow(req.NewWindow)

	logger.Debug().
		Str("session_id", req.SessionId).
		Int32("processed", req.Processed).
		Int32("new_window", req.NewWindow).
		Msg("ACK received")

	return &planxv1.AckResponse{Success: true}, nil
}

// CloseSession implements SourcePlugin.CloseSession.
// SDK handles cleanup, calls SPI.Close.
func (s *Server) CloseSession(ctx context.Context, req *planxv1.SessionCloseRequest) (*planxv1.Empty, error) {
	// Close SPI instance
	if spiVal, ok := s.instances.LoadAndDelete(req.SessionId); ok {
		spi := spiVal.(SPI)
		if err := spi.Close(); err != nil {
			logger.Warn().Err(err).Str("session_id", req.SessionId).Msg("SPI close error")
		}
	}

	// Close session
	if err := s.sessions.Close(req.SessionId); err != nil {
		logger.Warn().Err(err).Str("session_id", req.SessionId).Msg("Session close error")
	} else {
		logger.Info().Str("session_id", req.SessionId).Msg("Source session closed")
	}

	return &planxv1.Empty{}, nil
}
