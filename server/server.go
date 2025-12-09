// Package server provides gRPC server wrapper for Planx plugins.
// This package bootstraps plugin processes and handles registration.
package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/planx-lab/planx-common/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// PluginType represents the type of plugin.
type PluginType string

const (
	PluginTypeSource    PluginType = "source"
	PluginTypeProcessor PluginType = "processor"
	PluginTypeSink      PluginType = "sink"
)

// Config holds server configuration.
type Config struct {
	// Address to listen on (e.g., ":50051" or "unix:///tmp/plugin.sock")
	Address string

	// PluginName is the name of this plugin (e.g., "mysql", "http")
	PluginName string

	// PluginType is the type of this plugin
	PluginType PluginType

	// MaxConcurrentStreams limits concurrent streams per connection
	MaxConcurrentStreams uint32

	// EnableReflection enables gRPC reflection for debugging
	EnableReflection bool
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		Address:              ":50051",
		MaxConcurrentStreams: 100,
		EnableReflection:     true,
	}
}

// Server wraps a gRPC server for plugin bootstrap.
type Server struct {
	config     Config
	grpcServer *grpc.Server
	listener   net.Listener
}

// New creates a new plugin server.
func New(cfg Config) *Server {
	opts := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams),
	}

	grpcServer := grpc.NewServer(opts...)

	// Register health check
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus(cfg.PluginName, grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection for debugging
	if cfg.EnableReflection {
		reflection.Register(grpcServer)
	}

	return &Server{
		config:     cfg,
		grpcServer: grpcServer,
	}
}

// GRPCServer returns the underlying gRPC server for service registration.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Run starts the server and blocks until shutdown.
func (s *Server) Run(ctx context.Context) error {
	var err error
	s.listener, err = net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	logger.Info().
		Str("plugin", s.config.PluginName).
		Str("type", string(s.config.PluginType)).
		Str("address", s.config.Address).
		Msg("Plugin server starting")

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		logger.Info().Msg("Shutting down plugin server")
		s.grpcServer.GracefulStop()
	}()

	if err := s.grpcServer.Serve(s.listener); err != nil {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

// RunWithSignals runs the server and handles OS signals for graceful shutdown.
func (s *Server) RunWithSignals() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	return s.Run(ctx)
}

// Stop gracefully stops the server.
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
