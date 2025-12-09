package sdk

import (
	"context"
	"os"

	"github.com/planx-lab/planx-common/logger"
	"github.com/planx-lab/planx-common/otel"
	planxv1 "github.com/planx-lab/planx-proto/gen/go/planx/v1"
	"github.com/planx-lab/planx-sdk-go/server"
	"github.com/planx-lab/planx-sdk-go/sink"
	"github.com/planx-lab/planx-sdk-go/source"
)

// ServeSource starts a source plugin.
func ServeSource(factory source.Factory) {
	serve("source", func(ctx context.Context, srv *server.Server) {
		sourceSrv := source.NewServer(factory)
		planxv1.RegisterSourcePluginServer(srv.GRPCServer(), sourceSrv)
	})
}

// ServeSink starts a sink plugin.
func ServeSink(factory sink.Factory) {
	serve("sink", func(ctx context.Context, srv *server.Server) {
		sinkSrv := sink.NewServer(factory)
		planxv1.RegisterSinkPluginServer(srv.GRPCServer(), sinkSrv)
	})
}

func serve(role string, registerFunc func(context.Context, *server.Server)) {
	// Initialize Logger
	logger.Init(logger.DefaultConfig())

	// Initialize Tracer
	otelCfg := otel.TracingConfig{
		ServiceName: "planx-plugin-" + role,
	}
	if ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); ep != "" {
		otelCfg.Endpoint = ep
	}
	if err := otel.InitTracing(context.Background(), otelCfg); err != nil {
		logger.Warn().Err(err).Msg("Failed to initialize tracing")
	}

	logger.Info().Str("role", role).Msg("Starting plugin")

	// Create and start server
	cfg := server.DefaultConfig()
	cfg.PluginName = "plugin-" + role
	cfg.PluginType = server.PluginType(role)

	// Check for address override
	if addr := os.Getenv("PLANX_PLUGIN_ADDR"); addr != "" {
		cfg.Address = addr
	}
	// And flag? simple main usually doesn't parse flags for us if we hide it here.
	// Spec pattern usually passes config via gRPC CreateSession.
	// Bootstrapping address is key.

	srv := server.New(cfg)
	registerFunc(context.Background(), srv)

	if err := srv.RunWithSignals(); err != nil {
		logger.Error().Err(err).Msg("Plugin failed")
		os.Exit(1)
	}
}
