package sdk

import (
	"context"

	"github.com/planx-lab/planx-sdk-go/internal/runtime"
	"google.golang.org/grpc"
)

func ServeSource(factory func() SourceSPI) {
	s := runtime.NewSourceServer(func() runtime.SourceSPI {
		return &sourceWrapper{spi: factory()}
	})
	runtime.ServeGRPC(func(server *grpc.Server) {
		runtime.RegisterSourceServer(server, s)
	})
}

func ServeSink(factory func() SinkSPI) {
	s := runtime.NewSinkServer(func() runtime.SinkSPI {
		return &sinkWrapper{spi: factory()}
	})
	runtime.ServeGRPC(func(server *grpc.Server) {
		runtime.RegisterSinkServer(server, s)
	})
}

func ServeProcessor(factory func() ProcessorSPI) {
	s := runtime.NewProcessorServer(func() runtime.ProcessorSPI {
		return &processorWrapper{spi: factory()}
	})
	runtime.ServeGRPC(func(server *grpc.Server) {
		runtime.RegisterProcessorServer(server, s)
	})
}

type sourceWrapper struct {
	spi SourceSPI
}

func (w *sourceWrapper) Init(ctx context.Context, config []byte) error {
	return w.spi.Init(ctx, config)
}
func (w *sourceWrapper) ReadBatch() (any, error) { return w.spi.ReadBatch() }
func (w *sourceWrapper) Close() error            { return w.spi.Close() }

type sinkWrapper struct {
	spi SinkSPI
}

func (w *sinkWrapper) Init(ctx context.Context, config []byte) error { return w.spi.Init(ctx, config) }
func (w *sinkWrapper) WriteBatch(batch any) error                    { return w.spi.WriteBatch(batch) }
func (w *sinkWrapper) Close() error                                  { return w.spi.Close() }

type processorWrapper struct {
	spi ProcessorSPI
}

func (w *processorWrapper) Init(ctx context.Context, config []byte) error {
	return w.spi.Init(ctx, config)
}
func (w *processorWrapper) Process(batch any) (any, error) { return w.spi.Process(batch) }
func (w *processorWrapper) Close() error                   { return w.spi.Close() }
