package sdk

import (
	"github.com/planx-lab/planx-sdk-go/internal/runtime"
	"google.golang.org/grpc"
)

func ServeSource(factory func() SourceSPI) {
	s := runtime.NewSourceServer(func() runtime.SourceSPI {
		return factory()
	})
	runtime.ServeGRPC(func(server *grpc.Server) {
		runtime.RegisterSourceServer(server, s)
	})
}

func ServeSink(factory func() SinkSPI) {
	s := runtime.NewSinkServer(func() runtime.SinkSPI {
		return factory()
	})
	runtime.ServeGRPC(func(server *grpc.Server) {
		runtime.RegisterSinkServer(server, s)
	})
}

func ServeProcessor(factory func() ProcessorSPI) {
	s := runtime.NewProcessorServer(func() runtime.ProcessorSPI {
		return factory()
	})
	runtime.ServeGRPC(func(server *grpc.Server) {
		runtime.RegisterProcessorServer(server, s)
	})
}
