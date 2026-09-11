package runtime

import (
	"context"
	"runtime/debug"

	"github.com/planx-lab/planx-sdk-go/internal/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recoveryUnaryInterceptor recovers from panics in unary gRPC handlers and
// converts them into gRPC INTERNAL errors so the server process stays alive.
func recoveryUnaryInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			observability.Logger().Error("planx-sdk panic recovered in unary RPC",
				"method", info.FullMethod,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			err = status.Errorf(codes.Internal, "plugin panic recovered")
		}
	}()
	return handler(ctx, req)
}

// recoveryStreamInterceptor recovers from panics in streaming gRPC handlers
// and converts them into gRPC INTERNAL errors so the server process stays alive.
func recoveryStreamInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			observability.Logger().Error("planx-sdk panic recovered in stream RPC",
				"method", info.FullMethod,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			err = status.Errorf(codes.Internal, "plugin panic recovered")
		}
	}()
	return handler(srv, ss)
}
