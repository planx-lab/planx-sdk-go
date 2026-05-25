package runtime

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryUnaryInterceptor_NoPanic(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	resp, err := recoveryUnaryInterceptor(
		context.Background(),
		"req",
		&grpc.UnaryServerInfo{FullMethod: "/test/Method"},
		handler,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("got %v, want ok", resp)
	}
}

func TestRecoveryUnaryInterceptor_Panic(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		panic("boom")
	}

	resp, err := recoveryUnaryInterceptor(
		context.Background(),
		"req",
		&grpc.UnaryServerInfo{FullMethod: "/test/Method"},
		handler,
	)
	if resp != nil {
		t.Fatal("expected nil response on panic")
	}
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if s.Code() != codes.Internal {
		t.Fatalf("expected INTERNAL, got %s", s.Code())
	}
}

type mockServerStream struct {
	grpc.ServerStream
}

func TestRecoveryStreamInterceptor_NoPanic(t *testing.T) {
	handler := func(srv any, ss grpc.ServerStream) error {
		return nil
	}

	err := recoveryStreamInterceptor(
		"srv",
		&mockServerStream{},
		&grpc.StreamServerInfo{FullMethod: "/test/Stream"},
		handler,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRecoveryStreamInterceptor_Panic(t *testing.T) {
	handler := func(srv any, ss grpc.ServerStream) error {
		panic("stream boom")
	}

	err := recoveryStreamInterceptor(
		"srv",
		&mockServerStream{},
		&grpc.StreamServerInfo{FullMethod: "/test/Stream"},
		handler,
	)
	s, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if s.Code() != codes.Internal {
		t.Fatalf("expected INTERNAL, got %s", s.Code())
	}
}

func TestGenerateSessionID(t *testing.T) {
	id := generateSessionID()
	if id == "" {
		t.Fatal("session ID should not be empty")
	}
	if len(id) != 36 {
		t.Fatalf("UUID length: got %d, want 36", len(id))
	}
}
