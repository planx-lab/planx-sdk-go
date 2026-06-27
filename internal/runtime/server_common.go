package runtime

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/util"
	"google.golang.org/grpc"
)

// Handshake is the JSON the plugin prints to STDOUT (and writes to
// planx.handshake) so the Engine can discover the listening address.
type Handshake struct {
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
}

// ServeGRPC listens on an ephemeral loopback port, hands the *grpc.Server to
// register, emits the handshake, then blocks serving.
func ServeGRPC(register func(*grpc.Server)) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recoveryUnaryInterceptor),
		grpc.ChainStreamInterceptor(recoveryStreamInterceptor),
	)
	register(grpcServer)

	hs := Handshake{
		Protocol: "v4",
		Address:  lis.Addr().String(),
	}

	data, err := json.Marshal(hs)
	if err != nil {
		panic(err)
	}

	if err := os.WriteFile("planx.handshake", data, 0600); err != nil {
		panic(err)
	}

	// NOTE: The first line written to STDOUT is reserved
	// exclusively for the Planx handshake JSON.
	fmt.Printf("%s\n", data)

	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

// RegisterAll registers all five protocol services for one PluginServer.
func RegisterAll(s *grpc.Server, srv *PluginServer) {
	pb.RegisterPluginServiceServer(s, srv)
	pb.RegisterSessionServiceServer(s, srv)
	pb.RegisterSourceServiceServer(s, srv)
	pb.RegisterProcessorServiceServer(s, srv)
	pb.RegisterSinkServiceServer(s, srv)
}

func generateSessionID() string {
	return util.NewSessionID()
}
