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

type Handshake struct {
	Protocol string `json:"protocol"`
	Address  string `json:"address"`
}

func ServeGRPC(register func(*grpc.Server)) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer()
	register(grpcServer)

	hs := Handshake{
		Protocol: "v4",
		Address:  lis.Addr().String(),
	}

	data, err := json.Marshal(hs)
	if err != nil {
		panic(err)
	}

	if err := os.WriteFile("planx.handshake", data, 0644); err != nil {
		panic(err)
	}

	// NOTE: The first line written to STDOUT is reserved
	// exclusively for Planx handshake JSON.
	fmt.Printf("%s\n", data)

	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

func RegisterSourceServer(s *grpc.Server, srv pb.SourcePluginServer) {
	pb.RegisterSourcePluginServer(s, srv)
}

func RegisterSinkServer(s *grpc.Server, srv pb.SinkPluginServer) {
	pb.RegisterSinkPluginServer(s, srv)
}

func RegisterProcessorServer(s *grpc.Server, srv pb.ProcessorPluginServer) {
	pb.RegisterProcessorPluginServer(s, srv)
}

func generateSessionID() string {
	return util.NewSessionID()
}
