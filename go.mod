module github.com/planx-lab/planx-sdk-go

go 1.25.3

replace github.com/planx-lab/planx-proto => ../planx-proto

require (
	github.com/google/uuid v1.6.0
	github.com/planx-lab/planx-proto v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.77.0
)

require (
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.39.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)
