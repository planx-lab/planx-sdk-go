# planx-sdk-go

Official Go SDK for developing Planx plugins.

## Features
- **gRPC Server Wrappers**: Zero-boilerplate plugin servers.
- **Session Management**: Handles multi-tenant sessions automatically.
- **Flow Control**: Implements backpressure and window management.
- **Tracing**: Built-in OpenTelemetry support.

## Usage
Import `github.com/planx-lab/planx-sdk-go/sdk` and use `sdk.ServeSource` or `sdk.ServeSink`.
