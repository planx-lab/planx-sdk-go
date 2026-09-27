package sdk

import (
	"context"

	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
	"github.com/planx-lab/planx-sdk-go/internal/observability"
	"github.com/planx-lab/planx-sdk-go/internal/runtime"
	"google.golang.org/grpc"
)

// ComponentKind is the runtime role of a Component (SOURCE/PROCESSOR/SINK).
// Aliased from the proto so plugins do not import proto directly.
type ComponentKind = pb.ComponentKind

const (
	KindSource    = pb.ComponentKind_COMPONENT_KIND_SOURCE
	KindProcessor = pb.ComponentKind_COMPONENT_KIND_PROCESSOR
	KindSink      = pb.ComponentKind_COMPONENT_KIND_SINK
)

// Example is a documentation example embedded in the plugin descriptor.
type Example = pb.Example

// ConfigSchema re-exports the proto ConfigSchema for declaration via sdk.Schema.
type ConfigSchema = pb.ConfigSchema

// Capabilities re-exports the proto Capabilities.
type Capabilities = pb.Capabilities

// ComponentStatus re-exports the proto ComponentStatus (maturity / visibility).
type ComponentStatus = pb.ComponentStatus

// ComponentSpec declares one Component of a Plugin and binds it to its SPI
// factory. Exactly one of Source/Processor/Sink is set, matching Kind.
type ComponentSpec struct {
	ID           string
	Kind         ComponentKind
	DisplayName  string
	Description  string
	ConfigSchema *ConfigSchema
	Capabilities *Capabilities
	Status       *ComponentStatus

	// Validate is the optional live config check (Designer connectivity).
	// nil => ValidateConfig responds ok=true (schema-only validation).
	Validate func(ctx context.Context, config []byte) (bool, string)

	// DiscoverSchema is the optional schema-discovery hook (DB sources).
	// nil => DiscoverSchema responds with an empty result. See
	// SchemaDiscoverer for the public types.
	DiscoverSchema func(ctx context.Context, config []byte) (*SchemaDiscovery, error)

	// Health is the optional live-readiness probe (e.g. DB connectivity). nil =>
	// the component is considered healthy. The PluginServer.Health RPC returns
	// STATE_NOT_READY if ANY component's hook reports unhealthy, so the Engine
	// can stop routing traffic to a plugin whose dependency is down.
	Health func(ctx context.Context) (bool, string)

	// Exactly one factory is set, matching Kind.
	Source    func() SourceSPI
	Processor func() ProcessorSPI
	Sink      func() SinkSPI
}

// Plugin declares a plugin binary: its identity and its Components (>= 1).
type Plugin struct {
	ID          string
	Version     string
	DisplayName string
	Description string
	Vendor      string
	License     string
	Homepage    string
	Icon        string
	Keywords    []string
	Summary     string
	Examples    []Example
	Components  []ComponentSpec
}

// Serve builds and runs the plugin's gRPC server (all five services). It
// blocks forever; a construction error (no components, duplicate id) panics.
//
// If the PLANX_SDK_METRICS_ADDR environment variable is set to a non-empty
// listen address (e.g. "127.0.0.1:9100"), Serve also starts an HTTP server
// exposing Prometheus metrics at /metrics. The metrics server is opt-in and
// disabled by default, so existing plugins behave exactly as before unless
// the env var is set.
func Serve(p Plugin) {
	// Opt-in metrics endpoint. No-op (returns nil) when env var is unset. A
	// failure to start the metrics server must NOT take down the data plane:
	// log and continue without metrics. Stop() is nil-safe.
	metricsSrv, err := observability.StartMetricsServer()
	if err != nil {
		observability.Logger().Error("planx-sdk metrics server failed to start; continuing without metrics", "err", err)
		metricsSrv = nil
	}
	defer metricsSrv.Stop()

	desc, comps := buildRegistration(p)
	srv, err := runtime.NewPluginServer(desc, comps)
	if err != nil {
		panic(err)
	}
	runtime.ServeGRPC(func(s *grpc.Server) {
		runtime.RegisterAll(s, srv)
	})
}

// buildRegistration converts a sdk.Plugin into a proto PluginDescriptor and a
// slice of runtime.ComponentRegistration. Pure; tested directly.
//
// NOTE on the factory wrappers: Go function types are INVARIANT --
// `func() sdk.SourceSPI` is NOT assignable to `func() runtime.SourceSPI`
// even though the two SPI interfaces have identical method sets (structural
// typing only applies to a value satisfying an interface, not to function
// type assignability). The wrapper `func() runtime.SourceSPI { return c.Source() }`
// works because inside it `c.Source()` returns an interface value (sdk.SourceSPI)
// which IS assignable to the runtime.SourceSPI interface (interface-to-interface,
// identical method sets). The `if c.Source != nil` guard preserves the nil
// semantics that CreateSession relies on (factory-nil -> FailedPrecondition).
func buildRegistration(p Plugin) (*pb.PluginDescriptor, []runtime.ComponentRegistration) {
	examples := make([]*pb.Example, len(p.Examples))
	for i := range p.Examples {
		examples[i] = &p.Examples[i]
	}
	desc := &pb.PluginDescriptor{
		Id:             p.ID,
		Version:        p.Version,
		RuntimeVersion: "v4",
		DisplayName:    p.DisplayName,
		Description:    p.Description,
		Vendor:         p.Vendor,
		License:        p.License,
		Homepage:       p.Homepage,
		Icon:           p.Icon,
		Keywords:       p.Keywords,
		Documentation: &pb.Documentation{
			Summary:  p.Summary,
			Homepage: p.Homepage,
			Examples: examples,
		},
	}
	comps := make([]runtime.ComponentRegistration, len(p.Components))
	for i, c := range p.Components {
		descC := &pb.ComponentDescriptor{
			Id:           c.ID,
			Kind:         c.Kind,
			DisplayName:  c.DisplayName,
			Description:  c.Description,
			ConfigSchema: c.ConfigSchema,
			Capabilities: c.Capabilities,
			Status:       c.Status,
		}
		desc.Components = append(desc.Components, descC)
		reg := runtime.ComponentRegistration{
			Descriptor: descC,
			Validate:   c.Validate, // identical anonymous func type; no wrapper needed
			Health:     c.Health,   // identical anonymous func type; no wrapper needed
		}
		if c.Source != nil {
			reg.SourceFactory = func() runtime.SourceSPI { return c.Source() }
		}
		if c.Processor != nil {
			reg.ProcessorFactory = func() runtime.ProcessorSPI { return c.Processor() }
		}
		if c.Sink != nil {
			reg.SinkFactory = func() runtime.SinkSPI { return c.Sink() }
		}
		if c.DiscoverSchema != nil {
			reg.Discover = func(ctx context.Context, config []byte) (*pb.DiscoverSchemaResponse, error) {
				disc, err := c.DiscoverSchema(ctx, config)
				if err != nil {
					return nil, err
				}
				return discoveryToProto(disc), nil
			}
		}
		comps[i] = reg
	}
	return desc, comps
}

// discoveryToProto projects an sdk.SchemaDiscovery into the proto response.
// Lives in the sdk package (not runtime) to avoid an sdk<->runtime cycle.
func discoveryToProto(d *SchemaDiscovery) *pb.DiscoverSchemaResponse {
	resp := &pb.DiscoverSchemaResponse{}
	for _, t := range d.Tables {
		resp.Tables = append(resp.Tables, &pb.TableInfo{Schema: t.Schema, Name: t.Name})
	}
	for _, c := range d.Columns {
		resp.Columns = append(resp.Columns, &pb.ColumnInfo{
			Name: c.Name, Type: c.Type, Nullable: c.Nullable,
		})
	}
	if d.TargetMetadata != nil {
		meta := &pb.TargetTableMetadata{UsableUpsertKeysComplete: d.TargetMetadata.UsableUpsertKeysComplete}
		for _, policy := range d.TargetMetadata.Columns {
			meta.Columns = append(meta.Columns, &pb.TargetColumnPolicy{
				Name: policy.Name, HasDefault: policy.HasDefault, Generated: policy.Generated,
				Identity: policy.Identity, IdentityAlways: policy.IdentityAlways,
			})
		}
		for _, key := range d.TargetMetadata.UsableUpsertKeys {
			meta.UsableUpsertKeys = append(meta.UsableUpsertKeys, &pb.ColumnSet{Columns: append([]string(nil), key...)})
		}
		resp.TargetMetadata = meta
	}
	return resp
}
