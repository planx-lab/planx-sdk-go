package sdk

import "context"

// SchemaDiscoverer is an OPTIONAL interface for components that support
// schema discovery (DB sources). Non-DB sources do NOT implement it; the
// PluginServer reports an empty discovery response for them.
type SchemaDiscoverer interface {
	DiscoverSchema(ctx context.Context, config []byte) (*SchemaDiscovery, error)
}

// SchemaDiscovery is the result of a DiscoverSchema call. Phase 1 (config
// has no table) populates Tables; Phase 2 (config has a table) populates
// Columns.
type SchemaDiscovery struct {
	Tables  []TableInfo
	Columns []ColumnInfo
}

// TableInfo describes one table discovered via information_schema.
type TableInfo struct {
	Schema string
	Name   string
}

// ColumnInfo describes one column of a selected table.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}
