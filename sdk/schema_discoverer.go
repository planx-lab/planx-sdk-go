package sdk

import "context"

// SchemaDiscoverer is an OPTIONAL interface for components that support
// schema discovery. Components without discovery do NOT implement it; the
// PluginServer reports an empty discovery response for them.
type SchemaDiscoverer interface {
	DiscoverSchema(ctx context.Context, config []byte) (*SchemaDiscovery, error)
}

// SchemaDiscovery is the result of a DiscoverSchema call. Phase 1 (config
// has no table) populates Tables; Phase 2 (config has a table) populates
// Columns. A Sink may also supply optional read-only TargetMetadata.
type SchemaDiscovery struct {
	Tables         []TableInfo
	Columns        []ColumnInfo
	TargetMetadata *TargetTableMetadata
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

// TargetTableMetadata is evidence for a selected target. Absent means unknown.
// UsableUpsertKeysComplete applies only to the PostgreSQL ON CONFLICT arbiter
// inventory; an empty list without it does not mean there is no usable key.
type TargetTableMetadata struct {
	Columns                  []TargetColumnPolicy
	UsableUpsertKeys         [][]string
	UsableUpsertKeysComplete bool
}

type TargetColumnPolicy struct {
	Name           string
	HasDefault     bool
	Generated      bool
	Identity       bool
	IdentityAlways bool
}
