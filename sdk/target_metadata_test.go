package sdk

import (
	"reflect"
	"testing"
)

func TestSchemaDiscoveryCanCarryOptionalTargetEvidence(t *testing.T) {
	if _, ok := reflect.TypeOf(SchemaDiscovery{}).FieldByName("TargetMetadata"); !ok {
		t.Fatal("SDK discovery must expose optional target metadata for external Sink components")
	}
}

func TestDiscoveryToProtoPreservesTargetEvidenceAndOldUnknown(t *testing.T) {
	if discoveryToProto(&SchemaDiscovery{Columns: []ColumnInfo{{Name: "id", Type: "bigint"}}}).GetTargetMetadata() != nil {
		t.Fatal("ordinary discovery invented target evidence")
	}
	response := discoveryToProto(&SchemaDiscovery{TargetMetadata: &TargetTableMetadata{
		Columns:          []TargetColumnPolicy{{Name: "id", Identity: true, IdentityAlways: true}},
		UsableUpsertKeys: [][]string{{"id"}}, UsableUpsertKeysComplete: true,
	}})
	meta := response.GetTargetMetadata()
	if meta == nil || !meta.GetUsableUpsertKeysComplete() || len(meta.GetColumns()) != 1 ||
		!meta.Columns[0].GetIdentityAlways() || len(meta.GetUsableUpsertKeys()) != 1 || meta.UsableUpsertKeys[0].Columns[0] != "id" {
		t.Fatalf("SDK target metadata projection lost evidence: %+v", meta)
	}
}
