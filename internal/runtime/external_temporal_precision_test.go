//go:build integration

package runtime_test

import (
	"testing"
	"time"

	"github.com/planx-lab/planx-sdk-go/record"
)

func TestExternalTypedFixtureIncludesKnownMicroseconds(t *testing.T) {
	assertExternalMicroseconds(t, externalTypedFixture())
}

func assertExternalMicroseconds(t *testing.T, batch record.Batch) {
	t.Helper()
	const name = "timestamp_microseconds"
	var field *record.Field
	for i := range batch.Schema.Fields {
		if batch.Schema.Fields[i].Name == name {
			field = &batch.Schema.Fields[i]
			break
		}
	}
	if field == nil || field.Kind != record.Timestamp || field.TimePrecision == nil || *field.TimePrecision != 6 {
		t.Fatalf("external fixture must retain %s Timestamp timePrecision=6: %+v", name, field)
	}
	want := time.Date(2024, time.February, 29, 4, 0, 0, 123456000, time.UTC)
	for i, row := range batch.Records {
		value := row[name]
		instant, ok := value.Data.(time.Time)
		if !ok || value.Kind != record.Timestamp || !value.Present || value.Null || !instant.Equal(want) || instant.Nanosecond() != 123456000 {
			t.Fatalf("record %d lost exact p6 instant %s: %+v", i, want.Format(time.RFC3339Nano), value)
		}
	}
}
