package record_test

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/planx-lab/planx-sdk-go/record"
	"github.com/shopspring/decimal"
)

func value(k record.Kind, data any) record.Value {
	return record.Value{Kind: k, Present: true, Data: data}
}

func fixture() record.Batch {
	return record.Batch{
		Schema: record.Schema{Fields: []record.Field{
			{Name: "decimal38", Kind: record.Decimal, Precision: 38, Scale: 2},
			{Name: "fraction", Kind: record.Decimal},
			{Name: "signed", Kind: record.Int64},
			{Name: "unsigned", Kind: record.Uint64},
			{Name: "empty", Kind: record.String},
			{Name: "missing", Kind: record.String},
			{Name: "absent", Kind: record.String},
			{Name: "null", Kind: record.String, Nullable: true},
			{Name: "binary", Kind: record.Binary},
			{Name: "date", Kind: record.Date},
			{Name: "local", Kind: record.DateTime},
			{Name: "instant", Kind: record.Timestamp},
			{Name: "uuid", Kind: record.UUID},
			{Name: "list", Kind: record.List, Element: &record.Field{Kind: record.Binary}},
			{Name: "object", Kind: record.Struct, Fields: []record.Field{{Name: "flag", Kind: record.Bool}}},
		}},
		Records: []record.Record{{
			"decimal38": value(record.Decimal, decimal.RequireFromString("123456789012345678901234567890123456.78")),
			"fraction":  value(record.Decimal, decimal.RequireFromString("0."+strings.Repeat("1234567890", 32))),
			"signed":    value(record.Int64, int64(math.MinInt64)),
			"unsigned":  value(record.Uint64, uint64(math.MaxUint64)),
			"empty":     value(record.String, ""),
			"missing":   record.MissingValue(record.String),
			"null":      record.NullValue(record.String),
			"binary":    value(record.Binary, []byte{0, 255, 1}),
			"date":      value(record.Date, record.DateValue{Year: 2024, Month: time.February, Day: 29}),
			"local":     value(record.DateTime, record.DateTimeValue{DateValue: record.DateValue{Year: 2024, Month: time.February, Day: 29}, Hour: 23, Minute: 59, Second: 58, Nanosecond: 123456789}),
			"instant":   value(record.Timestamp, time.Date(2024, 2, 29, 23, 59, 58, 123456789, time.FixedZone("test", 8*3600))),
			"uuid":      value(record.UUID, [16]byte{1, 2, 3, 4}),
			"list":      value(record.List, []record.Value{value(record.Binary, []byte{4, 5})}),
			"object":    value(record.Struct, record.Record{"flag": value(record.Bool, false)}),
		}},
	}
}

func TestExactValuesAndPresence(t *testing.T) {
	b := fixture()
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, v := range b.Records[0] {
		if v.Present && !v.Null {
			if _, err := record.NewValue(v.Kind, v.Data); err != nil {
				t.Fatalf("%s: %v", v.Kind, err)
			}
		}
	}
	if _, ok := b.Records[0]["absent"]; ok {
		t.Fatal("absent key appeared")
	}
	if b.Records[0]["missing"].Present || !b.Records[0]["null"].Null || b.Records[0]["empty"].Data != "" {
		t.Fatal("presence conflated")
	}
}

func TestNewValueRejectsUnsupportedAndInexactConcreteTypes(t *testing.T) {
	for _, tc := range []struct {
		kind record.Kind
		data any
	}{
		{record.Int64, int(1)}, {record.Int64, float64(1)}, {record.Uint64, int64(1)},
		{record.Decimal, "1.23"}, {record.String, []byte("x")}, {record.Binary, "x"},
		{record.Float64, math.NaN()}, {record.Float64, math.Inf(1)}, {record.Bool, nil},
		{record.String, string([]byte{255})}, {record.UUID, "uuid"},
		{record.Date, record.DateValue{Year: 2023, Month: 2, Day: 29}},
		{record.DateTime, record.DateTimeValue{DateValue: record.DateValue{Year: 2024, Month: 1, Day: 1}, Hour: 24}},
		{record.List, []any{1}}, {record.Struct, map[string]any{"x": 1}}, {"unknown", "x"},
	} {
		t.Run(fmt.Sprintf("%s/%T", tc.kind, tc.data), func(t *testing.T) {
			if _, err := record.NewValue(tc.kind, tc.data); err == nil {
				t.Fatal("accepted unsupported value")
			}
		})
	}
}

func TestBatchValidationRejectsSchemaAndValueMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*record.Batch){
		"duplicate field":        func(b *record.Batch) { b.Schema.Fields = append(b.Schema.Fields, b.Schema.Fields[0]) },
		"unknown kind":           func(b *record.Batch) { b.Schema.Fields[0].Kind = "unknown" },
		"extra key":              func(b *record.Batch) { b.Records[0]["extra"] = value(record.String, "x") },
		"wrong concrete integer": func(b *record.Batch) { b.Records[0]["signed"] = value(record.Int64, float64(1)) },
		"null forbidden":         func(b *record.Batch) { b.Records[0]["empty"] = record.NullValue(record.String) },
		"null with data": func(b *record.Batch) {
			b.Records[0]["null"] = record.Value{Kind: record.String, Present: true, Null: true, Data: "secret"}
		},
		"missing with data":  func(b *record.Batch) { b.Records[0]["missing"] = record.Value{Kind: record.String, Data: "secret"} },
		"precision overflow": func(b *record.Batch) { b.Schema.Fields[0].Precision = 37 },
		"inexact scale":      func(b *record.Batch) { b.Schema.Fields[0].Scale = 1 },
		"list element mismatch": func(b *record.Batch) {
			b.Records[0]["list"] = value(record.List, []record.Value{value(record.String, "x")})
		},
		"struct field mismatch": func(b *record.Batch) {
			b.Records[0]["object"] = value(record.Struct, record.Record{"flag": value(record.String, "x")})
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := fixture()
			mutate(&b)
			if err := b.Validate(); err == nil {
				t.Fatal("accepted invalid batch")
			}
		})
	}
}

func TestDecimalNegativeScaleAndScaleGreaterThanPrecision(t *testing.T) {
	for _, tc := range []struct {
		p, s  int32
		text  string
		valid bool
	}{
		{2, -2, "1200", true}, {2, -2, "1230", false},
		{2, -2, "10000", false}, {2, 3, "0.012", true}, {2, 3, "0.123", false},
		{0, 0, "0.00000000000000000000000000000000000000000000001", true},
	} {
		b := record.Batch{Schema: record.Schema{Fields: []record.Field{{Name: "d", Kind: record.Decimal, Precision: tc.p, Scale: tc.s}}}, Records: []record.Record{{"d": value(record.Decimal, decimal.RequireFromString(tc.text))}}}
		if err := b.Validate(); (err == nil) != tc.valid {
			t.Fatalf("p=%d s=%d: %v", tc.p, tc.s, err)
		}
	}
}

func TestCloneOwnsNestedDataAndSchema(t *testing.T) {
	original := fixture()
	cloned := original.Clone()
	cloned.Records[0]["binary"].Data.([]byte)[0] = 99
	cloned.Records[0]["list"].Data.([]record.Value)[0].Data.([]byte)[0] = 99
	cloned.Records[0]["object"].Data.(record.Record)["flag"] = value(record.Bool, true)
	cloned.Schema.Fields[13].Element.Kind = record.String
	cloned.Schema.Fields[14].Fields[0].Name = "changed"
	delete(cloned.Records[0], "signed")
	if original.Records[0]["binary"].Data.([]byte)[0] != 0 || original.Records[0]["list"].Data.([]record.Value)[0].Data.([]byte)[0] != 4 {
		t.Fatal("binary storage shared")
	}
	if original.Records[0]["object"].Data.(record.Record)["flag"].Data != false {
		t.Fatal("record map shared")
	}
	if original.Schema.Fields[13].Element.Kind != record.Binary || original.Schema.Fields[14].Fields[0].Name != "flag" {
		t.Fatal("schema shared")
	}
	if _, ok := original.Records[0]["signed"]; !ok {
		t.Fatal("record storage shared")
	}
}

func TestAppendPreservesArgumentOrderAndEmptySchema(t *testing.T) {
	a, b := fixture(), fixture()
	a.Records[0]["empty"] = value(record.String, "edge-z")
	b.Records[0]["empty"] = value(record.String, "edge-a")
	empty := a.Clone()
	empty.Records = nil
	joined, err := record.Append(a, empty, b)
	if err != nil {
		t.Fatal(err)
	}
	if len(joined.Records) != 2 || joined.Records[0]["empty"].Data != "edge-z" || joined.Records[1]["empty"].Data != "edge-a" {
		t.Fatal("edge order changed")
	}
	joined.Records[0]["binary"].Data.([]byte)[0] = 99
	if a.Records[0]["binary"].Data.([]byte)[0] != 0 {
		t.Fatal("append shares mutable input")
	}
	joined, err = record.Append(empty, empty)
	if err != nil || len(joined.Records) != 0 || !reflect.DeepEqual(joined.Schema, empty.Schema) {
		t.Fatal("empty schema lost")
	}
	b.Schema.Fields[0].Scale++
	if _, err := record.Append(a, b); err == nil {
		t.Fatal("appended incompatible schemas")
	}
	if _, err := record.Append(); err == nil {
		t.Fatal("no schema supplied")
	}
}

func TestTypedEncodingRoundTrip(t *testing.T) {
	original := fixture()
	encoded, err := record.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := record.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original.Schema, decoded.Schema) {
		t.Fatal("schema changed")
	}
	for k, want := range original.Records[0] {
		got, ok := decoded.Records[0][k]
		if !ok || got.Kind != want.Kind || got.Present != want.Present || got.Null != want.Null {
			t.Fatalf("%s presence/type changed", k)
		}
		switch v := want.Data.(type) {
		case decimal.Decimal:
			d, ok := got.Data.(decimal.Decimal)
			if !ok || !d.Equal(v) || d.Exponent() != v.Exponent() {
				t.Fatalf("%s decimal changed", k)
			}
		case time.Time:
			if !got.Data.(time.Time).Equal(v) {
				t.Fatal("instant changed")
			}
		default:
			if !reflect.DeepEqual(got.Data, want.Data) {
				t.Fatalf("%s data changed: %T -> %T", k, want.Data, got.Data)
			}
		}
	}
	if _, ok := decoded.Records[0]["absent"]; ok {
		t.Fatal("absent became explicit")
	}
	decoded.Records[0]["binary"].Data.([]byte)[0] = 22
	if original.Records[0]["binary"].Data.([]byte)[0] != 0 {
		t.Fatal("codec shared binary storage")
	}
}

func TestTypedEncodingRejectsInvalidVersionBoundsAndDuplicateKeys(t *testing.T) {
	for _, data := range [][]byte{
		[]byte("planx-record/2\n{}"),
		[]byte("planx-record/1\n{\"schema\":{},\"schema\":{},\"records\":[]}"),
		[]byte("planx-record/1\n{} {}"),
		[]byte(strings.Repeat("x", record.MaxEncodedBytes+1)),
	} {
		if _, err := record.Decode(data); err == nil {
			t.Fatal("accepted invalid encoding")
		}
	}
	b := fixture()
	b.Records[0]["binary"] = value(record.Binary, make([]byte, record.MaxEncodedBytes+1))
	if _, err := record.Encode(b); err == nil {
		t.Fatal("accepted oversized batch")
	}
}
