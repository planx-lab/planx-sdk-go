// Package record defines transport-independent business data shared by builtin
// components and external adapters. Published batches are read-only. Runtime
// identity, delivery acknowledgments, drivers and expressions live elsewhere.
package record

import "time"

type Kind string

const (
	Bool      Kind = "bool"
	Int64     Kind = "int64"
	Uint64    Kind = "uint64"
	Decimal   Kind = "decimal"
	Float64   Kind = "float64"
	String    Kind = "string"
	Binary    Kind = "binary"
	Date      Kind = "date"
	DateTime  Kind = "datetime"
	Timestamp Kind = "timestamp"
	UUID      Kind = "uuid"
	List      Kind = "list"
	Struct    Kind = "struct"
)

type Field struct {
	Name      string `json:"name"`
	Kind      Kind   `json:"kind"`
	Nullable  bool   `json:"nullable"`
	Precision int32  `json:"precision"`
	Scale     int32  `json:"scale"`
	// TimePrecision is a fractional-second bound for DateTime/Timestamp only.
	// Nil means unknown; 0 means whole seconds. Known bounds are in 0..9.
	TimePrecision *int32  `json:"timePrecision,omitempty"`
	Fields        []Field `json:"fields,omitempty"`
	Element       *Field  `json:"element,omitempty"`
}

type Schema struct {
	Fields []Field `json:"fields"`
}

// Value distinguishes absent data (Present=false), SQL NULL (Present=true,
// Null=true), and a concrete value. Data is a closed set checked by Validate:
// bool, int64, uint64, decimal.Decimal, float64, string, []byte, DateValue,
// DateTimeValue, time.Time, [16]byte, []Value, or Record, matching Kind.
type Value struct {
	Kind    Kind `json:"kind"`
	Present bool `json:"present"`
	Null    bool `json:"null"`
	Data    any  `json:"data"`
}

type Record map[string]Value

type Batch struct {
	Schema  Schema   `json:"schema"`
	Records []Record `json:"records"`
}

// DateValue is a Gregorian civil date, without a time or timezone.
type DateValue struct {
	Year  int
	Month time.Month
	Day   int
}

// DateTimeValue is a civil wall-clock value, without an offset or timezone.
// Timestamp instead carries time.Time and represents an absolute instant.
type DateTimeValue struct {
	DateValue
	Hour       int
	Minute     int
	Second     int
	Nanosecond int
}

func NullValue(kind Kind) Value { return Value{Kind: kind, Present: true, Null: true} }

func MissingValue(kind Kind) Value { return Value{Kind: kind} }
