package record

import (
	"time"

	"github.com/shopspring/decimal"
)

// EstimatedBytes accounts for retained business-data storage without serializing
// a valid batch. It counts slice capacity and charges shared data to each owner.
// Fixed overhead allowances are conservative accounting units, not an exact Go
// heap/RSS measurement or a substitute for measured process memory limits.
// Call Validate first; invalid cyclic values are outside the typed contract.
func EstimatedBytes(b Batch) int64 {
	n := int64(64 + cap(b.Records)*8)
	n += estimatedFields(b.Schema.Fields)
	for _, r := range b.Records {
		n += estimatedRecord(r)
	}
	return n
}

func estimatedFields(fields []Field) int64 {
	n := int64(cap(fields) * 128)
	for _, f := range fields {
		n += int64(len(f.Name) + len(f.Kind))
		if f.TimePrecision != nil {
			n += 8 // Conservative allocation allowance, independent of its value.
		}
		n += estimatedFields(f.Fields)
		if f.Element != nil {
			n += estimatedFields([]Field{*f.Element})
		}
	}
	return n
}

func estimatedRecord(r Record) int64 {
	n := int64(64 + len(r)*128)
	for name, v := range r {
		n += int64(len(name)) + estimatedValue(v)
	}
	return n
}

func estimatedValue(v Value) int64 {
	switch data := v.Data.(type) {
	case nil:
		return 0
	case string:
		return int64(len(data))
	case []byte:
		return int64(cap(data))
	case []Value:
		n := int64(cap(data) * 64)
		for _, item := range data {
			n += estimatedValue(item)
		}
		return n
	case Record:
		return estimatedRecord(data)
	case decimal.Decimal:
		return 64 + int64((data.Coefficient().BitLen()+7)/8)
	case DateValue:
		return 32
	case DateTimeValue:
		return 64
	case time.Time:
		return 32
	default:
		return 16 // Valid fixed-size scalar or UUID.
	}
}
