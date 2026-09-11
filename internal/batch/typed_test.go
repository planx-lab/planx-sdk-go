package batch

import (
	"bytes"
	"testing"

	"github.com/planx-lab/planx-sdk-go/record"
	"github.com/shopspring/decimal"
)

func TestTypedBatchUsesPublicVersionedEncoding(t *testing.T) {
	d := decimal.RequireFromString("123456789012345678901234567890123456.78")
	b := record.Batch{Schema: record.Schema{Fields: []record.Field{{Name: "d", Kind: record.Decimal, Precision: 38, Scale: 2}}}, Records: []record.Record{{"d": {Kind: record.Decimal, Present: true, Data: d}}}}
	codec := NewCodec()
	for _, input := range []any{b, &b} {
		encoded, err := codec.Pack(input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(encoded, []byte(record.WirePrefix)) {
			t.Fatal("typed data did not use versioned boundary")
		}
		decoded, err := record.Decode(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !decoded.Records[0]["d"].Data.(decimal.Decimal).Equal(d) {
			t.Fatal("decimal changed")
		}
	}
	encoded, err := record.Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Unpack(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.(record.Batch).Records[0]["d"].Data.(decimal.Decimal).Equal(d) {
		t.Fatal("public encoding not accepted")
	}
	if _, err := codec.Pack((*record.Batch)(nil)); err == nil {
		t.Fatal("accepted nil typed pointer")
	}
	if _, err := codec.Unpack([]byte(record.WirePrefix + "999\n{}")); err == nil {
		t.Fatal("accepted unknown version")
	}
}
