package record

import "testing"

func TestEstimatedBytesAccountsForRetainedVariableStorage(t *testing.T) {
	base := Batch{Schema: Schema{Fields: []Field{{Name: "value", Kind: Binary}}}, Records: []Record{{"value": {Kind: Binary, Present: true, Data: []byte{}}}}}
	large := base.Clone()
	large.Records[0]["value"] = Value{Kind: Binary, Present: true, Data: make([]byte, 1, 4096)}
	if EstimatedBytes(base) <= 0 || EstimatedBytes(large)-EstimatedBytes(base) < 4096 {
		t.Fatal("retained binary capacity not accounted")
	}
	nested := Batch{Schema: Schema{Fields: []Field{{Name: "items", Kind: List, Element: &Field{Kind: Binary}}}}, Records: []Record{{"items": {Kind: List, Present: true, Data: []Value{{Kind: Binary, Present: true, Data: make([]byte, 8192)}}}}}}
	if EstimatedBytes(nested) < 8192 {
		t.Fatal("nested storage not accounted")
	}
	if len(large.Records[0]["value"].Data.([]byte)) != 1 {
		t.Fatal("size accounting mutated data")
	}
}
