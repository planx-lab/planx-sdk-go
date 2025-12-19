package batch

import (
	"bytes"
	"encoding/gob"
)

type Codec interface {
	Pack(batch any) (PackedBatch, error)
	Unpack(p PackedBatch) (any, error)
}

type gobCodec struct{}

func NewCodec() Codec {
	return &gobCodec{}
}

func (c *gobCodec) Pack(b any) (PackedBatch, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(b)
	return buf.Bytes(), err
}

func (c *gobCodec) Unpack(p PackedBatch) (any, error) {
	var b any
	err := gob.NewDecoder(bytes.NewReader(p)).Decode(&b)
	return b, err
}
