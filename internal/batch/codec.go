package batch

import (
	"bytes"
	"encoding/gob"
)

func init() {
	gob.Register(map[string]string{})
}

// RegisterType registers a custom type with the gob codec so it can be
// serialized in plugin batch data. Plugins that return custom types from
// ReadBatch/Process must call this before serving.
func RegisterType(value any) {
	gob.Register(value)
}

type batchWrapper struct {
	Batch any
}

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
	err := gob.NewEncoder(&buf).Encode(batchWrapper{Batch: b})
	return buf.Bytes(), err
}

func (c *gobCodec) Unpack(p PackedBatch) (any, error) {
	var w batchWrapper
	err := gob.NewDecoder(bytes.NewReader(p)).Decode(&w)
	return w.Batch, err
}
