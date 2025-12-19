package sdk

import "context"

type SinkSPI interface {
	Init(ctx context.Context, config []byte) error
	WriteBatch(batch Batch) error
	Close() error
}
