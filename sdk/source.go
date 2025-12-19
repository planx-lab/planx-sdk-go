package sdk

import "context"

type SourceSPI interface {
	Init(ctx context.Context, config []byte) error
	ReadBatch() (Batch, error)
	Close() error
}
