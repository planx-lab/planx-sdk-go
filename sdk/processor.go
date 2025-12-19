package sdk

import "context"

type ProcessorSPI interface {
	Init(ctx context.Context, config []byte) error
	Process(batch Batch) (Batch, error)
	Close() error
}
