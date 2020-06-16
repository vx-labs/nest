package stream

import (
	"context"
	"io"
)

// Processor is a function that will process stream records
type Processor func(context.Context, Batch) error

// Consume starts a Session, and calls Processor on each stream records
func Consume(ctx context.Context, r io.ReadSeeker, opts ConsumerOptions, processor Processor) error {
	session := NewSession(ctx, r, opts)
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-session.Ready():
			err := processor(ctx, batch)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
		}
	}
}
