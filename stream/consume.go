package stream

import (
	"context"
	"io"
)

// Processor is a function that will process stream records
type Processor func(context.Context, Batch) error

// Consume starts a poller, and calls Processor on each stream records
func Consume(ctx context.Context, r io.ReadSeeker, opts ConsumerOptions, processor Processor) error {
	poller := newPoller(ctx, r, opts)
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-poller.Ready():
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
