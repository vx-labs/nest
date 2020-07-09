package stream

import (
	"context"
	"io"
)

// Processor is a function that will process stream records
type Processor func(context.Context, Batch) error

// consume starts a poller, and calls Processor on each stream records
func consume(ctx context.Context, r io.ReadSeeker, opts ConsumerOpts, processor Processor) error {
	poller := newPoller(ctx, r, opts)
	for _, middleware := range opts.Middleware {
		processor = middleware(processor, opts)
	}
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
				return poller.Error()
			}
		}
	}
}
