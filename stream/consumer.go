package stream

import (
	"context"
	"io"
)

type consumer struct {
	opts ConsumerOpts
}
type consumerOpts func(*ConsumerOpts)

func FromOffset(o int64) consumerOpts {
	return func(c *ConsumerOpts) { c.FromOffset = o }
}
func WithMaxBatchSize(v int) consumerOpts {
	return func(c *ConsumerOpts) { c.MaxBatchSize = v }
}
func WithEOFBehaviour(v eofBehaviour) consumerOpts {
	return func(c *ConsumerOpts) { c.EOFBehaviour = v }
}

// TODO: WithCheckpoint(SnapshotStorage, interval) consumerOpts ?

type Consumer interface {
	Consume(ctx context.Context, r io.ReadSeeker, processor Processor) error
}

func NewConsumer(opts ...consumerOpts) Consumer {
	config := ConsumerOpts{
		MaxBatchSize: 250,
		EOFBehaviour: EOFBehaviourPoll,
		FromOffset:   0,
	}
	for _, opt := range opts {
		opt(&config)
	}
	return consumer{opts: config}
}

func (c consumer) Consume(ctx context.Context, r io.ReadSeeker, processor Processor) error {
	return consume(ctx, r, c.opts, processor)
}
