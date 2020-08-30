package stream

import (
	"context"
	"io"

	"go.uber.org/zap"
)

type consumer struct {
	opts ConsumerOpts
}
type consumerOpts func(*ConsumerOpts)
type OffsetIterator interface {
	Next() (uint64, error)
	AdvanceTo(uint64)
}

func FromOffset(o int64) consumerOpts {
	return func(c *ConsumerOpts) { c.FromOffset = o }
}
func WithMaxRecordCount(o int64) consumerOpts {
	return func(c *ConsumerOpts) { c.MaxRecordCount = o }
}
func WithMaxBatchSize(v int) consumerOpts {
	return func(c *ConsumerOpts) { c.MaxBatchSize = v }
}
func WithEOFBehaviour(v eofBehaviour) consumerOpts {
	return func(c *ConsumerOpts) { c.EOFBehaviour = v }
}
func WithOffsetIterator(v OffsetIterator) consumerOpts {
	return func(c *ConsumerOpts) { c.OffsetProvider = v }
}
func WithName(v string) consumerOpts {
	return func(c *ConsumerOpts) { c.Name = v }
}
func WithPerformanceLogging(latenessEstimator LatenessEstimator, logger *zap.Logger) consumerOpts {
	return func(c *ConsumerOpts) {
		c.Middleware = append(c.Middleware, func(p Processor, opts ConsumerOpts) Processor {
			if (opts.Name) != "" {
				logger = logger.With(zap.String("consumer_name", opts.Name))
			}
			logger = logger.With(
				zap.Int("consumer_max_batch_size", opts.MaxBatchSize),
			)
			return PerformanceLogger(latenessEstimator, logger, p)
		})
	}
}

// TODO: WithCheckpoint(SnapshotStorage, interval) consumerOpts ?

type Consumer interface {
	Consume(ctx context.Context, r io.ReadSeeker, processor Processor) error
}

func NewConsumer(opts ...consumerOpts) Consumer {
	config := ConsumerOpts{
		MaxBatchSize:              10,
		MaxBatchMemorySizeInBytes: 20000000,
		EOFBehaviour:              EOFBehaviourPoll,
		FromOffset:                0,
		MaxRecordCount:            -1,
	}
	for _, opt := range opts {
		opt(&config)
	}
	return consumer{opts: config}
}

func (c consumer) Consume(ctx context.Context, r io.ReadSeeker, processor Processor) error {
	return consume(ctx, r, c.opts, processor)
}
