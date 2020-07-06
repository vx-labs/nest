package stream

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type LatenessEstimator interface {
	Latest() uint64
}

func PerformanceLogger(latenessEstimator LatenessEstimator, logger *zap.Logger, processor Processor) Processor {
	return func(ctx context.Context, batch Batch) error {
		if len(batch.Records) > 0 {
			start := time.Now()
			err := processor(ctx, batch)
			l := logger.With(zap.Int("batch_size", len(batch.Records)),
				zap.Duration("batch_processing_time", time.Since(start)),
				zap.Duration("processor_lateness", time.Duration(latenessEstimator.Latest()-batch.LastTimestamp)))

			if err == nil {
				l.Info("stream processed")
			} else {
				l.Error("stream processing failed", zap.Error(err))
			}
			return err
		}
		return nil
	}
}
