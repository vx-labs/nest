package nest

import (
	"context"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
	"go.uber.org/zap"
)

type EventProcessor func(context.Context, uint64, []*api.Event) error

type EventsLog interface {
	PutEvents(ctx context.Context, b []*api.Event) error
	LookupTimestamp(ts uint64) uint64
	Consume(ctx context.Context, name string, consumer stream.Consumer, processor EventProcessor) error
}

type eventsLog struct {
	shard  Shard
	logger *zap.Logger
}

func NewEventsLog(ctx context.Context, shard Shard, logger *zap.Logger) (EventsLog, error) {
	s := &eventsLog{
		shard:  shard,
		logger: logger,
	}
	return s, nil
}

func (s *eventsLog) PutEvents(ctx context.Context, b []*api.Event) error {
	payloads := make([][]byte, len(b))
	var err error
	for idx, record := range b {
		payloads[idx], err = proto.Marshal(record)
		if err != nil {
			return err
		}
	}
	return s.shard.PutRecords(ctx, payloads)
}

func (s *eventsLog) LookupTimestamp(ts uint64) uint64 {
	return s.shard.LookupTimestamp(ts)
}
func (s *eventsLog) Consume(ctx context.Context, name string, consumer stream.Consumer, processor EventProcessor) error {
	return s.shard.Consume(func(r io.ReadSeeker) error {
		return consumer.Consume(ctx, r, stream.PerformanceLogger(s.shard,
			s.logger.With(zap.String("stream_name", "events"), zap.String("consumer_name", name)),
			EventDecoder(processor)))
	})
}

// RecordDecoder returns a stream.Processor decoding api.Record and passing them to the provided callback function
func EventDecoder(processor func(context.Context, uint64, []*api.Event) error) stream.Processor {
	return func(ctx context.Context, batch stream.Batch) error {
		records := make([]*api.Event, len(batch.Records))
		for idx, buf := range batch.Records {
			record := &api.Event{}
			err := proto.Unmarshal(buf, record)
			if err != nil {
				return err
			}
			records[idx] = record
		}
		return processor(ctx, batch.FirstOffset, records)
	}
}

func EventTenantMatcher(tenant string, f EventProcessor) EventProcessor {
	return func(ctx context.Context, offset uint64, records []*api.Event) error {
		for idx, record := range records {
			if tenant == record.Tenant {
				err := f(ctx, offset+uint64(idx), []*api.Event{record})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}
