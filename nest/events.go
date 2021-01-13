package nest

import (
	"context"
	"io"
	"log"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/commitlog/stream"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

type EventProcessor func(context.Context, uint64, []*api.Event) error

type EventsLog interface {
	Shards() ([]uint64, error)
	Shard(shard uint64) (io.ReadSeeker, error)
	PutEvents(ctx context.Context, b []*api.Event) error
	LookupTimestamp(shard uint64, ts uint64) uint64
	Consume(ctx context.Context, consumer stream.Consumer, processor EventProcessor) error
	ConsumeShard(ctx context.Context, shard uint64, consumer stream.Consumer, processor EventProcessor) error
}

type eventsLog struct {
	streamID string
	shard    ShardsController
	logger   *zap.Logger
}

func NewEventsLog(ctx context.Context, shard ShardsController, streamID string, logger *zap.Logger) (EventsLog, error) {
	s := &eventsLog{
		streamID: streamID,
		shard:    shard,
		logger:   logger,
	}
	return s, nil
}

func (s *eventsLog) PutEvents(ctx context.Context, b []*api.Event) error {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return err
	}
	for _, record := range b {
		payload, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		err = s.shard.Write(ctx, stream, string(record.Tenant), payload)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *eventsLog) Shards() ([]uint64, error) {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return nil, err
	}
	return s.shard.ListShards(stream)
}
func (s *eventsLog) LookupTimestamp(shard uint64, ts uint64) uint64 {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return 0
	}
	v, _ := s.shard.ResolveTimestamp(stream, shard, ts)
	return v
}

func (s *eventsLog) ConsumeShard(ctx context.Context, shard uint64, consumer stream.Consumer, processor EventProcessor) error {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return err
	}
	r, err := s.shard.Reader(stream, shard)
	if err != nil {
		return err
	}
	return consumer.Consume(ctx, r, EventDecoder(processor))
}

func (s *eventsLog) Shard(shard uint64) (io.ReadSeeker, error) {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return nil, err
	}
	return s.shard.Reader(stream, shard)
}

func (s *eventsLog) Consume(ctx context.Context, consumer stream.Consumer, processor EventProcessor) error {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return err
	}
	shards, err := s.shard.ListShards(stream)
	if err != nil {
		log.Printf("no shard found for stream: %v", err)
	}
	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for idx := range shards {
		shard := shards[idx]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.ConsumeShard(ctx, shard, consumer, processor)
			if err != nil {
				log.Print(err)
				cancel()
				return
			}
		}()
	}
	wg.Wait()
	return nil
}

// EventDecoder returns a stream.Processor decoding api.Record and passing them to the provided callback function
func EventDecoder(processor func(context.Context, uint64, []*api.Event) error) stream.Processor {
	return func(ctx context.Context, batch stream.Batch) error {
		if len(batch.Records) == 0 {
			return nil
		}
		records := make([]*api.Event, len(batch.Records))
		for idx, buf := range batch.Records {
			record := &api.Event{}
			err := proto.Unmarshal(buf.Payload(), record)
			if err != nil {
				return err
			}
			records[idx] = record
		}
		return processor(ctx, batch.Records[0].Offset(), records)
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
