package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/commitlog/stream"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	messagesBucketName []byte = []byte("messages")
	appliedIndexKey    []byte = []byte("_index")
	encoding                  = binary.BigEndian
)

type RemoteCaller func(id uint64, f func(*grpc.ClientConn) error) error
type RecordProcessor func(context.Context, uint64, []*api.Record) error

type MessageLog interface {
	StartConsumers(ctx context.Context)
	ListShards() []uint64
	PutRecords(ctx context.Context, b []*api.Record) error
	ListTopics(pattern []byte) []*api.TopicMetadata
	TopicsIterator(shard uint64, pattern []byte) stream.OffsetIterator
	Consume(ctx context.Context, shard uint64, consumer stream.Consumer, processor RecordProcessor) error
}

type Snapshot struct {
	Remote         uint64 `json:"remote,omitempty"`
	MessagesOffset uint64 `json:"messages_offset,omitempty"`
	StateOffset    uint64 `json:"state_offset,omitempty"`
}

type messageLog struct {
	streamID string
	shard    ShardsController
	logger   *zap.Logger
	topics   map[uint64]*topicAggregate
}

func NewMessageLog(ctx context.Context, shard ShardsController, streamID string, logger *zap.Logger) (MessageLog, error) {
	s := &messageLog{
		streamID: streamID,
		shard:    shard,
		logger:   logger,
		topics:   make(map[uint64]*topicAggregate),
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			_, err := s.shard.Stream(s.streamID)
			if err == nil {
				s.StartConsumers(ctx)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return s, nil
}

func (s *messageLog) ListShards() []uint64 {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return nil
	}
	v, err := s.shard.ListShards(stream)
	if err != nil {
		return nil
	}
	return v
}
func (s *messageLog) StartConsumers(ctx context.Context) {
	streamID, err := s.shard.Stream(s.streamID)
	if err != nil {
		return
	}
	shards, err := s.shard.ListShards(streamID)
	if err != nil {
		log.Printf("no shard found for stream: %v", err)
	}
	for idx := range shards {
		shard := shards[idx]
		s.topics[shard] = &topicAggregate{topics: NewTopicState()}
		go func() {
			for {
				consumer := stream.NewConsumer(
					stream.WithName("topics_indexer"),
					stream.FromOffset(0),
					stream.WithMaxBatchSize(100),
					stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
				)
				err := s.Consume(ctx, shard, consumer, s.topics[shard].Processor())
				if err != nil {
					if err != context.Canceled {
						s.logger.Error("topics indexer failed to run", zap.Error(err))
						<-time.After(1 * time.Second)
					} else {
						return
					}
				}
			}
		}()
	}
}

func (s *messageLog) PutRecords(ctx context.Context, b []*api.Record) error {
	streamID, err := s.shard.Stream(s.streamID)
	if err != nil {
		return err
	}
	for idx := range b {
		record := b[idx]
		payload, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		err = s.shard.Write(ctx, streamID, string(record.Topic), payload)
		if err != nil {
			return err
		}
	}
	return nil
}

func cut(t []byte) ([]byte, string) {
	end := bytes.IndexByte(t, '/')
	if end < 0 {
		return nil, string(t)
	}
	return t[end+1:], string(t[:end])
}

func match(pattern []byte, topic []byte) bool {
	var patternToken string
	var topicToken string
	pattern, patternToken = cut(pattern)
	if patternToken == "#" {
		return true
	}
	topic, topicToken = cut(topic)
	if len(topic) == 0 || len(pattern) == 0 {
		return len(topic) == 0 && len(pattern) == 0 && (topicToken == patternToken || patternToken == "+")
	}
	if topicToken == patternToken || patternToken == "+" {
		return match(pattern, topic)
	}
	return false
}

func (s *messageLog) ListTopics(pattern []byte) []*api.TopicMetadata {
	if len(pattern) == 0 {
		pattern = []byte("#")
	}
	out := make([]*api.TopicMetadata, 0)
	for _, aggreg := range s.topics {
		topics := aggreg.topics.Match(pattern)
		for idx := range topics {
			out = append(out, &api.TopicMetadata{
				Name:               topics[idx].Name,
				MessageCount:       uint64(len(topics[idx].Messages)),
				LastRecord:         topics[idx].LastRecord,
				SizeInBytes:        topics[idx].SizeInBytes,
				GuessedContentType: topics[idx].GuessedContentType,
			})
		}
	}
	return out
}

func (s *messageLog) Consume(ctx context.Context, shard uint64, consumer stream.Consumer, processor RecordProcessor) error {
	stream, err := s.shard.Stream(s.streamID)
	if err != nil {
		return err
	}
	r, err := s.shard.Reader(stream, shard)
	if err != nil {
		return err
	}
	return consumer.Consume(ctx, r, RecordDecoder(processor))
}

func RecordMatcher(patterns [][]byte, f RecordProcessor) RecordProcessor {
	return func(ctx context.Context, offset uint64, records []*api.Record) error {
		if len(patterns) > 0 {
			for idx, record := range records {
				for _, pattern := range patterns {
					if match(pattern, record.Topic) {
						err := f(ctx, offset+uint64(idx), []*api.Record{record})
						if err != nil {
							return err
						}
						break
					}
				}
			}
		} else {
			f(ctx, offset, records)
		}
		return nil
	}
}

// RecordDecoder returns a stream.Processor decoding api.Record and passing them to the provided callback function
func RecordDecoder(processor func(context.Context, uint64, []*api.Record) error) stream.Processor {
	return func(ctx context.Context, batch stream.Batch) error {
		if len(batch.Records) == 0 {
			return nil
		}
		records := make([]*api.Record, len(batch.Records))
		for idx, buf := range batch.Records {
			record := &api.Record{}
			err := proto.Unmarshal(buf.Payload(), record)
			if err != nil {
				return err
			}
			records[idx] = record
		}
		return processor(ctx, batch.Records[0].Offset(), records)
	}
}

func (s *messageLog) TopicsIterator(shard uint64, pattern []byte) stream.OffsetIterator {
	topics, ok := s.topics[shard]
	if !ok {
		return nil
	}
	return topics.Iterator(pattern)
}
