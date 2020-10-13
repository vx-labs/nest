package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/commitlog/stream"
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
	LookupTimestamp(ts uint64) uint64
	PutRecords(ctx context.Context, b []*api.Record) error
	ListTopics(pattern []byte) []*api.TopicMetadata
	TopicsIterator(pattern []byte) stream.OffsetIterator
	Consume(ctx context.Context, consumer stream.Consumer, processor RecordProcessor) error
}

type Snapshot struct {
	Remote         uint64 `json:"remote,omitempty"`
	MessagesOffset uint64 `json:"messages_offset,omitempty"`
	StateOffset    uint64 `json:"state_offset,omitempty"`
}

type messageLog struct {
	shard  Shard
	logger *zap.Logger
	topics *topicAggregate
}

func NewMessageLog(ctx context.Context, shard Shard, logger *zap.Logger) (MessageLog, error) {
	s := &messageLog{
		shard:  shard,
		logger: logger,
		topics: &topicAggregate{topics: NewTopicState()},
	}

	return s, nil
}

func (s *messageLog) StartConsumers(ctx context.Context) {
	go func() {
		for {
			consumer := stream.NewConsumer(
				stream.WithName("topics_indexer"),
				stream.WithPerformanceLogging(s.shard, s.logger.With(
					zap.String("stream_name", "messages"),
					zap.Uint64("shard_id", 0),
				)),
			)
			err := s.Consume(ctx, consumer, s.topics.Processor())
			if err != nil && err != context.Canceled {
				s.logger.Error("topics indexer failed to run", zap.Error(err))
				<-time.After(1 * time.Second)
			}
		}
	}()
}
func (s *messageLog) LookupTimestamp(ts uint64) uint64 {
	return s.shard.LookupTimestamp(ts)
}
func (s *messageLog) Dump(sink io.Writer, fromOffset uint64) error {
	return s.shard.Dump(sink, fromOffset)
}
func (s *messageLog) PutRecords(ctx context.Context, b []*api.Record) error {
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
	topics := s.topics.topics.Match(pattern)
	out := make([]*api.TopicMetadata, len(topics))
	for idx := range out {
		out[idx] = &api.TopicMetadata{
			Name:               topics[idx].Name,
			MessageCount:       uint64(len(topics[idx].Messages)),
			LastRecord:         topics[idx].LastRecord,
			SizeInBytes:        topics[idx].SizeInBytes,
			GuessedContentType: topics[idx].GuessedContentType,
		}
	}
	return out
}

func (s *messageLog) Consume(ctx context.Context, consumer stream.Consumer, processor RecordProcessor) error {
	return s.shard.Consume(func(r io.ReadSeeker) error {
		return consumer.Consume(ctx, r, RecordDecoder(processor))
	})
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
		records := make([]*api.Record, len(batch.Records))
		for idx, buf := range batch.Records {
			record := &api.Record{}
			err := proto.Unmarshal(buf, record)
			if err != nil {
				return err
			}
			records[idx] = record
		}
		return processor(ctx, batch.FirstOffset, records)
	}
}

func (s *messageLog) TopicsIterator(pattern []byte) stream.OffsetIterator {
	return s.topics.Iterator(pattern)
}
