package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/nest/commitlog"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
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
	io.Closer
	SetCurrentStateOffset(v uint64)
	CurrentStateOffset() uint64
	Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error
	Dump(sink io.Writer, fromOffset, lastOffset uint64) error
	PutRecords(stateOffset uint64, b []*api.Record) error
	ListTopics(pattern []byte) []*api.TopicMetadata
	GetTopics(ctx context.Context, pattern []byte, processor RecordProcessor) error
	Consume(ctx context.Context, consumer stream.Consumer, processor RecordProcessor) error
}

type Snapshot struct {
	Remote         uint64 `json:"remote,omitempty"`
	MessagesOffset uint64 `json:"messages_offset,omitempty"`
	StateOffset    uint64 `json:"state_offset,omitempty"`
}

type messageLog struct {
	recorder Recorder
	topics   *topicAggregate
}

func NewMessageLog(ctx context.Context, recoder Recorder) (MessageLog, error) {
	s := &messageLog{
		recorder: recoder,
		topics:   &topicAggregate{topics: NewTopicState()},
	}
	go s.recorder.Consume(func(r io.ReadSeeker) error {
		consumer := stream.NewConsumer()
		return consumer.Consume(ctx, r, RecordDecoder(s.topics.Processor()))
	})
	return s, nil
}

func (s *messageLog) CurrentStateOffset() uint64 {
	return s.recorder.CurrentStateOffset()
}
func (s *messageLog) SetCurrentStateOffset(v uint64) {
	s.recorder.SetCurrentStateOffset(v)
}
func (s *messageLog) Close() error {
	return s.recorder.Close()
}

func (s *messageLog) Dump(sink io.Writer, fromOffset, lastOffset uint64) error {
	return s.recorder.Dump(sink, fromOffset, lastOffset)
}
func (s *messageLog) PutRecords(stateOffset uint64, b []*api.Record) error {
	payloads := make([][]byte, len(b))
	var err error
	for idx, record := range b {
		payloads[idx], err = proto.Marshal(record)
		if err != nil {
			return err
		}
	}
	return s.recorder.Append(stateOffset, payloads)
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

type DumpRecord struct {
	Offset  uint64 `json:"offset"`
	Payload []byte `json:"payload"`
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
	return s.recorder.Consume(func(r io.ReadSeeker) error {
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

func (s *messageLog) GetTopics(ctx context.Context, pattern []byte, processor RecordProcessor) error {
	return s.recorder.Consume(func(logReader io.ReadSeeker) error {
		consumer := stream.NewConsumer(stream.WithMaxBatchSize(10), stream.WithEOFBehaviour(stream.EOFBehaviourExit))
		r := commitlog.OffsetReader(s.topics.Get(pattern), logReader)
		return consumer.Consume(ctx, r, RecordDecoder(processor))
	})
}

func (s *messageLog) Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error {
	L(ctx).Debug("restoring snapshot")

	snapshotDescription := Snapshot{}
	err := json.Unmarshal(snapshot, &snapshotDescription)
	if err != nil {
		L(ctx).Debug("failed to decode state snapshot", zap.Error(err))
	} else {
		file, err := ioutil.TempFile("", "sst-incoming.*.nest")
		if err != nil {
			L(ctx).Fatal("failed to create tmp file to receive snapshot", zap.Error(err))
		}
		defer os.Remove(file.Name())
		defer file.Close()
		err = caller(snapshotDescription.Remote, func(c *grpc.ClientConn) error {
			stream, err := api.NewMessagesClient(c).SST(ctx, &api.SSTRequest{
				ToOffset:   snapshotDescription.MessagesOffset,
				FromOffset: s.recorder.Offset(),
			})
			if err != nil {
				return err
			}

			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_, err = file.Write(chunk.Chunk)
				if err != nil {
					return err
				}
			}
			return file.Sync()
		})
		if err != nil {
			L(ctx).Fatal("failed to receive snapshot", zap.Error(err))
		}
		return s.recorder.Restore(ctx, snapshotDescription, file)
	}
	return nil
}
