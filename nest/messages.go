package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
	"go.uber.org/zap"
)

var (
	messagesBucketName []byte = []byte("messages")
	appliedIndexKey    []byte = []byte("_index")
	encoding                  = binary.BigEndian
)

type RecordConsumer func(topic []byte, ts int64, payload []byte) error
type MessageLog interface {
	io.Closer
	PutRecords(b []*api.Record) error
	GetRecords(ctx context.Context, shard int, patterns [][]byte, fromOffset uint64, f RecordConsumer) (uint64, error)
	Shards() int
}

type messageLog struct {
	offset uint64
	stream stream.Stream
}

type compatLogger struct {
	l *zap.Logger
}

func (c *compatLogger) Debugf(string, ...interface{})   {}
func (c *compatLogger) Infof(string, ...interface{})    {}
func (c *compatLogger) Warningf(string, ...interface{}) {}
func (c *compatLogger) Errorf(string, ...interface{})   {}

func NewMessageLog(ctx context.Context, datadir string) (MessageLog, error) {
	s, err := stream.Open("messages", datadir)
	if err != nil {
		return nil, err
	}
	return &messageLog{
		stream: s,
	}, nil
}

func (s *messageLog) Close() error {
	return s.stream.Close()
}
func (s *messageLog) Shards() int {
	return s.stream.Shards()
}
func (s *messageLog) PutRecords(b []*api.Record) error {
	for idx := range b {
		err := json.NewEncoder(s.stream.Writer(b[idx].Topic)).Encode(b[idx])
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

func (s *messageLog) GetRecords(ctx context.Context, shard int, patterns [][]byte, fromOffset uint64, f RecordConsumer) (uint64, error) {

	r, err := s.stream.Reader(shard, fromOffset)
	if err != nil {
		return fromOffset, err
	}
	decoder := json.NewDecoder(r)
	entry := api.Record{}
	var idx uint64
	for idx = 0; idx >= 0; idx++ {
		err := decoder.Decode(&entry)
		if err == io.EOF {
			return fromOffset + idx, nil
		}
		if err != nil {
			return fromOffset + idx, err
		}
		if len(patterns) > 0 {
			for _, pattern := range patterns {
				if match(pattern, entry.Topic) {
					err = f(entry.Topic, entry.Timestamp, entry.Payload)
					if err != nil {
						return fromOffset + idx, err
					}
				}
			}
		} else {
			err = f(entry.Topic, entry.Timestamp, entry.Payload)
			if err != nil {
				return fromOffset + idx, err
			}

		}
	}
	return fromOffset, err
}
