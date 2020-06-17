package nest

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/wasp/topics"
)

type Topic struct {
	Name               []byte
	Messages           []uint64
	SizeInBytes        uint64
	LastRecord         *api.Record
	GuessedContentType string
}

func encodeTopic(t Topic) []byte {
	buf, err := json.Marshal(t)
	if err != nil {
		panic(err)
	}
	return buf
}
func decodeTopic(buf []byte) Topic {
	t := Topic{}
	err := json.Unmarshal(buf, &t)
	if err != nil {
		panic(err)
	}
	return t
}

func NewTopicState() *topicsState {
	return &topicsState{store: topics.NewTree()}
}

type topicsState struct {
	store topics.Store
}

func (t *topicsState) Match(pattern []byte) []Topic {
	bufs := [][]byte{}
	err := t.store.Match(pattern, &bufs)
	if err != nil {
		return nil
	}
	out := make([]Topic, len(bufs))
	for idx := range out {
		out[idx] = decodeTopic(bufs[idx])
	}
	return out
}

func (t *topicsState) Set(topic Topic) error {
	return t.store.Insert(topic.Name, encodeTopic(topic))
}

type topicAggregate struct {
	topics *topicsState
}

func (s *topicAggregate) Processor() RecordProcessor {
	return func(ctx context.Context, firstOffset uint64, records []*api.Record) error {

		topicValues := map[string]*Topic{}
		for idx, record := range records {
			offset := firstOffset + uint64(idx)

			v, ok := topicValues[string(record.Topic)]
			if !ok {
				topicValues[string(record.Topic)] = &Topic{
					Name:               record.Topic,
					Messages:           []uint64{offset},
					SizeInBytes:        uint64(len(record.Payload)),
					LastRecord:         record,
					GuessedContentType: http.DetectContentType(record.Payload),
				}
			} else {
				v.Messages = append(v.Messages, offset)
				v.SizeInBytes += uint64(len(record.Payload))
				v.LastRecord = record
				contentType := http.DetectContentType(record.Payload)
				if v.GuessedContentType != contentType {
					v.GuessedContentType = "application/octet-stream"
				}
			}
		}
		for _, t := range topicValues {
			v := s.topics.Match(t.Name)
			if len(v) == 0 {
				s.topics.Set(*t)
			} else {
				v[0].Messages = append(v[0].Messages, t.Messages...)
				t.Messages = v[0].Messages
				t.SizeInBytes += v[0].SizeInBytes
				if t.LastRecord == nil {
					t.LastRecord = v[0].LastRecord
				}
				if t.GuessedContentType != v[0].GuessedContentType {
					t.GuessedContentType = "application/octet-stream"
				}
				s.topics.Set(*t)
			}
		}
		return nil
	}
}

func (s *topicAggregate) List(pattern []byte) []*api.TopicMetadata {
	if len(pattern) == 0 {
		pattern = []byte("#")
	}
	topics := s.topics.Match(pattern)
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

func (s *topicAggregate) Get(pattern []byte) []uint64 {
	topics := s.topics.Match(pattern)
	offsets := []uint64{}
	for _, topic := range topics {
		offsets = append(offsets, topic.Messages...)
	}
	return offsets
}
