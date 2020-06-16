package nest

import (
	"encoding/json"

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
