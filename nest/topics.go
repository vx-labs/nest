package nest

import (
	"encoding/json"
	"errors"

	"github.com/vx-labs/wasp/topics"
)

type Topic struct {
	Name     []byte
	Messages []uint64
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
func (t *topicsState) Insert(name []byte, offset uint64) error {
	out := [][]byte{}
	err := t.store.Match(name, &out)
	if err != nil {
		return err
	}
	if len(out) > 1 {
		return errors.New("invalid topic provided")
	}
	if len(out) == 0 {
		return t.store.Insert(name, encodeTopic(Topic{
			Name:     name,
			Messages: []uint64{offset},
		}))
	}
	topic := decodeTopic(out[0])
	topic.Messages = append(topic.Messages, offset)
	return t.store.Insert(name, encodeTopic(topic))
}
func (t *topicsState) Set(name []byte, offsets []uint64) error {
	out := [][]byte{}
	err := t.store.Match(name, &out)
	if err != nil {
		return err
	}
	if len(out) > 1 {
		return errors.New("invalid topic provided")
	}
	if len(out) == 0 {
		return t.store.Insert(name, encodeTopic(Topic{
			Name:     name,
			Messages: offsets,
		}))
	}
	topic := decodeTopic(out[0])
	topic.Messages = offsets
	return t.store.Insert(name, encodeTopic(topic))
}
