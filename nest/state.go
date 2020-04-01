package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
)

type State interface {
	MarshalBinary() ([]byte, error)
	Load(buf []byte) error
	PutRecords(int64, []*api.Record) error
	GetRecords(ctx context.Context, topic []byte, fromTimestamp int64, f RecordConsumer) error
}

type state struct {
	log MessageLog
}

func NewState(log MessageLog) State {
	return &state{log: log}
}

type StateDump struct {
}

func (s *state) PutRecords(t int64, b []*api.Record) error {
	return s.log.Append(t, b)
}
func (s *state) GetRecords(ctx context.Context, topic []byte, fromTimestamp int64, f RecordConsumer) error {
	return s.log.GetRecords(ctx, topic, fromTimestamp, f)
}
func (s *state) Load(buf []byte) error {
	return nil
}
func (s *state) MarshalBinary() ([]byte, error) {
	return nil, nil
}
