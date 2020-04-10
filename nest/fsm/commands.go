package fsm

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"

	api "github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/wasp/cluster/raft"
)

type State interface {
	PutRecords(int64, []*api.Record) error
}

func decode(payload []byte) ([]*StateTransition, error) {
	format := StateTransitionSet{}
	err := proto.Unmarshal(payload, &format)
	if err != nil {
		return nil, err
	}
	return format.Events, nil
}
func encode(events ...*StateTransition) ([]byte, error) {
	format := StateTransitionSet{
		Events: events,
	}
	return proto.Marshal(&format)
}

func NewFSM(id uint64, state State, commandsCh chan raft.Command) *FSM {
	return &FSM{id: id, state: state, commandsCh: commandsCh}
}

type FSM struct {
	id         uint64
	state      State
	commandsCh chan raft.Command
}

func (f *FSM) commit(ctx context.Context, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out := make(chan error)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.commandsCh <- raft.Command{Ctx: ctx, ErrCh: out, Payload: payload}:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-out:
			return err
		}
	}
}
func (f *FSM) Shutdown(ctx context.Context) error {
	payload, err := encode(&StateTransition{Event: &StateTransition_PeerLost{
		PeerLost: &PeerLost{
			Peer: f.id,
		},
	}})
	if err != nil {
		return err
	}
	return f.commit(ctx, payload)
}
func (f *FSM) PutRecords(ctx context.Context, records []*api.Record) error {
	for _, record := range records {
		if record.Timestamp == 0 {
			record.Timestamp = time.Now().UnixNano()
		}
	}
	payload, err := encode(&StateTransition{Event: &StateTransition_RecordsPut{
		RecordsPut: &RecordsPut{
			Timestamp: time.Now().UnixNano(),
			Records:   records,
		},
	}})
	if err != nil {
		return err
	}
	return f.commit(ctx, payload)
}

func (f *FSM) Apply(b []byte) error {
	events, err := decode(b)
	if err != nil {
		return err
	}
	for _, event := range events {
		switch event := event.GetEvent().(type) {
		case *StateTransition_RecordsPut:
			return f.state.PutRecords(event.RecordsPut.Timestamp, event.RecordsPut.Records)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
