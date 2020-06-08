package fsm

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/vx-labs/wasp/cluster/raft"
)

type State interface {
	ShardReplicaProgressed(id, peer, offset uint64) error
	ShardLeadershipAssigned(id, newLeader uint64) error
	PeerLost(peer uint64) error
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

func (f *FSM) Apply(index uint64, b []byte) error {
	events, err := decode(b)
	if err != nil {
		return err
	}
	for _, event := range events {
		switch event := event.GetEvent().(type) {
		case *StateTransition_PeerLost:
			input := event.PeerLost
			return f.state.PeerLost(input.Peer)
		case *StateTransition_ShardLeadershipAssigned:
			input := event.ShardLeadershipAssigned
			return f.state.ShardLeadershipAssigned(input.ID, input.NewLeader)
		case *StateTransition_ShardReplicaProgressed:
			input := event.ShardReplicaProgressed
			return f.state.ShardReplicaProgressed(input.ID, input.Peer, input.Offset)
		}
	}
	return nil
}
