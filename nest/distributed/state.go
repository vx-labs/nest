package distributed

import (
	"context"
	"errors"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/vx-labs/nest/nest/api"
)

var (
	clock = func() int64 {
		return time.Now().UnixNano()
	}
)

var (
	ErrInvalidPayload = errors.New("invalid payload")
)

type Channel interface {
	Events() chan []byte
	Broadcast(b []byte)
	BroadcastFullState(b []byte)
}

type ShardReplicaState interface {
	Get(shard uint64) map[uint64]uint64
	GetPeerProgress(shard, peer uint64) uint64
	Set(shard uint64, progress, latest uint64) error
	Watermark(peers []uint64, shard uint64) uint64
	OnPeerProgressed(ctx context.Context, peer, shard, from uint64) chan struct{}
	All() *api.ShardReplicaState
}
type State interface {
	ShardReplicas() ShardReplicaState
	Distributor() memberlist.Delegate
}

type state struct {
	bcast              *memberlist.TransmitLimitedQueue
	shardsReplicaState *shardsReplicaState
}

func NewState(peer uint64, bcast *memberlist.TransmitLimitedQueue) State {
	return &state{
		bcast:              bcast,
		shardsReplicaState: newshardsReplicaState(peer, bcast),
	}
}

func (s *state) Distributor() memberlist.Delegate { return s }
func (s *state) ShardReplicas() ShardReplicaState { return s.shardsReplicaState }
func (s *state) GetBroadcasts(overhead, limit int) [][]byte {
	return s.bcast.GetBroadcasts(overhead, limit)
}
func (s *state) MergeRemoteState(buf []byte, join bool) {
	payload := &api.ShardReplicaState{}
	err := proto.Unmarshal(buf, payload)
	if err != nil {
		return
	}
	s.shardsReplicaState.mergeSessions(payload)
}
func (s *state) NotifyMsg(b []byte) {
	s.MergeRemoteState(b, false)
}
func (s *state) NodeMeta(limit int) []byte { return nil }
func (s *state) LocalState(join bool) []byte {
	return s.shardsReplicaState.dump()
}
