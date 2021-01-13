package distributed

import (
	"context"
	"sort"
	"sync"

	"github.com/vx-labs/wasp/v4/crdt"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/memberlist"
	"github.com/vx-labs/nest/nest/api"
)

type shardsReplicaState struct {
	mu             sync.RWMutex
	peer           uint64
	shards         *api.ShardReplicaState
	bcast          *memberlist.TransmitLimitedQueue
	peerProgressed chan struct{}
}

func newshardsReplicaState(peer uint64, bcast *memberlist.TransmitLimitedQueue) *shardsReplicaState {
	return &shardsReplicaState{
		shards:         &api.ShardReplicaState{Peers: make(map[uint64]*api.PeerShardState)},
		peer:           peer,
		bcast:          bcast,
		peerProgressed: make(chan struct{}),
	}
}
func (s *shardsReplicaState) mergeSessions(sessions *api.ShardReplicaState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed := false
	for peer, peerConfig := range sessions.Peers {
		_, ok := s.shards.Peers[peer]
		if !ok {
			s.shards.Peers[peer] = peerConfig
			if !changed {
				changed = true
			}
		} else {
			for shard, shardConfig := range peerConfig.Shards {
				_, ok := s.shards.Peers[peer].Shards[shard]
				if !ok || crdt.IsEntryOutdated(s.shards.Peers[peer].Shards[shard], shardConfig) {
					s.shards.Peers[peer].Shards[shard] = shardConfig
					if !changed {
						changed = true
					}
				}
			}
		}
	}
	if changed {
		s.signalProgress()
	}
	return nil
}
func (s *shardsReplicaState) dump() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, _ := proto.Marshal(s.shards)
	return b
}

func (s *shardsReplicaState) signalProgress() {
	old := s.peerProgressed
	s.peerProgressed = make(chan struct{})
	close(old)
}
func (s *shardsReplicaState) OnPeerProgressed(ctx context.Context, peer, shard, from uint64) chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		if from < s.GetPeerProgress(shard, peer) {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.peerProgressed:
				if from < s.GetPeerProgress(shard, peer) {
					return
				}
			}
		}
	}()
	return ch
}
func (s *shardsReplicaState) DeletePeer(peer uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := clock()
	if peerConfig, ok := s.shards.Peers[s.peer]; ok {
		for _, shard := range peerConfig.Shards {
			shard.LastDeleted = now
		}
	}

	b, _ := proto.Marshal(s.shards)
	s.bcast.QueueBroadcast(simpleFullStateBroadcast(b))
	s.signalProgress()
	return nil
}
func (s *shardsReplicaState) Set(shard uint64, progress, latest uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.shards.Peers[s.peer]; !ok {
		s.shards.Peers[s.peer] = &api.PeerShardState{
			Shards: map[uint64]*api.ShardReplica{
				shard: {
					Committed:    progress,
					LastAdded:    clock(),
					Peer:         s.peer,
					LatestRecord: latest,
				},
			},
		}
	} else {
		if _, ok := s.shards.Peers[s.peer].Shards[shard]; !ok {
			s.shards.Peers[s.peer].Shards[shard] = &api.ShardReplica{
				Committed: progress,
				LastAdded: clock(),
				Peer:      s.peer,
			}
		} else {
			s.shards.Peers[s.peer].Shards[shard].Committed = progress
			s.shards.Peers[s.peer].Shards[shard].LastAdded = clock()
		}
	}

	b, _ := proto.Marshal(s.shards)
	s.bcast.QueueBroadcast(simpleFullStateBroadcast(b))
	s.signalProgress()
	return nil
}

func (s *shardsReplicaState) Watermark(peers []uint64, shard uint64) uint64 {
	s.mu.RLock()
	copy := s.clone()
	s.mu.RUnlock()

	set := []*api.ShardReplica{}
	offsets := []uint64{}

	for id, peerConfig := range copy.Peers {
		for _, peer := range peers {
			if id == peer && crdt.IsEntryAdded(peerConfig.Shards[shard]) {
				set = append(set, peerConfig.Shards[shard])
			}
		}
	}
	if len(set) == 0 {
		return 0
	}
	sort.SliceStable(set, func(i, j int) bool { return set[i].LatestRecord < set[j].LatestRecord })

	thresold := set[len(set)/2].LatestRecord

	for _, shardConfig := range set {

		if thresold > shardConfig.LatestRecord && thresold-shardConfig.LatestRecord > 5*1000*1000*1000 {
			//ignore if peer is later than thresold + 5s
			continue
		}
		offsets = append(offsets, shardConfig.Committed)
	}
	if len(offsets) == 0 {
		return 0
	}
	sort.SliceStable(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets[0]
}
func (s *shardsReplicaState) GetPeerProgress(shard, peer uint64) uint64 {
	s.mu.RLock()
	copy := s.clone()
	s.mu.RUnlock()
	peerConfig, ok := copy.Peers[peer]
	if !ok {
		return 0
	}
	shardConfig, ok := peerConfig.Shards[shard]
	if !ok {
		return 0
	}
	if crdt.IsEntryRemoved(shardConfig) {
		return 0
	}
	return shardConfig.Committed
}

func (s *shardsReplicaState) clone() *api.ShardReplicaState {
	return proto.Clone(s.shards).(*api.ShardReplicaState)
}
func (s *shardsReplicaState) All() *api.ShardReplicaState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.clone()
}
func (s *shardsReplicaState) Get(shard uint64) map[uint64]uint64 {
	s.mu.RLock()
	copy := s.clone()
	s.mu.RUnlock()
	out := map[uint64]uint64{}
	for peer, shards := range copy.Peers {
		if shardConfig, ok := shards.Shards[shard]; ok && crdt.IsEntryAdded(shardConfig) {
			out[peer] = shardConfig.Committed
		}
	}
	return out
}
