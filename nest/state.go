package nest

import (
	"errors"
	"sort"
	"sync"

	"github.com/vx-labs/nest/nest/fsm"
)

type state struct {
	mtx           sync.RWMutex
	shards        []*fsm.ShardMetadata
	clusterLeader bool
}

func NewState() *state {
	return &state{
		shards: []*fsm.ShardMetadata{
			{ID: 0},
			{ID: 1},
			{ID: 2},
		},
	}
}

func (s *state) SetLeader() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.clusterLeader = true
}
func (s *state) UnsetLeader() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.clusterLeader = false
}
func (s *state) PeerLost(peer uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	shardLen := len(s.shards)
	for _, shard := range s.shards {
		for idx, replica := range shard.Replicas {
			if replica.Peer == peer {
				if len(shard.Replicas) == 1 {
					shard.Replicas = []*fsm.ShardReplicas{}
					break
				}
				shard.Replicas[idx] = shard.Replicas[shardLen-1]
				shard.Replicas = shard.Replicas[:shardLen-1]
			}
		}
		sort.Slice(shard.Replicas, func(i, j int) bool { return shard.Replicas[i].Peer < shard.Replicas[j].Peer })
	}
	return nil
}
func (s *state) PeerJoined(peer uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, shard := range s.shards {
		found := false
		for _, replica := range shard.Replicas {
			if replica.Peer == peer {
				found = true
			}
		}
		if !found {
			shard.Replicas = append(shard.Replicas, &fsm.ShardReplicas{Peer: peer, Offset: 0})
		}
	}
	return nil
}
func (s *state) ShardReplicaProgressed(id, peer, offset uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, shard := range s.shards {
		if shard.ID == id {
			for _, replica := range shard.Replicas {
				if replica.Peer == peer {
					replica.Offset = offset
					return nil
				}
			}
			shard.Replicas = append(shard.Replicas, &fsm.ShardReplicas{Peer: peer, Offset: offset})
			return nil
		}
	}
	return errors.New("unknown shard")
}
func (s *state) ShardLeadershipAssigned(id, newLeader uint64) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, shard := range s.shards {
		if shard.ID == id {
			for _, replica := range shard.Replicas {
				if replica.Peer == newLeader {
					shard.Leader = newLeader
					return nil
				}
			}
			shard.Replicas = append(shard.Replicas, &fsm.ShardReplicas{Peer: newLeader, Offset: 0})
			shard.Leader = newLeader
			return nil
		}
	}
	return errors.New("unknown shard")
}
