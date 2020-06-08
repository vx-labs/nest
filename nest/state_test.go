package nest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vx-labs/nest/nest/fsm"
)

func emptyState() *state {
	return &state{
		shards: []*fsm.ShardMetadata{
			{ID: 0, Leader: 1, Replicas: []*fsm.ShardReplicas{{Peer: 1, Offset: 5}, {Peer: 2, Offset: 2}, {Peer: 3, Offset: 5}}},
			{ID: 1, Leader: 3, Replicas: []*fsm.ShardReplicas{{Peer: 1, Offset: 3}, {Peer: 2, Offset: 2}, {Peer: 3, Offset: 3}}},
			{ID: 2, Leader: 2, Replicas: []*fsm.ShardReplicas{{Peer: 1, Offset: 6}, {Peer: 2, Offset: 6}, {Peer: 3, Offset: 6}}},
		},
	}
}

func TestState_PeerJoined(t *testing.T) {
	s := emptyState()
	t.Run("should remove a lost peer from replica sets", func(t *testing.T) {
		require.NoError(t, s.PeerJoined(4))
		require.Equal(t, 4, len(s.shards[0].Replicas))
		require.Equal(t, uint64(1), s.shards[0].Replicas[0].Peer)
		require.Equal(t, uint64(2), s.shards[0].Replicas[1].Peer)
		require.Equal(t, uint64(3), s.shards[0].Replicas[2].Peer)
		require.Equal(t, uint64(4), s.shards[0].Replicas[3].Peer)

		require.Equal(t, 4, len(s.shards[1].Replicas))
		require.Equal(t, uint64(1), s.shards[1].Replicas[0].Peer)
		require.Equal(t, uint64(2), s.shards[1].Replicas[1].Peer)
		require.Equal(t, uint64(3), s.shards[1].Replicas[2].Peer)
		require.Equal(t, uint64(4), s.shards[1].Replicas[3].Peer)

		require.Equal(t, 4, len(s.shards[2].Replicas))
		require.Equal(t, uint64(1), s.shards[2].Replicas[0].Peer)
		require.Equal(t, uint64(2), s.shards[2].Replicas[1].Peer)
		require.Equal(t, uint64(3), s.shards[2].Replicas[2].Peer)
		require.Equal(t, uint64(4), s.shards[2].Replicas[3].Peer)

	})
}
func TestState_PeerLost(t *testing.T) {
	s := emptyState()
	t.Run("should remove a lost peer from replica sets", func(t *testing.T) {
		require.NoError(t, s.PeerLost(1))
		require.Equal(t, 2, len(s.shards[0].Replicas))
		require.Equal(t, uint64(2), s.shards[0].Replicas[0].Peer)
		require.Equal(t, uint64(3), s.shards[0].Replicas[1].Peer)

		require.Equal(t, 2, len(s.shards[1].Replicas))
		require.Equal(t, uint64(2), s.shards[1].Replicas[0].Peer)
		require.Equal(t, uint64(3), s.shards[1].Replicas[1].Peer)

		require.Equal(t, 2, len(s.shards[2].Replicas))
		require.Equal(t, uint64(2), s.shards[1].Replicas[0].Peer)
		require.Equal(t, uint64(3), s.shards[1].Replicas[1].Peer)
	})
}
func TestState_ShardReplicaProgressed(t *testing.T) {
	s := emptyState()
	t.Run("should update offset of a replica of an existing shard", func(t *testing.T) {
		require.NoError(t, s.ShardReplicaProgressed(0, 2, 5))
		require.Equal(t, uint64(5), s.shards[0].Replicas[1].Offset)
	})
	t.Run("should update offset of an unknown replica of an existing shard", func(t *testing.T) {
		require.NoError(t, s.ShardReplicaProgressed(0, 4, 5))
		require.Equal(t, uint64(5), s.shards[0].Replicas[3].Offset)
	})
}
func TestState_ShardAssigned(t *testing.T) {
	s := emptyState()
	t.Run("should update leader of an existing shard", func(t *testing.T) {
		require.NoError(t, s.ShardLeadershipAssigned(0, 2))
		require.Equal(t, uint64(2), s.shards[0].Leader)
	})
	t.Run("should add an unknown leader of an existing shard", func(t *testing.T) {
		require.NoError(t, s.ShardLeadershipAssigned(0, 4))
		require.Equal(t, uint64(4), s.shards[0].Leader)
	})
}
