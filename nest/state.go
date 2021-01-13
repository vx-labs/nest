package nest

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"sync"

	"github.com/asaskevich/EventBus"
	"github.com/golang/protobuf/proto"
	"github.com/vx-labs/nest/nest/api"
)

const (
	assignmentChangedEvent = "AssignmentChanged"
	streamChangedEvent     = "StreamChanged"
)

func decode(payload []byte) ([]*api.StateTransition, error) {
	format := api.StateTransitionSet{}
	err := proto.Unmarshal(payload, &format)
	if err != nil {
		return nil, err
	}
	return format.Events, nil
}

type ClusterState interface {
	GetLeader(streamID string, shardID uint64) (uint64, error)
	GetReplicaIDs(streamID string, shardID uint64) ([]uint64, error)
	GetStreams() []*api.Stream
	GetStream(streamID string) *api.Stream
	GetAssignedShards(peerID uint64) []*api.Shard
	AssignmentChanged(ctx context.Context, id uint64) chan struct{}
	StreamsChanged(ctx context.Context) chan struct{}
	ShardLeaderChanged(ctx context.Context, streamID string, shardID, leader uint64) chan struct{}
	GetShardForKey(streamID string, shardkey string) (*api.Shard, error)
}

type clusterState struct {
	mtx sync.RWMutex
	bus EventBus.Bus
	st  *api.State
}

func (s *clusterState) signalAssignmentChanged(shard *api.Shard) {
	s.bus.Publish(assignmentChangedEvent, shard)
}

func (s *clusterState) signalStreamsChanged(stream *api.Stream) {
	s.bus.Publish(streamChangedEvent, stream)
}

func newClusterState() *clusterState {
	return &clusterState{
		st:  &api.State{Configurations: make(map[string]*api.Stream)},
		bus: EventBus.New(),
	}
}
func (s *clusterState) Load(b []byte) error {
	st := &api.State{}
	err := proto.Unmarshal(b, st)
	if err == nil {
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.st = st
	}
	return err
}
func (s *clusterState) Snapshot() ([]byte, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return proto.Marshal(s.st)
}

func (s *clusterState) Apply(index uint64, b []byte) error {
	events, err := decode(b)
	if err != nil {
		return err
	}
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, event := range events {
		var err error
		switch event := event.GetEvent().(type) {
		case *api.StateTransition_StreamCreated:
			in := event.StreamCreated
			s.streamCreated(in.ID, in.Name, in.Shards, in.DesiredReplicaCount)
			if err == nil {
				s.signalStreamsChanged(s.st.Configurations[in.ID])
			}
		case *api.StateTransition_ShardAssigned:
			in := event.ShardAssigned
			err = s.shardAssigned(in.StreamID, in.ShardID, in.Peer)
			if err == nil {
				shard, err := s.getShard(in.StreamID, in.ShardID)
				if err == nil {
					s.signalAssignmentChanged(shard)
				}
			}
		case *api.StateTransition_ShardUnassigned:
			in := event.ShardUnassigned
			err = s.shardUnassigned(in.StreamID, in.ShardID, in.Peer)
			shard, err := s.getShard(in.StreamID, in.ShardID)
			if err == nil {
				s.signalAssignmentChanged(shard)
			}
		case *api.StateTransition_ShardLeaderElected:
			in := event.ShardLeaderElected
			err = s.shardLeaderElected(in.StreamID, in.ShardID, in.Peer)
			shard, err := s.getShard(in.StreamID, in.ShardID)
			if err == nil {
				s.signalAssignmentChanged(shard)
			}
		}
		if err != nil {
			log.Printf("failed to apply commit: %v", err)
			return err
		}
	}
	return nil
}
func (s *clusterState) ShardLeaderChanged(ctx context.Context, streamID string, shardID, leader uint64) chan struct{} {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	ch := make(chan struct{})
	out, _ := s.getLeader(streamID, shardID)
	if out != leader {
		close(ch)
		return ch
	}

	once := &sync.Once{}

	handler := func(shard *api.Shard) {
		if shard.ID == shardID && shard.Leader != leader {
			once.Do(func() {
				close(ch)
			})
		}
	}
	err := s.bus.SubscribeAsync(assignmentChangedEvent, handler, true)
	if err != nil {
		close(ch)
		return ch
	}

	go func() {
		<-ch
		s.bus.Unsubscribe(assignmentChangedEvent, handler)
	}()

	go func() {
		<-ctx.Done()
		once.Do(func() {
			close(ch)
		})
	}()
	return ch
}

func (s *clusterState) StreamsChanged(ctx context.Context) chan struct{} {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	ch := make(chan struct{})

	once := &sync.Once{}

	handler := func(shard *api.Stream) {
		once.Do(func() {
			close(ch)
		})
	}
	err := s.bus.SubscribeAsync(streamChangedEvent, handler, true)
	if err != nil {
		log.Print(err)
		close(ch)
		return ch
	}

	go func() {
		<-ch
		s.bus.Unsubscribe(streamChangedEvent, handler)
	}()

	go func() {
		<-ctx.Done()
		once.Do(func() {
			close(ch)
		})
	}()
	return ch
}
func (s *clusterState) AssignmentChanged(ctx context.Context, peer uint64) chan struct{} {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	ch := make(chan struct{})

	once := &sync.Once{}

	handler := func(shard *api.Shard) {
		for _, replica := range shard.Replicas {
			if replica == peer {
				once.Do(func() {
					close(ch)
				})
			}
		}
	}
	err := s.bus.SubscribeAsync(assignmentChangedEvent, handler, true)
	if err != nil {
		log.Print(err)
		close(ch)
		return ch
	}

	go func() {
		<-ch
		s.bus.Unsubscribe(assignmentChangedEvent, handler)
	}()

	go func() {
		<-ctx.Done()
		once.Do(func() {
			close(ch)
		})
	}()
	return ch
}

func (s *clusterState) streamCreated(streamID, name string, shardIDs []uint64, replicaCount int64) {
	if _, ok := s.st.Configurations[streamID]; ok {
		return
	}
	shards := make([]*api.Shard, len(shardIDs))
	for idx := range shards {
		shards[idx] = &api.Shard{
			StreamID: streamID,
			ID:       shardIDs[idx],
			Replicas: []uint64{},
		}
	}
	s.st.Configurations[streamID] = &api.Stream{
		ID:                  streamID,
		Name:                name,
		DesiredReplicaCount: replicaCount,
		Shards:              shards,
	}
}
func (s *clusterState) shardUnassigned(streamID string, shardID, peerID uint64) error {
	shard, err := s.getShard(streamID, shardID)
	if err != nil {
		return err
	}
	replicas := make([]uint64, 0)
	for _, replica := range shard.Replicas {
		if replica != peerID {
			replicas = append(replicas, replica)
		}
	}
	shard.Replicas = replicas
	return nil
}
func (s *clusterState) shardLeaderElected(streamID string, shardID, peerID uint64) error {
	shard, err := s.getShard(streamID, shardID)
	if err != nil {
		return err
	}
	shard.Leader = peerID
	return nil
}
func (s *clusterState) shardAssigned(streamID string, shardID, peerID uint64) error {
	shard, err := s.getShard(streamID, shardID)
	if err != nil {
		return err
	}
	shard.Replicas = append(shard.Replicas, peerID)
	return nil
}
func (s *clusterState) GetStreams() []*api.Stream {
	out := make([]*api.Stream, len(s.st.Configurations))
	idx := 0
	for k := range s.st.Configurations {
		out[idx] = s.st.Configurations[k]
		idx++
	}
	return out
}
func (s *clusterState) GetStream(streamID string) *api.Stream {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	if stream, ok := s.st.Configurations[streamID]; ok {
		return stream
	}
	for _, stream := range s.st.Configurations {
		if stream.Name == streamID {
			return stream
		}
	}
	return nil
}

func hashShardKey(key string, shardCount int) int {
	hash := fnv.New32()
	hash.Write([]byte(key))
	return int(hash.Sum32()) % shardCount
}

func (s *clusterState) GetShardForKey(streamID string, shardkey string) (*api.Shard, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	stream, ok := s.st.Configurations[streamID]
	if !ok {
		return nil, ErrStreamNotFound
	}
	idx := hashShardKey(shardkey, len(stream.Shards))
	return stream.Shards[idx], nil
}
func (s *clusterState) GetReplicaIDs(streamID string, shardID uint64) ([]uint64, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	shard, err := s.getShard(streamID, shardID)
	if err != nil {
		return nil, err
	}
	return shard.Replicas, nil
}
func (s *clusterState) GetLeader(streamID string, shardID uint64) (uint64, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.getLeader(streamID, shardID)
}
func (s *clusterState) getLeader(streamID string, shardID uint64) (uint64, error) {
	shard, err := s.getShard(streamID, shardID)
	if err != nil {
		return 0, err
	}
	if shard.Leader == 0 {
		return 0, errors.New("shard has no leader")
	}
	return shard.Leader, nil
}
func (s *clusterState) GetAssignedShards(peerID uint64) []*api.Shard {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	out := make([]*api.Shard, 0)
	for _, stream := range s.st.Configurations {
		for _, shard := range stream.Shards {
			for _, replica := range shard.Replicas {
				if replica == peerID {
					out = append(out, shard)
				}
			}
		}
	}
	return out
}

func (s *clusterState) getShard(streamID string, shardID uint64) (*api.Shard, error) {
	stream, ok := s.st.Configurations[streamID]
	if !ok {
		return nil, ErrStreamNotFound
	}
	for _, shard := range stream.Shards {
		if shard.ID == shardID {
			return shard, nil
		}
	}
	return nil, ErrShardNotFound
}
