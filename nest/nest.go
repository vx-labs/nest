package nest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/gogo/protobuf/proto"
	"github.com/vx-labs/cluster"
	"github.com/vx-labs/cluster/raft"
	"github.com/vx-labs/commitlog"
	"github.com/vx-labs/commitlog/stream"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/nest/distributed"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"google.golang.org/grpc"
)

var (
	ErrStreamNotFound     = errors.New("stream not found")
	ErrShardNotFound      = errors.New("shard not found")
	ErrShardAlreadyExists = errors.New("shard already exists")
	ErrPeerNotLeader      = errors.New("peer is not leader")
)

type Caller interface {
	Call(id uint64, f func(*grpc.ClientConn) error) error
}

type ShardsController interface {
	api.NestClusterServer
	Run(ctx context.Context)
	WaitReady(ctx context.Context)
	Serve(grpcServer *grpc.Server)
	CreateStream(ctx context.Context, name string, replicaCount int64, shardCount int64) (string, error)
	ListStreams(ctx context.Context, in *api.ListStreamsInput) (*api.ListStreamsOutput, error)
	Stream(name string) (string, error)
	ListShards(streamID string) ([]uint64, error)
	Write(ctx context.Context, stream string, shardKey string, payload []byte) error
	ResolveTimestamp(streamID string, shardID uint64, timestamp uint64) (uint64, error)
	Reader(streamID string, shardID uint64) (io.ReadSeeker, error)
	GetReplicaID(streamID string, shardID uint64) (*api.Stream, uint64, error)
	StopCluster(ctx context.Context) error
	Close() error
}

type userFrontend struct {
	id     uint64
	caller Caller
	shards ShardsController
}

func NewUserFrontend(id uint64, caller Caller, shards ShardsController) *userFrontend {
	return &userFrontend{shards: shards, id: id, caller: caller}
}
func (g *userFrontend) Serve(grpcServer *grpc.Server) {
	api.RegisterNestServer(grpcServer, g)
}

func (g *userFrontend) GetEntries(in *api.GetEntriesInput, client api.Nest_GetEntriesServer) error {
	s, replica, err := g.shards.GetReplicaID(in.StreamID, in.ShardID)
	if err != nil {
		return err
	}
	if replica != g.id {
		return g.caller.Call(replica, func(g *grpc.ClientConn) error {
			s, err := api.NewNestClient(g).GetEntries(client.Context(), in)
			if err != nil {
				return err
			}
			for {
				out, err := s.Recv()
				if err != nil {
					return err
				}
				err = client.Send(out)
				if err != nil {
					return err
				}
			}
		})
	}
	cursor, err := g.shards.Reader(s.ID, in.ShardID)
	if err != nil {
		return err
	}
	consumer := stream.NewConsumer(
		stream.FromOffset(int64(in.FromOffset)),
		stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
		stream.WithMaxRecordCount(int64(in.MaxTotalRecordsCount)),
		stream.WithMaxBatchSize(int(in.MaxBatchSize)),
	)

	return consumer.Consume(client.Context(), cursor, func(_ context.Context, b stream.Batch) error {
		if len(b.Records) == 0 {
			return nil
		}
		out := &api.GetEntriesOutput{
			StreamID: s.ID,
			ShardID:  in.ShardID,
			Entries:  make([]*api.Entry, len(b.Records)),
		}
		for idx := range b.Records {
			out.Entries[idx] = &api.Entry{
				Offset:    b.Records[idx].Offset(),
				Payload:   b.Records[idx].Payload(),
				Timestamp: b.Records[idx].Timestamp(),
			}
		}
		return client.Send(out)
	})
}
func (g *userFrontend) PutEntry(ctx context.Context, in *api.PutEntryInput) (*api.PutEntryOutput, error) {
	return &api.PutEntryOutput{}, g.shards.Write(ctx, in.StreamID, in.ShardKey, in.Payload)
}
func (g *userFrontend) ListStreams(ctx context.Context, in *api.ListStreamsInput) (*api.ListStreamsOutput, error) {
	return g.shards.ListStreams(ctx, in)
}

func (g *userFrontend) CreateStream(ctx context.Context, in *api.CreateStreamInput) (*api.CreateStreamOutput, error) {
	id, err := g.shards.CreateStream(ctx, in.Name, in.DesiredReplicaCount, in.ShardCount)
	if err != nil {
		return nil, err
	}
	return &api.CreateStreamOutput{
		ID: id,
	}, nil
}

type shardsController struct {
	id              uint64
	datadir         string
	node            cluster.Node
	topologyChanged chan struct{}
	state           ClusterState
	dstate          distributed.State
	caller          Caller
	shards          map[string]map[uint64]*assignedShard
	mtx             sync.RWMutex
	eventMtx        sync.Mutex
	writeMtx        sync.Mutex
	rand            *rand.Rand
}

func NewShardsController(id uint64, datadir string, mnode cluster.MultiNode, raftConfig cluster.RaftConfig, dstate distributed.State) ShardsController {
	state := newClusterState()
	server := &shardsController{
		id:              id,
		datadir:         datadir,
		caller:          mnode,
		topologyChanged: make(chan struct{}),
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
		shards:          make(map[string]map[uint64]*assignedShard),
		state:           state,
		dstate:          dstate,
	}
	raftConfig.CommitApplier = func(ctx context.Context, commit raft.Commit) error {
		return state.Apply(commit.Index, commit.Payload)
	}
	raftConfig.SnapshotApplier = func(ctx context.Context, index uint64, snapshot *snap.Snapshotter) error {
		value, err := snapshot.Load()
		if err != nil {
			return err
		}
		return state.Load(value.Data)
	}
	raftConfig.GetStateSnapshot = state.Snapshot
	raftConfig.OnNodeRemoved = func(id uint64, leader bool) {
		if leader {
			server.signalTopologyChanged()
		}
	}
	raftConfig.OnNodeAdded = func(id uint64, leader bool) {
		if leader {
			server.signalTopologyChanged()
		}
	}
	raftConfig.LeaderFunc = server.leaderFunc

	node := mnode.Node("nest", raftConfig)
	server.node = node
	return server
}

type loadedMember struct {
	id     uint64
	shards []uint64
}
type loadedMembers []*loadedMember

func (l loadedMembers) Add(id, shard uint64) {
	for _, member := range l {
		if id == member.id {
			member.shards = append(member.shards, shard)
			return
		}
	}
}
func (l loadedMembers) Del(id, shard uint64) {
	for _, member := range l {
		if id == member.id {
			member.shards = make([]uint64, 0)
			for _, s := range member.shards {
				if s != shard {
					member.shards = append(member.shards, s)
				}
			}
			return
		}
	}
}

func (l loadedMembers) Len() int { return len(l) }
func (l loadedMembers) count(id uint64) int {
	for _, m := range l {
		if m.id == id {
			return len(m.shards)
		}
	}
	return 0
}
func (l loadedMembers) Less(i, j int) bool { return len(l[i].shards) < len(l[j].shards) }
func (l loadedMembers) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l loadedMembers) filter(f func(*loadedMember) bool) loadedMembers {
	out := loadedMembers{}
	for idx := range l {
		m := l[idx]
		if f(m) {
			out = append(out, m)
		}
	}
	return out
}
func (g *shardsController) hasValidLeader(shard *api.Shard) bool {
	if shard.Leader != 0 {
		for _, replica := range shard.Replicas {
			if shard.Leader == replica {
				return true
			}
		}
	}
	return false
}
func (g *shardsController) balanceShardLeader(streams []*api.Stream) []*api.StateTransition {
	out := []*api.StateTransition{}

	for idx := range streams {
		stream := streams[idx]
		for idx := range stream.Shards {
			shard := stream.Shards[idx]
			if len(shard.Replicas) == 0 || g.hasValidLeader(shard) {
				continue
			}
			sort.SliceStable(shard.Replicas, func(i, j int) bool {
				return g.dstate.ShardReplicas().GetPeerProgress(shard.ID, shard.Replicas[i]) > g.dstate.ShardReplicas().GetPeerProgress(shard.ID, shard.Replicas[j])
			})
			leader := shard.Replicas[0]
			log.Printf("promoted %d as shard %d leader", leader, shard.ID)
			out = append(out, &api.StateTransition{
				Event: &api.StateTransition_ShardLeaderElected{
					ShardLeaderElected: &api.ShardLeaderElected{
						StreamID: shard.StreamID,
						ShardID:  shard.ID,
						Peer:     leader,
					},
				},
			})
		}
	}
	return out
}

func (g *shardsController) cleanupMembers(members []uint64, streams []*api.Stream) []*api.StateTransition {
	out := []*api.StateTransition{}
	for idx := range streams {
		stream := streams[idx]
		for _, shard := range stream.Shards {
			for _, replica := range shard.Replicas {
				found := false
				for _, m := range members {
					if replica == m {
						found = true
					}
				}
				if !found {
					log.Printf("removing member")
					out = append(out, &api.StateTransition{
						Event: &api.StateTransition_ShardUnassigned{
							ShardUnassigned: &api.ShardUnassigned{
								StreamID: shard.StreamID,
								Peer:     replica,
								ShardID:  shard.ID,
							},
						},
					})
				}
			}
		}
	}
	return out
}
func (g *shardsController) balanceShards(members []uint64, streams []*api.Stream) []*api.StateTransition {
	out := []*api.StateTransition{}
	replicaNeeded := []*api.Shard{}
	tooManyReplicas := []*api.Shard{}
	load := loadedMembers{}
	for _, m := range members {
		load = append(load, &loadedMember{id: m})
	}
	for idx := range streams {
		stream := streams[idx]
		for _, shard := range stream.Shards {
			for _, replica := range shard.Replicas {
				load.Add(replica, shard.ID)
			}
			if len(shard.Replicas) < int(stream.DesiredReplicaCount) {
				replicaNeeded = append(replicaNeeded, shard)
			} else if len(shard.Replicas) > int(stream.DesiredReplicaCount) {
				tooManyReplicas = append(tooManyReplicas, shard)
			}
		}
	}
	sort.Stable(load)
	for _, shard := range tooManyReplicas {
		sort.SliceStable(shard.Replicas, func(i, j int) bool {
			return load.count(shard.Replicas[i]) > load.count(shard.Replicas[j])
		})
		var desired int
		for _, stream := range streams {
			if stream.ID == shard.StreamID {
				desired = int(stream.DesiredReplicaCount)
			}
		}
		delta := len(shard.Replicas) - desired
		toRemove := shard.Replicas[0:delta]

		for idx := range toRemove {
			out = append(out, &api.StateTransition{
				Event: &api.StateTransition_ShardUnassigned{
					ShardUnassigned: &api.ShardUnassigned{
						StreamID: shard.StreamID,
						Peer:     toRemove[idx],
						ShardID:  shard.ID,
					},
				},
			})
			load.Del(toRemove[idx], shard.ID)
			sort.Stable(load)
		}
	}
	for idx := range replicaNeeded {
		shard := replicaNeeded[idx]
		var desired int
		for _, stream := range streams {
			if stream.ID == shard.StreamID {
				desired = int(stream.DesiredReplicaCount)
			}
		}
		delta := desired - len(shard.Replicas)
		for delta > 0 {
			candidates := load.filter(func(lm *loadedMember) bool {
				for idx := range lm.shards {
					if shard.ID == lm.shards[idx] {
						return false
					}
				}
				return true
			})
			log.Printf("replica needed on shard %d: current_count=%d, candidates=%d", shard.ID, len(shard.Replicas), len(candidates))
			if len(candidates) == 0 {
				break
			}
			out = append(out, &api.StateTransition{
				Event: &api.StateTransition_ShardAssigned{
					ShardAssigned: &api.ShardAssigned{
						StreamID: shard.StreamID,
						Peer:     candidates[0].id,
						ShardID:  shard.ID,
					},
				},
			})
			load.Add(candidates[0].id, shard.ID)
			sort.Stable(load)
			delta--
		}
	}
	return out
}

func (g *shardsController) signalTopologyChanged() {
	g.eventMtx.Lock()
	defer g.eventMtx.Unlock()

	old := g.topologyChanged
	g.topologyChanged = make(chan struct{})
	close(old)
}
func (g *shardsController) leaderFunc(ctx context.Context, node raft.RaftStatusProvider) error {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		// run shard rebalancing
		status := node.Status()
		voters := status.Config.Voters.IDs()
		members := make([]uint64, len(voters))
		idx := 0
		for id := range voters {
			members[idx] = id
			idx++
		}
		changes := g.cleanupMembers(members, g.state.GetStreams())
		if len(changes) > 0 {
			err := g.commit(ctx, changes...)
			if err != nil {
				log.Printf("failed to cleanup members: %v", err)
			}
		}
		changes = g.balanceShards(members, g.state.GetStreams())
		if len(changes) > 0 {
			err := g.commit(ctx, changes...)
			if err != nil {
				log.Printf("failed to apply shard rebalancing: %v", err)
			}
		}

		changes = g.balanceShardLeader(g.state.GetStreams())
		if len(changes) > 0 {
			err := g.commit(ctx, changes...)
			if err != nil {
				log.Printf("failed to apply shard leader rebalancing: %v", err)
			}
		}

		waitCtx, cancel := context.WithCancel(ctx)
		select {
		case <-g.topologyChanged:
			cancel()
		case <-g.state.StreamsChanged(waitCtx):
			cancel()
		case <-ticker.C:
			cancel()
		case <-ctx.Done():
			cancel()
			return nil
		}
	}
}
func (g *shardsController) GetReplicaID(streamID string, shardID uint64) (*api.Stream, uint64, error) {
	stream := g.state.GetStream(streamID)
	if stream == nil {
		return nil, 0, ErrStreamNotFound
	}
	for _, shard := range stream.Shards {
		if shard.ID == shardID {
			for _, replica := range shard.Replicas {
				if replica == g.id {
					return stream, g.id, nil
				}
			}
			return stream, shard.Replicas[rand.Intn(len(shard.Replicas))], nil
		}
	}
	return nil, 0, ErrShardNotFound
}
func (g *shardsController) ResolveTimestamp(streamID string, shardID uint64, timestamp uint64) (uint64, error) {
	shard, err := g.getShard(streamID, shardID)
	if err != nil {
		return 0, err
	}
	return shard.clog.LookupTimestamp(timestamp), nil
}
func (g *shardsController) Stream(name string) (string, error) {
	for _, stream := range g.state.GetStreams() {
		if name == stream.Name || name == stream.ID {
			return stream.ID, nil
		}
	}
	return "", ErrStreamNotFound
}
func (g *shardsController) ListShards(streamID string) ([]uint64, error) {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	stream := g.state.GetStream(streamID)
	if stream == nil {
		return nil, ErrStreamNotFound
	}
	out := make([]uint64, len(stream.Shards))
	for idx := range out {
		out[idx] = stream.Shards[idx].ID
	}
	return out, nil
}
func (g *shardsController) StopCluster(parentCtx context.Context) error {
	g.mtx.Lock()
	for streamID, shards := range g.shards {
		for shardID, shard := range shards {
			ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
			shard.Cancel(ctx)
			cancel()
			shard.clog.Close()
			delete(shards, shardID)
		}
		delete(g.shards, streamID)
	}
	g.mtx.Unlock()
	return g.node.Shutdown()
}
func (g *shardsController) Close() error {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	for _, shards := range g.shards {
		for _, shard := range shards {
			shard.clog.Close()
		}
	}
	return nil
}
func (g *shardsController) Serve(grpcServer *grpc.Server) {
	api.RegisterNestClusterServer(grpcServer, g)
}
func (g *shardsController) Run(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			ctx, cancel := context.WithCancel(ctx)
			g.syncShards(ctx, g.state.GetAssignedShards(g.id))

			select {
			case <-ctx.Done():
				cancel()
				return
			case <-ticker.C:
			case <-g.state.AssignmentChanged(ctx, g.id):
			}
			cancel()
		}
	}()
	g.node.Run(ctx)
}

func (g *shardsController) Reader(streamID string, shardID uint64) (io.ReadSeeker, error) {
	shard, err := g.getShard(streamID, shardID)
	if err != nil {
		return nil, err
	}
	return shard.clog.Reader(), nil
}
func (g *shardsController) WaitReady(ctx context.Context) {
	select {
	case <-g.node.Ready():
	case <-ctx.Done():
	}
}
func (g *shardsController) AddShard(ctx context.Context, streamID string, shardID uint64) error {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	clog, err := commitlog.Open(path.Join(g.datadir, "streams", streamID, fmt.Sprintf("%d", shardID)), 250)
	if err != nil {
		return nil
	}
	stream, ok := g.shards[streamID]
	if !ok {
		stream = map[uint64]*assignedShard{
			shardID: newAssignedShard(shardID, g.id, streamID, g.state, g.dstate, g.caller, g, clog),
		}
		g.shards[streamID] = stream
	} else {
		_, ok = stream[shardID]
		if ok {
			clog.Delete()
			return ErrShardAlreadyExists
		}
		stream[shardID] = newAssignedShard(shardID, g.id, streamID, g.state, g.dstate, g.caller, g, clog)
	}
	offset := stream[shardID].clog.Offset()
	if offset > 0 {
		offset = offset - 1
	}
	g.AdvanceShard(shardID, offset, stream[shardID].clog.Latest())
	go stream[shardID].Run(context.Background())
	log.Printf("created shard %d", shardID)
	return nil
}
func (g *shardsController) RemoveShard(ctx context.Context, streamID string, shardID uint64) error {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	stream, ok := g.shards[streamID]
	if !ok {
		return ErrStreamNotFound
	}
	_, ok = stream[shardID]
	if !ok {
		return ErrShardNotFound
	}
	stream[shardID].Cancel(ctx)
	err := stream[shardID].clog.Close()
	if err != nil {
		return err
	}
	delete(stream, shardID)
	if len(stream) == 0 {
		delete(g.shards, streamID)
	}
	log.Printf("deleted shard %d", shardID)
	return nil
}

func (g *shardsController) GetShard(streamID string, shardID uint64) (*assignedShard, error) {
	g.mtx.RLock()
	defer g.mtx.RUnlock()
	return g.getShard(streamID, shardID)
}
func (g *shardsController) getShard(streamID string, shardID uint64) (*assignedShard, error) {
	stream, ok := g.shards[streamID]
	if !ok {
		return nil, ErrStreamNotFound
	}
	shard, ok := stream[shardID]
	if !ok {
		return nil, ErrShardNotFound
	}
	return shard, nil
}

func (g *shardsController) syncShards(ctx context.Context, clusterShards []*api.Shard) {
	toCreate := []*api.Shard{}
	toStop := []*api.Shard{}

	for _, shard := range clusterShards {
		stream, ok := g.shards[shard.StreamID]
		if !ok {
			toCreate = append(toCreate, shard)
			continue
		}
		if _, ok := stream[shard.ID]; !ok {
			toCreate = append(toCreate, shard)
			continue
		}
	}
	for streamID, stream := range g.shards {
		for shard := range stream {
			found := false
			for _, clusterShards := range clusterShards {
				if clusterShards.ID == shard {
					found = true
				}
			}
			if !found {
				toStop = append(toStop, &api.Shard{ID: shard, StreamID: streamID})
			}
		}
	}

	for _, shard := range toStop {
		err := g.RemoveShard(ctx, shard.StreamID, shard.ID)
		if err != nil {
			log.Printf("failed to delete local shard: %v", err)
		}
	}
	for _, shard := range toCreate {
		err := g.AddShard(ctx, shard.StreamID, shard.ID)
		if err != nil {
			log.Printf("failed to create local shard: %v", err)
		}
	}
}

func encode(events ...*api.StateTransition) ([]byte, error) {
	format := api.StateTransitionSet{
		Events: events,
	}
	return proto.Marshal(&format)
}

func (g *shardsController) commit(ctx context.Context, events ...*api.StateTransition) error {
	if len(events) == 0 {
		return nil
	}
	payload, err := encode(events...)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = g.node.Apply(ctx, payload)
	if err == nil {
		//	log.Print(events)
	}
	return err
}
func (g *shardsController) callCluster(id uint64, f func(c api.NestClusterClient) error) error {
	return g.caller.Call(id, func(g *grpc.ClientConn) error {
		return f(api.NewNestClusterClient(g))
	})
}

func (g *shardsController) AdvanceShard(shard uint64, progress, latest uint64) error {
	return g.dstate.ShardReplicas().Set(shard, progress, latest)
}
func (g *shardsController) Write(ctx context.Context, stream string, shardKey string, payload []byte) error {
	var shard *api.Shard
	var err error
	for {
		shard, err = g.state.GetShardForKey(stream, shardKey)
		if err != nil {
			return err
		}
		if shard.Leader == g.id {
			break
		}
		err := g.caller.Call(shard.Leader, func(g *grpc.ClientConn) error {
			_, err := api.NewNestClient(g).PutEntry(ctx, &api.PutEntryInput{
				StreamID: stream,
				ShardKey: shardKey,
				Payload:  payload,
			})
			return err
		})
		if err == nil {
			return err
		}
	}
	g.mtx.RLock()
	localShard, err := g.getShard(shard.StreamID, shard.ID)
	g.mtx.RUnlock()

	if err != nil {
		return err
	}

	ts := uint64(time.Now().UnixNano())
	g.writeMtx.Lock()
	progress, err := localShard.clog.WriteEntry(ts, payload)
	if err != nil {
		g.writeMtx.Unlock()
		return err
	}
	err = g.AdvanceShard(localShard.ID, progress, ts)
	g.writeMtx.Unlock()
	return err
	// if err != nil {
	// 	return err
	// }
	// done := make(chan struct{})
	// ticker := time.NewTicker(200 * time.Millisecond)
	// defer ticker.Stop()
	// defer close(done)
	// for {
	// 	replicaIDs, err := g.state.GetReplicaIDs(shard.StreamID, shard.ID)
	// 	if err == nil {
	// 		if g.dstate.ShardReplicas().Watermark(replicaIDs, shard.ID) >= progress {
	// 			return nil
	// 		}
	// 	}
	// 	select {
	// 	case <-ctx.Done():
	// 		return ctx.Err()
	// 	case <-ticker.C:
	// 	}
	// }
}

func (g *shardsController) ListStreams(ctx context.Context, in *api.ListStreamsInput) (*api.ListStreamsOutput, error) {
	return &api.ListStreamsOutput{Streams: g.state.GetStreams(), ShardReplicaState: g.dstate.ShardReplicas().All()}, nil
}
func (g *shardsController) CreateStream(ctx context.Context, name string, replicaCount int64, shardCount int64) (string, error) {
	id := uuid.New().String()
	if shardCount < 1 {
		return "", errors.New("invalid shard count")
	}
	shards := make([]uint64, int(shardCount))
	for idx := range shards {
		shards[idx] = g.rand.Uint64()
	}
	event := &api.StateTransition{
		Event: &api.StateTransition_StreamCreated{
			StreamCreated: &api.StreamCreated{
				ID:                  id,
				DesiredReplicaCount: replicaCount,
				Name:                name,
				Shards:              shards,
			}},
	}
	return id, g.commit(ctx, event)
}

func (g *shardsController) GetClusterEntries(in *api.GetClusterEntriesInput, s api.NestCluster_GetClusterEntriesServer) error {

	if in.MaxBatchSize == 0 || in.MaxBatchSize > 200 {
		in.MaxBatchSize = 200
	}
	shard, err := g.GetShard(in.StreamID, in.ShardID)
	if err != nil {
		return err
	}
	if leader, err := g.state.GetLeader(in.StreamID, in.ShardID); err != nil || leader != g.id {
		return errors.New("node not leader")
	}
	cursor := shard.clog.Reader()
	consumer := stream.NewConsumer(
		stream.FromOffset(int64(in.FromOffset)),
		stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
		stream.WithMaxRecordCount(int64(in.MaxTotalRecordsCount)),
		stream.WithMaxBatchSize(int(in.MaxBatchSize)),
	)
	return consumer.Consume(s.Context(), cursor, func(_ context.Context, b stream.Batch) error {
		if len(b.Records) == 0 {
			return nil
		}
		out := &api.GetClusterEntriesOutput{
			StreamID: in.StreamID,
			ShardID:  in.ShardID,
			Entries:  make([]*api.Entry, len(b.Records)),
		}
		for idx := range b.Records {
			out.Entries[idx] = &api.Entry{
				Offset:    b.Records[idx].Offset(),
				Payload:   b.Records[idx].Payload(),
				Timestamp: b.Records[idx].Timestamp(),
			}
		}
		return s.Send(out)
	})
}
