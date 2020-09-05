package nest

import (
	"context"
	"fmt"
	"io"
	"path"

	"github.com/vx-labs/nest/commitlog"
	"github.com/vx-labs/nest/nest/fsm"
	"github.com/vx-labs/wasp/async"
	"github.com/vx-labs/wasp/cluster"
	"github.com/vx-labs/wasp/cluster/raft"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Shard interface {
	PutRecords(ctx context.Context, records [][]byte) error
	LookupTimestamp(ts uint64) uint64
	Shutdown(ctx context.Context) error
	Consume(f func(r io.ReadSeeker) error) error
	Dump(w io.Writer, fromOffset uint64) error
	Load(r io.Reader) error
	Offset() uint64
	Ready() <-chan struct{}
	Stop() error
	GetStatistics() commitlog.Statistics
	Latest() uint64
}

type shard struct {
	ctx        context.Context
	cancel     context.CancelFunc
	operations async.Operations
	node       cluster.Node
	fsm        *fsm.FSM
	recorder   Recorder
}

func (s *shard) GetStatistics() commitlog.Statistics {
	return s.recorder.GetStatistics()
}
func (s *shard) Ready() <-chan struct{} {
	return s.node.Ready()
}
func (s *shard) LookupTimestamp(ts uint64) uint64 {
	return s.recorder.LookupTimestamp(ts)
}
func (s *shard) Stop() error {
	err := s.node.Shutdown()
	if err != nil {
		return err
	}
	s.cancel()
	s.operations.Wait()
	return s.recorder.Close()
}
func (s *shard) Shutdown(ctx context.Context) error {
	return s.fsm.Shutdown(ctx)
}
func (s *shard) PutRecords(ctx context.Context, records [][]byte) error {
	return s.fsm.PutRecords(ctx, records)
}
func (s *shard) Consume(f func(r io.ReadSeeker) error) error {
	return s.recorder.Consume(f)
}
func (s *shard) Dump(w io.Writer, fromOffset uint64) error {
	return s.recorder.Dump(w, fromOffset)
}
func (s *shard) Load(r io.Reader) error {
	return s.recorder.Load(r)
}
func (s *shard) Offset() uint64 {
	return s.recorder.Offset()
}
func (s *shard) Latest() uint64 {
	return s.recorder.Latest()
}

func newShard(id uint64, stream string, shardID uint64, datadir string, clusterMultiNode cluster.MultiNode, raftConfig cluster.RaftConfig, logger *zap.Logger) (*shard, error) {
	datadir = path.Join(datadir, stream, fmt.Sprintf("%d", shardID))
	commandsCh := make(chan raft.Command)

	recorder, err := NewRecorder(id, stream, shardID, datadir, logger)
	if err != nil {
		return nil, err
	}

	stateMachine := fsm.NewFSM(id, recorder, commandsCh)

	bootOffset := recorder.CurrentStateOffset()
	raftConfig.GetStateSnapshot = recorder.Snapshot
	raftConfig.CommitApplier = func(ctx context.Context, event raft.Commit) error {
		if event.Index <= bootOffset {
			return nil
		}
		return stateMachine.Apply(event.Index, event.Payload)
	}
	var remoteCaller func(id uint64, f func(*grpc.ClientConn) error) error
	raftConfig.SnapshotApplier = func(ctx context.Context, index uint64, snapshotter *snap.Snapshotter) error {
		if index <= bootOffset {
			return nil
		}
		logger.Debug("starting snapshot restore")
		snapshot, err := snapshotter.Load()
		if err != nil {
			logger.Error("failed to load snapshot", zap.Error(err))
			return err
		}
		err = recorder.Restore(ctx, snapshot.Data, remoteCaller)
		if err != nil {
			logger.Error("failed to restore snapshot", zap.Error(err))
		}
		return err
	}
	node := clusterMultiNode.Node(fmt.Sprintf("%s-%d", stream, shardID), raftConfig)
	// nodeIndex := node.Index()
	// if false && nodeIndex < raftConfig.AppliedIndex {
	// 	logger.Fatal("raft index is behind log index", zap.Error(err), zap.Uint64("log_index", raftConfig.AppliedIndex), zap.Uint64("raft_index", nodeIndex))
	// 	err := recorder.Truncate()
	// 	if err != nil {
	// 		logger.Fatal("raft index is behind log index, and truncating failed", zap.Error(err))
	// 	} else {
	// 		logger.Warn("raft index is behind log index, truncated log")
	// 		panic("index late")
	// 		raftConfig.AppliedIndex = 0
	// 	}
	// }
	remoteCaller = node.Call
	ctx, cancel := context.WithCancel(context.Background())
	ctx = StoreLogger(ctx, logger)
	operations := async.NewOperations(ctx, logger)
	operations.Run("command publisher", func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-commandsCh:
				err := node.Apply(event.Ctx, event.Payload)
				select {
				case <-ctx.Done():
				case <-event.Ctx.Done():
				case event.ErrCh <- err:
				}
				close(event.ErrCh)
			}
		}
	})
	operations.Run("cluster node", func(ctx context.Context) {
		node.RunFromAppliedIndex(ctx, raftConfig.AppliedIndex)
	})
	return &shard{
		ctx:        ctx,
		cancel:     cancel,
		fsm:        stateMachine,
		node:       node,
		recorder:   recorder,
		operations: operations,
	}, nil
}

type Controller interface {
	WaitReady(ctx context.Context)
	Shards() []Shard
	Shutdown(ctx context.Context) error
	Stop() error
}

type controller struct {
	shards []Shard
}

func NewController(ctx context.Context, id uint64, stream string, shardCount int, datadir string, clusterMultiNode cluster.MultiNode, server StreamsServer, raftConfig cluster.RaftConfig, logger *zap.Logger) (Controller, error) {
	outShards := make([]Shard, shardCount)
	var err error
	for idx := range outShards {
		outShards[idx], err = newShard(id, stream, uint64(idx), datadir, clusterMultiNode, raftConfig, logger.With(zap.String("recorder_stream_name", stream), zap.Int("shard_id", idx)))
		if err != nil {
			return nil, err
		}
	}
	server.RegisterShards(stream, outShards)
	return &controller{
		shards: outShards,
	}, nil
}

func (c *controller) Shards() []Shard { return c.shards }
func (c *controller) WaitReady(ctx context.Context) {
	for _, shard := range c.shards {
		select {
		case <-shard.Ready():
		case <-ctx.Done():
			return
		}
	}
}
func (c *controller) Shutdown(ctx context.Context) error {
	for _, shard := range c.shards {
		err := shard.Shutdown(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *controller) Stop() error {
	for idx := range c.shards {
		shard := c.shards[idx]
		err := shard.Stop()
		if err != nil {
			return err
		}
	}
	return nil
}
