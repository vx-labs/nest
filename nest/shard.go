package nest

import (
	"context"
	"log"
	"sync"

	"github.com/vx-labs/commitlog"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/nest/distributed"
	"google.golang.org/grpc"
)

type shardState uint32

const (
	unknown shardState = iota
	replica
	leader
)

type Node interface {
	AdvanceShard(id uint64, progress, latest uint64) error
}

func newAssignedShard(id, peerID uint64, streamID string, state ClusterState, dstate distributed.State, caller Caller, node Node, clog commitlog.CommitLog) *assignedShard {
	return &assignedShard{
		ID:       id,
		PeerID:   peerID,
		streamID: streamID,
		state:    state,
		dstate:   dstate,
		clog:     clog,
		caller:   caller,
		advancer: node,
		done:     make(chan struct{}),
	}
}

type assignedShard struct {
	ID       uint64
	PeerID   uint64
	uptodate uint32
	streamID string
	cancel   context.CancelFunc
	done     chan struct{}
	state    ClusterState
	dstate   distributed.State
	caller   Caller
	advancer Node
	clog     commitlog.CommitLog
	mtx      sync.Mutex
}

func (s *assignedShard) getShardState() (shardState, uint64) {
	shardLeader, err := s.state.GetLeader(s.streamID, s.ID)
	if err == nil && shardLeader == s.PeerID {
		return leader, shardLeader
	}
	return replica, shardLeader
}
func (s *assignedShard) callCluster(id uint64, f func(c api.NestClusterClient) error) error {
	return s.caller.Call(id, func(g *grpc.ClientConn) error {
		return f(api.NewNestClusterClient(g))
	})
}
func (s *assignedShard) Cancel(ctx context.Context) {
	if s.cancel == nil {
		return
	}
	s.cancel()
	select {
	case <-s.done:
	case <-ctx.Done():
	}
}
func (s *assignedShard) Run(ctx context.Context) {
	s.done = make(chan struct{})
	defer close(s.done)
	rootCtx, rootCancel := context.WithCancel(ctx)
	defer rootCancel()
	s.cancel = rootCancel
	for {
		ctx, cancel := context.WithCancel(rootCtx)
		shardState, currentLeader := s.getShardState()
		switch shardState {
		case leader:
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case <-s.state.ShardLeaderChanged(ctx, s.streamID, s.ID, currentLeader):
			}
		case replica:
			if currentLeader != s.PeerID {
				select {
				case <-rootCtx.Done():
					cancel()
					return
				case <-ctx.Done():
					cancel()
					return
				case <-s.state.ShardLeaderChanged(ctx, s.streamID, s.ID, currentLeader):
				case <-s.RunReplica(ctx, currentLeader):
				}
			}
		}
		cancel()
	}
}
func (s *assignedShard) replicaLoop(ctx context.Context, leader uint64) error {
	err := s.callCluster(leader, func(c api.NestClusterClient) error {
		from := s.clog.Offset()
		stream, err := c.GetClusterEntries(ctx, &api.GetClusterEntriesInput{
			StreamID:   s.streamID,
			ShardID:    s.ID,
			FromOffset: from,
			MaxCount:   20,
		})
		if err != nil {
			if err == context.Canceled {
				return nil
			}
			log.Printf("failed to contact shard leader: %v", err)
			return err
		}
		for {
			out, err := stream.Recv()
			if err == context.Canceled {
				return nil
			}
			if err != nil {
				log.Printf("failed to stream from shard leader: %v", err)
				return err
			}
			if len(out.Entries) > 0 {
				var offset uint64
				for idx := range out.Entries {
					entry := out.Entries[idx]
					if entry.Offset != s.clog.Offset() {
						log.Panicf("would have corrupted shard %d: written leader entry %d would have local offset %d", s.ID, entry.Offset, s.clog.Offset())
					}
					offset, err = s.clog.WriteEntry(entry.Timestamp, entry.Payload)
					if err != nil {
						log.Printf("failed to replicate shard: %v", err)
					} else if offset != entry.Offset {
						log.Panicf("corrupted shard: written leader entry %d has local offset %d", entry.Offset, offset)
					}
					//log.Printf("from=%d, leader=%d, cur=%d", from, entry.Offset, offset)
				}
				// var thresold uint64 = 5000
				// if out.DeltaMilisec < thresold && atomic.LoadUint32(&s.uptodate) == 0 {
				// 	atomic.StoreUint32(&s.uptodate, 1)
				// 	log.Printf("shard is now up to date")
				// } else if out.DeltaMilisec >= thresold && atomic.LoadUint32(&s.uptodate) == 1 {
				// 	atomic.StoreUint32(&s.uptodate, 0)
				// 	log.Printf("shard is late: %d ms", out.DeltaMilisec)
				// }

				//log.Printf("written %d entries, new offset is %d", len(out.Entries), s.clog.Offset()-1)
				err = s.advancer.AdvanceShard(s.ID, offset, out.Entries[len(out.Entries)-1].Timestamp)
				if err != nil {
					log.Printf("failed to inform leader progress: %v", err)
					return nil
				}
			}
		}
	})
	if err != nil {
		log.Printf("shard leader %d unreachable: %v", leader, err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.dstate.ShardReplicas().OnPeerProgressed(ctx, leader, s.ID, s.clog.Offset()-1):
	}
	return nil
}

func (s *assignedShard) RunReplica(ctx context.Context, leader uint64) chan error {
	ch := make(chan error)
	go func() {
		defer close(ch)
		for {
			err := s.replicaLoop(ctx, leader)
			if err != nil {
				select {
				case <-ctx.Done():
				case ch <- err:
				}
				return
			}
		}
	}()
	return ch
}
