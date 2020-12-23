package nest

import (
	"context"
	"sync"

	"github.com/vx-labs/commitlog/stream"
	"github.com/vx-labs/nest/nest/api"
	"google.golang.org/grpc"
)

type FSM interface {
	PutRecords(ctx context.Context, records [][]byte) error
}

func NewServer(state MessageLog) *server {
	return &server{
		state: state,
	}
}

type server struct {
	state MessageLog
}

func (s *server) Serve(grpcServer *grpc.Server) {
	api.RegisterMessagesServer(grpcServer, s)
}
func (s *server) ListTopics(ctx context.Context, in *api.ListTopicsRequest) (*api.ListTopicsResponse, error) {
	out := s.state.ListTopics(in.Pattern)
	return &api.ListTopicsResponse{TopicMetadatas: out}, nil
}

func (s *server) GetTopics(in *api.GetTopicsRequest, client api.Messages_GetTopicsServer) error {
	var consumer stream.Consumer
	ctx, cancel := context.WithCancel(client.Context())
	defer cancel()

	offset := uint64(in.FromOffset)
	shards := s.state.ListShards()
	wg := sync.WaitGroup{}
	for idx := range shards {
		shard := shards[idx]

		if in.Watch {
			consumer = stream.NewConsumer(
				stream.FromOffset(int64(offset)),
				stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
				stream.WithOffsetIterator(s.state.TopicsIterator(shard, in.Pattern)),
			)
		} else {
			consumer = stream.NewConsumer(
				stream.FromOffset(int64(offset)),
				stream.WithEOFBehaviour(stream.EOFBehaviourExit),
				stream.WithOffsetIterator(s.state.TopicsIterator(shard, in.Pattern)),
			)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.state.Consume(ctx, 0, consumer,
				func(_ context.Context, _ uint64, batch []*api.Record) error {
					return client.Send(&api.GetTopicsResponse{Records: batch})
				},
			)
			if err != nil {
				cancel()
				return
			}
		}()
	}
	wg.Wait()
	return nil
}
