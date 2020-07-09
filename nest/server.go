package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
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

func (s *server) PutRecords(ctx context.Context, in *api.PutRecordsRequest) (*api.PutRecordsResponse, error) {
	return &api.PutRecordsResponse{}, s.state.PutRecords(ctx, in.Records)
}
func (s *server) GetRecords(in *api.GetRecordsRequest, client api.Messages_GetRecordsServer) error {
	var consumer stream.Consumer
	offset := uint64(in.FromOffset)
	if in.FromTimestamp > 0 {
		timestampOffset := s.state.LookupTimestamp(uint64(in.FromTimestamp))
		if timestampOffset > offset {
			offset = timestampOffset
		}
	}

	if in.Watch {
		consumer = stream.NewConsumer(
			stream.FromOffset(int64(offset)),
			stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
			stream.WithMaxBatchSize(250),
		)
	} else {
		consumer = stream.NewConsumer(
			stream.FromOffset(int64(offset)),
			stream.WithEOFBehaviour(stream.EOFBehaviourExit),
			stream.WithMaxBatchSize(250),
		)
	}
	return s.state.Consume(client.Context(), consumer,
		RecordMatcher(in.Patterns,
			func(_ context.Context, _ uint64, batch []*api.Record) error {
				return client.Send(&api.GetRecordsResponse{Records: batch})
			},
		),
	)
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
	offset := uint64(in.FromOffset)
	if in.FromTimestamp > 0 {
		timestampOffset := s.state.LookupTimestamp(uint64(in.FromTimestamp))
		if timestampOffset > offset {
			offset = timestampOffset
		}
	}

	if in.Watch {
		consumer = stream.NewConsumer(
			stream.FromOffset(int64(offset)),
			stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
			stream.WithMaxBatchSize(250),
			stream.WithOffsetIterator(s.state.TopicsIterator(in.Pattern)),
		)
	} else {
		consumer = stream.NewConsumer(
			stream.FromOffset(int64(offset)),
			stream.WithEOFBehaviour(stream.EOFBehaviourExit),
			stream.WithMaxBatchSize(250),
			stream.WithOffsetIterator(s.state.TopicsIterator(in.Pattern)),
		)
	}
	return s.state.Consume(client.Context(), consumer,
		func(_ context.Context, _ uint64, batch []*api.Record) error {
			return client.Send(&api.GetTopicsResponse{Records: batch})
		},
	)
}
