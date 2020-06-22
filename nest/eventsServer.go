package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
	"google.golang.org/grpc"
)

func NewEventsServer(state EventsLog) *eventServer {
	return &eventServer{
		state: state,
	}
}

type eventServer struct {
	state EventsLog
}

func (s *eventServer) PutEvent(ctx context.Context, in *api.PutEventRequest) (*api.PutEventResponse, error) {
	return &api.PutEventResponse{}, s.state.PutEvents(ctx, []*api.Event{
		{Timestamp: in.Timestamp, Kind: in.Kind, Service: in.Service, Tenant: in.Tenant, Attributes: in.Attributes},
	})
}
func (s *eventServer) GetEvents(in *api.GetEventRequest, client api.Events_GetEventsServer) error {
	var consumer stream.Consumer
	if in.Watch {
		consumer = stream.NewConsumer(
			stream.FromOffset(in.FromOffset),
			stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
			stream.WithMaxBatchSize(250),
		)
	} else {
		consumer = stream.NewConsumer(
			stream.FromOffset(in.FromOffset),
			stream.WithEOFBehaviour(stream.EOFBehaviourExit),
			stream.WithMaxBatchSize(250),
		)
	}
	return s.state.Consume(client.Context(), consumer,
		func(_ context.Context, _ uint64, batch []*api.Event) error {
			return client.Send(&api.GetEventResponse{Events: batch})
		},
	)
}

func (s *eventServer) Serve(grpcServer *grpc.Server) {
	api.RegisterEventsServer(grpcServer, s)
}