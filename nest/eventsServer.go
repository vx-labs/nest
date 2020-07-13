package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if in.Tenant == "" {
		return status.Error(codes.InvalidArgument, "missing Tenant in request")
	}
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
		)
	} else {
		consumer = stream.NewConsumer(
			stream.FromOffset(int64(offset)),
			stream.WithEOFBehaviour(stream.EOFBehaviourExit),
		)
	}
	return s.state.Consume(client.Context(), consumer,
		EventTenantMatcher(in.Tenant,
			func(_ context.Context, _ uint64, batch []*api.Event) error {
				return client.Send(&api.GetEventResponse{Events: batch})
			},
		))
}

func (s *eventServer) Serve(grpcServer *grpc.Server) {
	api.RegisterEventsServer(grpcServer, s)
}
