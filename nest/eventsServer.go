package nest

import (
	"context"

	"github.com/vx-labs/commitlog/stream"
	"github.com/vx-labs/nest/nest/api"
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

func (s *eventServer) GetEvents(in *api.GetEventRequest, client api.Events_GetEventsServer) error {
	if in.Tenant == "" {
		return status.Error(codes.InvalidArgument, "missing Tenant in request")
	}
	var consumer stream.Consumer
	offset := uint64(in.FromOffset)

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
