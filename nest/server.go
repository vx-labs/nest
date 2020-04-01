package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"google.golang.org/grpc"
)

type FSM interface {
	PutRecords(ctx context.Context, records []*api.Record) error
}

func NewServer(fsm FSM, state State) *server {
	return &server{
		fsm:   fsm,
		state: state,
	}
}

type server struct {
	fsm   FSM
	state State
}

func (s *server) PutRecords(ctx context.Context, in *api.PutRecordsRequest) (*api.PutRecordsResponse, error) {
	return &api.PutRecordsResponse{}, s.fsm.PutRecords(ctx, in.Records)
}
func (s *server) GetRecords(in *api.GetRecordsRequest, stream api.Messages_GetRecordsServer) error {
	return s.state.GetRecords(stream.Context(), in.Topic, in.FromTimestamp, func(topic []byte, ts int64, payload []byte) error {
		return stream.Send(&api.GetRecordsResponse{
			Records: []*api.Record{
				&api.Record{
					Topic:     topic,
					Timestamp: ts,
					Payload:   payload,
				},
			},
		})
	})
}

func (s *server) Serve(grpcServer *grpc.Server) {
	api.RegisterMessagesServer(grpcServer, s)
}
