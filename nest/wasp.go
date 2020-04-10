package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/wasp/wasp/taps"
	"google.golang.org/grpc"
)

type WaspReceiver struct {
	fsm FSM
}

func NewWaspReceiver(fsm FSM) *WaspReceiver {
	return &WaspReceiver{fsm: fsm}
}

func (w *WaspReceiver) Serve(server *grpc.Server) {
	taps.RegisterTapServer(server, w)
}

func (w *WaspReceiver) PutWaspRecords(ctx context.Context, in *taps.PutWaspRecordRequest) (*taps.PutWaspRecordsResponse, error) {
	records := make([]*api.Record, len(in.WaspRecords))
	for idx := range records {
		records[idx] = &api.Record{
			Timestamp: in.WaspRecords[idx].Timestamp,
			Payload:   in.WaspRecords[idx].Payload,
			Topic:     in.WaspRecords[idx].Topic,
		}
	}
	return &taps.PutWaspRecordsResponse{}, w.fsm.PutRecords(ctx, records)
}
