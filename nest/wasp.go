package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/wasp/wasp/taps"
	"google.golang.org/grpc"
)

type WaspReceiver struct {
	messages MessageLog
}

func NewWaspReceiver(messages MessageLog) *WaspReceiver {
	return &WaspReceiver{messages: messages}
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
	return &taps.PutWaspRecordsResponse{}, w.messages.PutRecords(ctx, records)
}
