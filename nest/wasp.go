package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/wasp/wasp/audit"
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

type WaspAuditRecorder struct {
	events EventsLog
}

func NewWaspAuditRecorder(events EventsLog) *WaspAuditRecorder {
	return &WaspAuditRecorder{events: events}
}

func (w *WaspAuditRecorder) Serve(server *grpc.Server) {
	audit.RegisterWaspAuditRecorderServer(server, w)
}

func (w *WaspAuditRecorder) PutWaspEvents(ctx context.Context, in *audit.PutWaspEventRequest) (*audit.PutWaspWaspEventsResponse, error) {
	records := make([]*api.Event, len(in.Events))
	for idx := range records {
		attributes := make([]*api.EventAttribute, len(in.Events[idx].Attributes))
		for attributeIdx := range attributes {
			attributes[attributeIdx] = &api.EventAttribute{
				Key:   in.Events[idx].Attributes[attributeIdx].Key,
				Value: in.Events[idx].Attributes[attributeIdx].Value,
			}
		}
		records[idx] = &api.Event{
			Timestamp:  in.Events[idx].Timestamp,
			Tenant:     in.Events[idx].Tenant,
			Kind:       in.Events[idx].Kind,
			Attributes: attributes,
		}
	}
	return &audit.PutWaspWaspEventsResponse{}, w.events.PutEvents(ctx, records)
}
