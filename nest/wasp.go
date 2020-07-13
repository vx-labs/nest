package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
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
			Retained:  in.WaspRecords[idx].Retained,
			Sender:    in.WaspRecords[idx].Sender,
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

func (w *WaspAuditRecorder) GetWaspEvents(in *audit.GetWaspEventsRequest, client audit.WaspAuditRecorder_GetWaspEventsServer) error {
	offset := w.events.LookupTimestamp(uint64(in.FromTimestamp))
	consumer := stream.NewConsumer(
		stream.FromOffset(int64(offset)),
		stream.WithEOFBehaviour(stream.EOFBehaviourPoll),
	)
	return w.events.Consume(client.Context(), consumer, func(_ context.Context, _ uint64, events []*api.Event) error {
		out := make([]*audit.WaspAuditEvent, len(events))
		for idx := range events {
			event := events[idx]
			attributes := make([]*audit.WaspEventAttribute, len(event.Attributes))
			for attrIdx := range attributes {
				attributes[attrIdx] = &audit.WaspEventAttribute{
					Key:   event.Attributes[attrIdx].Key,
					Value: event.Attributes[attrIdx].Value,
				}
			}
			out[idx] = &audit.WaspAuditEvent{Timestamp: event.Timestamp, Tenant: event.Tenant, Service: event.Service, Kind: event.Kind, Attributes: attributes}
		}
		return client.Send(&audit.GetWaspEventsResponse{Events: out})
	})
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
			Service:    "wasp",
			Attributes: attributes,
		}
	}
	return &audit.PutWaspWaspEventsResponse{}, w.events.PutEvents(ctx, records)
}
