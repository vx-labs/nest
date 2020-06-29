package nest

import (
	"context"

	"github.com/vx-labs/nest/nest/api"
	vespiary "github.com/vx-labs/vespiary/vespiary/api"
	"google.golang.org/grpc"
)

type VespiaryAuditRecorder struct {
	events EventsLog
}

func NewVespiaryAuditRecorder(events EventsLog) *VespiaryAuditRecorder {
	return &VespiaryAuditRecorder{events: events}
}

func (w *VespiaryAuditRecorder) Serve(server *grpc.Server) {
	vespiary.RegisterVespiaryAuditRecorderServer(server, w)
}

func (w *VespiaryAuditRecorder) PutVespiaryEvents(ctx context.Context, in *vespiary.PutVespiaryEventRequest) (*vespiary.PutVespiaryEventsResponse, error) {
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
			Service:    "vespiary",
			Attributes: attributes,
		}
	}
	return &vespiary.PutVespiaryEventsResponse{}, w.events.PutEvents(ctx, records)
}
