package nest

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"

	"github.com/vx-labs/nest/nest/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FSM interface {
	PutRecords(ctx context.Context, records []*api.Record) error
}

func NewServer(fsm FSM, state MessageLog) *server {
	return &server{
		fsm:   fsm,
		state: state,
	}
}

type server struct {
	fsm   FSM
	state MessageLog
}

type URLWriter func(ctx context.Context, url *url.URL) (io.Writer, error)
type URLReader func(ctx context.Context, url *url.URL) (io.Reader, error)

func FileURLWriter() URLWriter {
	return func(ctx context.Context, url *url.URL) (io.Writer, error) {
		filename := url.Path
		_, err := os.Stat(filename)
		if err == nil {
			return nil, errors.New("file exists")
		}
		fd, err := os.Create(filename)
		if err != nil {
			return nil, err
		}
		go func() {
			<-ctx.Done()
			fd.Close()
		}()
		return fd, nil
	}
}

func FileURLReader() URLReader {
	return func(ctx context.Context, url *url.URL) (io.Reader, error) {
		filename := url.Path
		_, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		fd, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		go func() {
			<-ctx.Done()
			fd.Close()
		}()
		return fd, nil
	}
}

func (s *server) Dump(in *api.DumpRequest, stream api.Messages_DumpServer) error {
	ctx := stream.Context()
	destinationURL, err := url.Parse(in.DestinationURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse DestinationURL: %v", err)
	}
	var urlWriter URLWriter
	switch destinationURL.Scheme {
	case "file":
		urlWriter = FileURLWriter()
	default:
		return status.Errorf(codes.InvalidArgument, "unknown DestinationURL scheme: %s", destinationURL.Scheme)
	}
	w, err := urlWriter(ctx, destinationURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to open DestinationURL: %v", err)
	}
	return s.state.Dump(w)
}

func (s *server) Load(in *api.LoadRequest, stream api.Messages_LoadServer) error {
	ctx := stream.Context()
	source, err := url.Parse(in.SourceURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse SourceURL: %v", err)
	}
	var urlReader URLReader
	switch source.Scheme {
	case "file":
		urlReader = FileURLReader()
	default:
		return status.Errorf(codes.InvalidArgument, "unknown SourceURL scheme: %s", source.Scheme)
	}
	r, err := urlReader(ctx, source)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to open SourceURL: %v", err)
	}
	return s.state.Load(r)
}
func (s *server) PutRecords(ctx context.Context, in *api.PutRecordsRequest) (*api.PutRecordsResponse, error) {
	return &api.PutRecordsResponse{}, s.fsm.PutRecords(ctx, in.Records)
}
func (s *server) GetRecords(in *api.GetRecordsRequest, stream api.Messages_GetRecordsServer) error {
	return s.state.GetRecords(stream.Context(), in.Patterns, in.FromTimestamp, func(topic []byte, ts int64, payload []byte) error {
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
