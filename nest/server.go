package nest

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
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

func HTTPURLReader() URLReader {
	return func(ctx context.Context, url *url.URL) (io.Reader, error) {
		resp, err := http.Get(url.String())
		if err != nil {
			return nil, err
		}
		go func() {
			<-ctx.Done()
			resp.Body.Close()
		}()
		return resp.Body, nil
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
	return s.state.Dump(w, 0, io.SeekEnd)
}

func (s *server) SST(in *api.SSTRequest, stream api.Messages_SSTServer) error {
	file, err := ioutil.TempFile("", "sst.*.nest")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	err = s.state.Dump(file, in.ToOffset, io.SeekStart)
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}
	file.Seek(0, io.SeekStart)
	chunk := make([]byte, 4096)
	for {
		n, err := file.Read(chunk)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		err = stream.Send(&api.SSTResponseChunk{
			Chunk: chunk[:n],
		})
		if err != nil {
			return err
		}
	}
}

func (s *server) Load(in *api.LoadRequest, stream api.Messages_LoadServer) error {
	ctx := stream.Context()
	source, err := url.Parse(in.SourceURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse SourceURL: %v", err)
	}
	var urlReader URLReader
	switch source.Scheme {
	case "http":
		urlReader = HTTPURLReader()
	case "https":
		urlReader = HTTPURLReader()
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
	_, err := s.state.GetRecords(in.Patterns, in.FromOffset, func(_ uint64, topic []byte, ts int64, payload []byte) error {
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
	return err
}
func (s *server) Serve(grpcServer *grpc.Server) {
	api.RegisterMessagesServer(grpcServer, s)
}
func (s *server) ListTopics(ctx context.Context, in *api.ListTopicsRequest) (*api.ListTopicsResponse, error) {
	out := s.state.ListTopics(in.Pattern)
	return &api.ListTopicsResponse{TopicMetadatas: out}, nil
}
func (s *server) ReindexTopics(in *api.ReindexTopicsRequest, stream api.Messages_ReindexTopicsServer) error {
	for progress := range s.state.ReindexTopics() {
		if err := stream.Send(&api.ReindexTopicsResponse{
			Progress: uint64(progress),
			Total:    100,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) StreamRecords(in *api.GetRecordsRequest, stream api.Messages_StreamRecordsServer) error {
	patterns := in.Patterns
	return s.state.Consume(stream.Context(), in.FromOffset, func(ctx context.Context, records []*api.Record) error {
		out := []*api.Record{}
		if len(patterns) > 0 {
			for _, record := range records {
				for _, pattern := range patterns {
					if match(pattern, record.Topic) {
						out = append(out, record)
					}
				}
			}
			return stream.Send(&api.GetRecordsResponse{Records: out})
		}
		return stream.Send(&api.GetRecordsResponse{Records: records})
	})
}
