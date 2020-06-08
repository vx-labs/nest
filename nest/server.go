package nest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/vx-labs/nest/nest/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewServer(state MessageLog) *server {
	return &server{
		state: state,
	}
}

type server struct {
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

func (s *server) PutRecords(ctx context.Context, in *api.PutRecordsRequest) (*api.PutRecordsResponse, error) {
	return &api.PutRecordsResponse{}, s.state.PutRecords(in.Records)
}
func (s *server) GetRecords(in *api.GetRecordsRequest, stream api.Messages_GetRecordsServer) error {
	if in.Shard < 0 || int(in.Shard) >= s.state.Shards() {
		return status.Error(codes.InvalidArgument, "invalid shard")
	}
	_, err := s.state.GetRecords(stream.Context(), int(in.Shard), in.Patterns, in.FromOffset, func(topic []byte, ts int64, payload []byte) error {
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
