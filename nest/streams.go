package nest

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/vx-labs/nest/nest/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StreamsServer interface {
	RegisterShards(stream string, shards []Shard) error
	Serve(*grpc.Server)
	api.StreamsServer
}

func NewStreamsServer() StreamsServer {
	return &streamsServer{
		states: map[string][]Shard{},
	}
}

type streamsServer struct {
	mtx    sync.RWMutex
	states map[string][]Shard
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
func (s *streamsServer) RegisterShards(stream string, shards []Shard) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if _, ok := s.states[stream]; !ok {
		s.states[stream] = shards
	} else {
		return errors.New("stream already registered")
	}
	return nil
}
func (s *streamsServer) Dump(in *api.DumpRequest, stream api.Streams_DumpServer) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	shards, ok := s.states[in.Stream]
	if !ok {
		return status.Error(codes.NotFound, "stream not found")
	}
	if int(in.Shard) >= len(shards) {
		return status.Error(codes.NotFound, "shard not found")
	}
	shard := shards[int(in.Shard)]
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
	return shard.Dump(w, 0)
}

func (s *streamsServer) Load(in *api.LoadRequest, stream api.Streams_LoadServer) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	shards, ok := s.states[in.Stream]
	if !ok {
		return status.Error(codes.NotFound, "stream not found")
	}
	if int(in.Shard) >= len(shards) {
		return status.Error(codes.NotFound, "shard not found")
	}
	shard := shards[int(in.Shard)]
	var urlReader URLReader
	sourceURL, err := url.Parse(in.SourceURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "unable to parse SourceURL: %v", err)
	}
	switch sourceURL.Scheme {
	case "file":
		urlReader = FileURLReader()
	default:
		return status.Errorf(codes.InvalidArgument, "unknown SourceURL scheme: %s", sourceURL.Scheme)
	}
	r, err := urlReader(stream.Context(), sourceURL)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to open SourceURL: %v", err)
	}
	return shard.Load(r)
}

func (s *streamsServer) SST(in *api.SSTRequest, stream api.Streams_SSTServer) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	shards, ok := s.states[in.Stream]
	if !ok {
		return status.Error(codes.NotFound, "stream not found")
	}
	if int(in.Shard) >= len(shards) {
		return status.Error(codes.NotFound, "shard not found")
	}
	shard := shards[int(in.Shard)]
	file, err := ioutil.TempFile("", "sst.*.nest")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	err = shard.Dump(file, in.FromOffset)
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

func (s *streamsServer) ListStreams(ctx context.Context, input *api.ListStreamsRequest) (*api.ListStreamsResponse, error) {
	out := make([]*api.StreamMetadata, len(s.states))
	streamIdx := 0
	for name, shards := range s.states {
		shardMetadatas := make([]*api.ShardMetadata, len(shards))
		for idx := range shards {
			stats := shards[idx].GetStatistics()
			shardMetadatas[idx] = &api.ShardMetadata{
				CurrentOffset: stats.CurrentOffset,
				ID:            uint64(idx),
				SegmentCount:  stats.SegmentCount,
				StoredBytes:   stats.StoredBytes,
			}
			out[streamIdx] = &api.StreamMetadata{
				Name:           name,
				ShardMetadatas: shardMetadatas,
			}
		}
		streamIdx++
	}
	return &api.ListStreamsResponse{StreamMetadatas: out}, nil
}

func (s *streamsServer) Serve(grpcServer *grpc.Server) {
	api.RegisterStreamsServer(grpcServer, s)
}
