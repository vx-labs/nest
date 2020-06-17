package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/tysontate/gommap"
	"github.com/vx-labs/nest/commitlog"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	messagesBucketName []byte = []byte("messages")
	appliedIndexKey    []byte = []byte("_index")
	encoding                  = binary.BigEndian
)

type RemoteCaller func(id uint64, f func(*grpc.ClientConn) error) error
type RecordProcessor func(context.Context, uint64, []*api.Record) error

type StateRecorder interface {
	io.Closer
	CurrentStateOffset() uint64
	SetCurrentStateOffset(v uint64)
	Snapshot() ([]byte, error)
	Consume(f func(r io.ReadSeeker) error) error
	Dump(w io.Writer, fromOffset, lastOffset uint64) error
	Load(w io.Reader) error
	PutRecords(stateOffset uint64, b []*api.Record) error
	Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error
}
type MessageLog interface {
	StateRecorder
	ListTopics(pattern []byte) []*api.TopicMetadata
	GetTopics(ctx context.Context, pattern []byte, processor RecordProcessor) error
}

type Snapshot struct {
	Remote         uint64 `json:"remote,omitempty"`
	MessagesOffset uint64 `json:"messages_offset,omitempty"`
	StateOffset    uint64 `json:"state_offset,omitempty"`
}

type messageLog struct {
	id            uint64
	datadir       string
	stateOffset   gommap.MMap
	stateOffsetFd *os.File
	log           commitlog.CommitLog
	topics        *topicAggregate
}

func NewMessageLog(ctx context.Context, id uint64, datadir string) (MessageLog, error) {
	L(ctx).Debug("opening commit log")
	start := time.Now()
	log, err := commitlog.Open(path.Join(datadir, "messages"), 250)
	if err != nil {
		return nil, err
	}
	L(ctx).Debug("commit log opened", zap.Duration("elapsed_time", time.Since(start)))
	statePath := path.Join(datadir, "messages.state")
	var fd *os.File

	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		fd, err = os.OpenFile(statePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0650)
		if err != nil {
			return nil, err
		}
		err = fd.Truncate(8)
		if err != nil {
			fd.Close()
			os.Remove(statePath)
			return nil, err
		}
	} else {
		fd, err = os.OpenFile(statePath, os.O_RDWR, 0650)
		if err != nil {
			return nil, err
		}
	}
	mmapedData, err := gommap.Map(fd.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	s := &messageLog{
		id:            id,
		datadir:       datadir,
		log:           log,
		stateOffset:   mmapedData,
		stateOffsetFd: fd,
		topics:        &topicAggregate{topics: NewTopicState()},
	}
	go s.Consume(func(r io.ReadSeeker) error {
		consumer := stream.NewConsumer()
		return consumer.Consume(ctx, r, RecordDecoder(s.topics.Processor()))
	})
	L(ctx).Info("loaded message log", zap.Uint64("current_log_offset", s.CurrentOffset()))
	return s, nil
}

func (s *messageLog) Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error {
	L(ctx).Debug("restoring snapshot")

	snapshotDescription := Snapshot{}
	err := json.Unmarshal(snapshot, &snapshotDescription)
	if err != nil {
		L(ctx).Debug("failed to decode state snapshot", zap.Error(err))
	} else {
		if s.id == snapshotDescription.Remote {
			L(ctx).Info("refusing to load snapshot from ourselves", zap.Uint64("remote_node", snapshotDescription.Remote), zap.Uint64("current_log_offset", s.CurrentOffset()), zap.Uint64("snapshot_log_offset", snapshotDescription.MessagesOffset))
			return nil
		}
		L(ctx).Info("loading snapshot", zap.Uint64("remote_node", snapshotDescription.Remote), zap.Uint64("current_log_offset", s.CurrentOffset()), zap.Uint64("snapshot_log_offset", snapshotDescription.MessagesOffset))
		file, err := ioutil.TempFile("", "sst-incoming.*.nest")
		if err != nil {
			L(ctx).Fatal("failed to create tmp file to receive snapshot", zap.Error(err))
		}
		defer os.Remove(file.Name())
		defer file.Close()
		err = caller(snapshotDescription.Remote, func(c *grpc.ClientConn) error {
			stream, err := api.NewMessagesClient(c).SST(ctx, &api.SSTRequest{
				ToOffset:   snapshotDescription.MessagesOffset,
				FromOffset: s.log.Offset(),
			})
			if err != nil {
				return err
			}

			for {
				chunk, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_, err = file.Write(chunk.Chunk)
				if err != nil {
					return err
				}
			}
			return file.Sync()
		})
		if err != nil {
			L(ctx).Fatal("failed to receive snapshot", zap.Error(err))
		}
		file.Seek(0, io.SeekStart)
		err = s.Load(file)
		if err != nil {
			return err
		}
		s.SetCurrentStateOffset(snapshotDescription.StateOffset)

		L(ctx).Info("loaded snapshot", zap.Uint64("current_log_offset", s.CurrentOffset()))
	}
	return nil
}
func (s *messageLog) Snapshot() ([]byte, error) {
	return json.Marshal(Snapshot{
		Remote:         s.id,
		StateOffset:    s.CurrentStateOffset(),
		MessagesOffset: s.CurrentOffset(),
	})
}
func (s *messageLog) Close() error {
	s.stateOffset.UnsafeUnmap()
	s.stateOffsetFd.Close()
	return s.log.Close()
}

func (s *messageLog) CurrentOffset() uint64 {
	return s.log.Offset()
}
func (s *messageLog) SetCurrentStateOffset(v uint64) {
	binary.BigEndian.PutUint64(s.stateOffset, v)
}
func (s *messageLog) CurrentStateOffset() uint64 {
	return binary.BigEndian.Uint64(s.stateOffset)
}

func (s *messageLog) PutRecords(stateOffset uint64, b []*api.Record) error {
	payloads := make([][]byte, len(b))
	var err error
	for idx, record := range b {
		payloads[idx], err = proto.Marshal(record)
		if err != nil {
			return err
		}
	}
	for _, payload := range payloads {
		_, err := s.log.Append(payload)
		if err != nil {
			return err
		}
	}
	s.SetCurrentStateOffset(stateOffset)
	return nil
}

func cut(t []byte) ([]byte, string) {
	end := bytes.IndexByte(t, '/')
	if end < 0 {
		return nil, string(t)
	}
	return t[end+1:], string(t[:end])
}

func match(pattern []byte, topic []byte) bool {
	var patternToken string
	var topicToken string
	pattern, patternToken = cut(pattern)
	if patternToken == "#" {
		return true
	}
	topic, topicToken = cut(topic)
	if len(topic) == 0 || len(pattern) == 0 {
		return len(topic) == 0 && len(pattern) == 0 && (topicToken == patternToken || patternToken == "+")
	}
	if topicToken == patternToken || patternToken == "+" {
		return match(pattern, topic)
	}
	return false
}

type DumpRecord struct {
	Offset  uint64 `json:"offset"`
	Payload []byte `json:"payload"`
}

func (s *messageLog) Dump(sink io.Writer, fromOffset, lastOffset uint64) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoder := json.NewEncoder(sink)
	return s.Consume(func(r io.ReadSeeker) error {
		if lastOffset == 0 {
			lastOffset = s.log.Offset()
		}

		limit, err := r.Seek(int64(lastOffset), io.SeekStart)
		if err != nil {
			return err
		}
		consumer := stream.NewConsumer(
			stream.FromOffset(int64(fromOffset)),
			stream.WithEOFBehaviour(stream.EOFBehaviourExit),
			stream.WithMaxBatchSize(10),
		)
		err = consumer.Consume(ctx, r,
			func(ctx context.Context, batch stream.Batch) error {
				fromOffset = batch.FirstOffset
				for idx, payload := range batch.Records {
					offset := fromOffset + uint64(idx)
					if offset >= uint64(limit) {
						return io.EOF
					}
					err = encoder.Encode(DumpRecord{Offset: offset, Payload: payload})
					if err != nil {
						return err
					}
				}
				return nil
			})
		if err == io.EOF {
			return nil
		}
		return err
	})
}

func (s *messageLog) Load(source io.Reader) error {
	firstOffset := s.log.Offset()
	dec := json.NewDecoder(source)
	record := &DumpRecord{}
	for {
		err := dec.Decode(&record)
		if err == io.EOF {
			return nil
		}
		if record.Offset >= firstOffset {
			_, err = s.log.Write(record.Payload)
			if err != nil {
				return err
			}
		}
	}
}
func (s *messageLog) ListTopics(pattern []byte) []*api.TopicMetadata {
	if len(pattern) == 0 {
		pattern = []byte("#")
	}
	topics := s.topics.topics.Match(pattern)
	out := make([]*api.TopicMetadata, len(topics))
	for idx := range out {
		out[idx] = &api.TopicMetadata{
			Name:               topics[idx].Name,
			MessageCount:       uint64(len(topics[idx].Messages)),
			LastRecord:         topics[idx].LastRecord,
			SizeInBytes:        topics[idx].SizeInBytes,
			GuessedContentType: topics[idx].GuessedContentType,
		}
	}
	return out
}
func (s *messageLog) Consume(f func(r io.ReadSeeker) error) error {
	r := s.log.Reader()
	defer r.Close()
	return f(r)
}

func RecordMatcher(patterns [][]byte, f RecordProcessor) RecordProcessor {
	return func(ctx context.Context, offset uint64, records []*api.Record) error {
		if len(patterns) > 0 {
			for idx, record := range records {
				for _, pattern := range patterns {
					if match(pattern, record.Topic) {
						err := f(ctx, offset+uint64(idx), []*api.Record{record})
						if err != nil {
							return err
						}
						break
					}
				}
			}
		} else {
			f(ctx, offset, records)
		}
		return nil
	}
}

// RecordDecoder returns a stream.Processor decoding api.Record and passing them to the provided callback function
func RecordDecoder(processor func(context.Context, uint64, []*api.Record) error) stream.Processor {
	return func(ctx context.Context, batch stream.Batch) error {
		records := make([]*api.Record, len(batch.Records))
		for idx, buf := range batch.Records {
			record := &api.Record{}
			err := proto.Unmarshal(buf, record)
			if err != nil {
				return err
			}
			records[idx] = record
		}
		return processor(ctx, batch.FirstOffset, records)
	}
}

func (s *messageLog) GetTopics(ctx context.Context, pattern []byte, processor RecordProcessor) error {
	return s.Consume(func(logReader io.ReadSeeker) error {
		consumer := stream.NewConsumer(stream.WithMaxBatchSize(10), stream.WithEOFBehaviour(stream.EOFBehaviourExit))
		r := commitlog.OffsetReader(s.topics.Get(pattern), logReader)
		return consumer.Consume(ctx, r, RecordDecoder(processor))
	})
}
