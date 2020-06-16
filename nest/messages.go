package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"sync"
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

type RecordConsumer func(offset uint64, topic []byte, ts int64, payload []byte) error

type MessageLog interface {
	io.Closer
	Dump(w io.Writer, fromOffset, lastOffset uint64) error
	Load(w io.Reader) error
	PutRecords(stateOffset uint64, b []*api.Record) error
	GetRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error)
	CurrentStateOffset() uint64
	CurrentOffset() uint64
	SetCurrentStateOffset(v uint64)
	Snapshot() ([]byte, error)
	Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error
	ListTopics(pattern []byte) []*api.TopicMetadata
	Consume(ctx context.Context, processor func(context.Context, uint64, []*api.Record) error, opts stream.ConsumerOptions) error
	GetTopics(ctx context.Context, pattern []byte, processor func(context.Context, uint64, []*api.Record) error) error
}

type Snapshot struct {
	Remote         uint64 `json:"remote,omitempty"`
	MessagesOffset uint64 `json:"messages_offset,omitempty"`
	StateOffset    uint64 `json:"state_offset,omitempty"`
}

type messageLog struct {
	id            uint64
	restorelock   sync.RWMutex
	indexlock     sync.Mutex
	datadir       string
	stateOffset   gommap.MMap
	stateOffsetFd *os.File
	log           commitlog.CommitLog
	topics        *topicsState
}

type compatLogger struct {
	l *zap.Logger
}

func (c *compatLogger) Debugf(string, ...interface{})   {}
func (c *compatLogger) Infof(string, ...interface{})    {}
func (c *compatLogger) Warningf(string, ...interface{}) {}
func (c *compatLogger) Errorf(string, ...interface{})   {}

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
		topics:        NewTopicState(),
	}
	go s.Consume(ctx, func(ctx context.Context, firstOffset uint64, records []*api.Record) error {
		topicValues := map[string]*Topic{}
		for idx, record := range records {
			offset := firstOffset + uint64(idx)

			v, ok := topicValues[string(record.Topic)]
			if !ok {
				topicValues[string(record.Topic)] = &Topic{
					Name:               record.Topic,
					Messages:           []uint64{offset},
					SizeInBytes:        uint64(len(record.Payload)),
					LastRecord:         record,
					GuessedContentType: http.DetectContentType(record.Payload),
				}
			} else {
				v.Messages = append(v.Messages, offset)
				v.SizeInBytes += uint64(len(record.Payload))
				v.LastRecord = record
				contentType := http.DetectContentType(record.Payload)
				if v.GuessedContentType != contentType {
					v.GuessedContentType = "application/octet-stream"
				}
			}
		}
		for _, t := range topicValues {
			v := s.topics.Match(t.Name)
			if len(v) == 0 {
				s.topics.Set(*t)
			} else {
				v[0].Messages = append(v[0].Messages, t.Messages...)
				t.Messages = v[0].Messages
				t.SizeInBytes += v[0].SizeInBytes
				if t.LastRecord == nil {
					t.LastRecord = v[0].LastRecord
				}
				if t.GuessedContentType != v[0].GuessedContentType {
					t.GuessedContentType = "application/octet-stream"
				}
				s.topics.Set(*t)
			}
		}
		return nil
	}, stream.ConsumerOptions{
		FromOffset:   0,
		MaxBatchSize: 250,
	})
	L(ctx).Info("loaded message log", zap.Uint64("current_log_offset", s.CurrentOffset()))
	return s, nil
}

type RemoteCaller func(id uint64, f func(*grpc.ClientConn) error) error

func (s *messageLog) Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()
	L(ctx).Debug("restoring snapshot")

	snapshotDescription := Snapshot{}
	err := json.Unmarshal(snapshot, &snapshotDescription)
	if err != nil {
		L(ctx).Debug("failed to decode state snapshot", zap.Error(err))
	} else {
		if s.id == snapshotDescription.Remote {
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
		err = s.load(file)
		if err != nil {
			return err
		}
		s.SetCurrentStateOffset(snapshotDescription.StateOffset)

		L(ctx).Info("loaded snapshot", zap.Uint64("current_log_offset", s.CurrentOffset()))
	}
	return nil
}
func (s *messageLog) Snapshot() ([]byte, error) {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()
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
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
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
	r, err := s.log.ReaderFrom(0)
	defer r.Close()
	if err != nil {
		return err
	}
	if lastOffset == 0 {
		lastOffset = s.log.Offset()
	}

	limit, err := r.Seek(int64(lastOffset), io.SeekStart)
	if err != nil {
		return err
	}
	_, err = r.Seek(int64(fromOffset), io.SeekStart)
	if err != nil {
		return err
	}
	defer r.Close()
	session := stream.NewSession(ctx, r, stream.ConsumerOptions{
		MaxBatchSize: 10,
		FromOffset:   int64(fromOffset),
		EOFBehaviour: stream.EOFBehaviourExit,
	})
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch, ok := <-session.Ready():
			records := make([]*api.Record, len(batch.Records))

			for idx, buf := range batch.Records {
				record := &api.Record{}
				err = proto.Unmarshal(buf, record)
				if err != nil {
					return err
				}
				records[idx] = record
			}
			for idx, record := range records {

				offset := batch.FirstOffset + uint64(idx)
				if offset >= uint64(limit) {
					return nil
				}
				payload, err := proto.Marshal(record)
				if err != nil {
					log.Print(err)
					return err
				}
				err = encoder.Encode(DumpRecord{Offset: offset, Payload: payload})
				if err != nil {
					log.Print(err)
					return err
				}
			}
			if !ok {
				return nil
			}
		}
	}
}

func (s *messageLog) Load(source io.Reader) error {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()
	return s.load(source)
}
func (s *messageLog) load(source io.Reader) error {
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
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	if len(pattern) == 0 {
		pattern = []byte("#")
	}
	topics := s.topics.Match(pattern)
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
func (s *messageLog) GetRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error) {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	return s.getRecords(patterns, fromOffset, f)
}
func (s *messageLog) Consume(ctx context.Context, processor func(context.Context, uint64, []*api.Record) error, opts stream.ConsumerOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, err := s.log.ReaderFrom(0)
	if err != nil {
		return err
	}
	defer r.Close()
	if opts.FromOffset > 0 {
		_, err = r.Seek(opts.FromOffset, io.SeekStart)
		if err != nil {
			return err
		}
	} else if opts.FromOffset < 0 {
		_, err = r.Seek(opts.FromOffset, io.SeekEnd)
		if err != nil {
			return err
		}
	}
	session := stream.NewSession(ctx, r, opts)
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch := <-session.Ready():
			records := make([]*api.Record, len(batch.Records))

			for idx, buf := range batch.Records {
				record := &api.Record{}
				err = proto.Unmarshal(buf, record)
				if err != nil {
					log.Print(err)
					return err
				}
				records[idx] = record
			}
			err := processor(ctx, batch.FirstOffset, records)
			if err != nil {
				return err
			}
		}
	}
}
func (s *messageLog) getRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error) {
	r, err := s.log.ReaderFrom(0)
	if err != nil {
		return fromOffset, err
	}
	defer r.Close()
	current, err := r.Seek(fromOffset, io.SeekStart)
	if err != nil {
		return current, err
	}
	buf := make([]byte, 20*1000*1000)
	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			return current, nil
		}
		if err != nil {
			return current, err
		}
		record := &api.Record{}
		err = proto.Unmarshal(buf[:n], record)
		if err != nil {
			return current, err
		}
		if len(patterns) > 0 {
			for _, pattern := range patterns {
				if match(pattern, record.Topic) {
					err = f(uint64(current), record.Topic, record.Timestamp, record.Payload)
					if err != nil {
						return current, err
					}
				}
			}
		} else {
			err = f(uint64(current), record.Topic, record.Timestamp, record.Payload)
			if err != nil {
				return current, err
			}
		}
		current++
	}
}

func (s *messageLog) GetTopics(ctx context.Context, pattern []byte, processor func(context.Context, uint64, []*api.Record) error) error {

	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	topics := s.topics.Match(pattern)
	r, err := s.log.ReaderFrom(0)
	if err != nil {
		return err
	}
	defer r.Close()
	for _, topic := range topics {
		r := commitlog.OffsetReader(topic.Messages, r)
		session := stream.NewSession(ctx, r, stream.ConsumerOptions{
			MaxBatchSize: 10,
			FromOffset:   0,
			EOFBehaviour: stream.EOFBehaviourExit,
		})
	loop:
		for {
			select {
			case <-ctx.Done():
				return nil
			case batch, ok := <-session.Ready():
				records := make([]*api.Record, len(batch.Records))
				for idx, buf := range batch.Records {
					record := &api.Record{}
					err = proto.Unmarshal(buf, record)
					if err != nil {
						return err
					}
					records[idx] = record
				}
				err := processor(ctx, batch.FirstOffset, records)
				if err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
				if !ok {
					break loop
				}
			}
		}
	}
	return nil
}
