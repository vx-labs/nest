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
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/tysontate/gommap"
	"github.com/vx-labs/nest/commitlog"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	messagesBucketName []byte = []byte("messages")
	appliedIndexKey    []byte = []byte("_index")
	encoding                  = binary.BigEndian
)

type RecordConsumer func(offset uint64, topic []byte, ts int64, payload []byte) error

type eofBehaviour int

const (
	// EOFBehaviourPoll will make the session poll for new records after an EOF error is received
	EOFBehaviourPoll eofBehaviour = 1 << iota
	// EOFBehaviourExit wil make the session exit when EOF is received
	EOFBehaviourExit eofBehaviour = 1 << iota
)

// ConsumerOptions describes stream session preferences
type ConsumerOptions struct {
	MaxBatchSize int
	FromOffset   int64
	EOFBehaviour eofBehaviour
}

type MessageLog interface {
	io.Closer
	Dump(w io.Writer, lastOffset uint64, whence int) error
	Load(w io.Reader) error
	PutRecords(stateOffset uint64, b []*api.Record) error
	GetRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error)
	CurrentStateOffset() uint64
	CurrentOffset() uint64
	SetCurrentStateOffset(v uint64)
	Snapshot() ([]byte, error)
	Restore(ctx context.Context, snapshot []byte, caller RemoteCaller) error
	ListTopics(pattern []byte) []*api.TopicMetadata
	Consume(ctx context.Context, processor func(context.Context, Batch) error, opts ConsumerOptions) error
	GetTopics(ctx context.Context, pattern []byte, processor func(context.Context, Batch) error) error
}

type Snapshot struct {
	Remote         uint64 `json:"remote,omitempty"`
	MessagesOffset uint64 `json:"messages_offset,omitempty"`
	StateOffset    uint64 `json:"state_offset,omitempty"`
	Topics         []byte `json:"topics,omitempty"`
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
	go s.Consume(ctx, func(ctx context.Context, batch Batch) error {
		topicValues := map[string]*Topic{}
		for idx, record := range batch.Records {
			offset := batch.FirstOffset + uint64(idx)

			v, ok := topicValues[string(record.Topic)]
			if !ok {
				topicValues[string(record.Topic)] = &Topic{Name: record.Topic, Messages: []uint64{offset}}
			} else {
				v.Messages = append(v.Messages, offset)
			}
		}
		for _, t := range topicValues {
			v := s.topics.Match(t.Name)
			if len(v) == 0 {
				s.topics.Set(t.Name, t.Messages)
			} else {
				v[0].Messages = append(v[0].Messages, t.Messages...)
				s.topics.Set(t.Name, v[0].Messages)
			}
		}
		return nil
	}, ConsumerOptions{
		FromOffset:   0,
		MaxBatchSize: 2500,
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
				ToOffset: snapshotDescription.MessagesOffset,
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
		s.topics.store.Load(snapshotDescription.Topics)

		L(ctx).Info("loaded snapshot", zap.Uint64("current_log_offset", s.CurrentOffset()))
	}
	return nil
}
func (s *messageLog) Snapshot() ([]byte, error) {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()
	topics, err := s.topics.store.Dump()
	if err != nil {
		return nil, err
	}
	return json.Marshal(Snapshot{
		Remote:         s.id,
		StateOffset:    s.CurrentStateOffset(),
		MessagesOffset: s.CurrentOffset(),
		Topics:         topics,
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
		//	s.topics.Insert(b[idx].Topic, offset)
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

func (s *messageLog) Dump(sink io.Writer, lastOffset uint64, whence int) error {
	encoder := json.NewEncoder(sink)
	r, err := s.log.ReaderFrom(0)
	defer r.Close()
	if err != nil {
		return err
	}
	limit, err := r.Seek(int64(lastOffset), whence)
	if err != nil {
		return err
	}
	_, err = s.GetRecords(nil, 0, func(offset uint64, topic []byte, ts int64, payload []byte) error {
		if int64(offset) >= limit {
			return io.EOF
		}
		return encoder.Encode(api.Record{Timestamp: ts, Payload: payload, Topic: topic})
	})
	if err == io.EOF {
		return nil
	}
	return err
}

func (s *messageLog) Load(source io.Reader) error {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()
	return s.load(source)
}
func (s *messageLog) load(source io.Reader) error {
	err := s.log.Delete()
	if err != nil {
		return err
	}
	log, err := commitlog.Open(s.log.Datadir(), 250)
	if err != nil {
		return err
	}
	s.log = log
	dec := json.NewDecoder(source)
	record := &api.Record{}
	for {
		err := dec.Decode(&record)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		payload, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		_, err = s.log.Write(payload)
		if err != nil {
			return err
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
			Name:         topics[idx].Name,
			MessageCount: uint64(len(topics[idx].Messages)),
		}
	}
	return out
}
func (s *messageLog) GetRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error) {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	return s.getRecords(patterns, fromOffset, f)
}
func (s *messageLog) Consume(ctx context.Context, processor func(context.Context, Batch) error, opts ConsumerOptions) error {
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
	session := NewSession(ctx, r, opts)
	for {
		select {
		case <-ctx.Done():
			return nil
		case batch := <-session.Ready():
			err := processor(ctx, batch)
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

func (s *messageLog) GetTopics(ctx context.Context, pattern []byte, processor func(context.Context, Batch) error) error {

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
		session := NewSession(ctx, r, ConsumerOptions{MaxBatchSize: 10, FromOffset: 0, EOFBehaviour: EOFBehaviourExit})
	loop:
		for {
			select {
			case <-ctx.Done():
				return nil
			case batch, ok := <-session.Ready():
				err := processor(ctx, batch)
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
