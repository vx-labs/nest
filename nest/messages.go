package nest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/tysontate/gommap"
	"github.com/vx-labs/nest/commitlog"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

var (
	messagesBucketName []byte = []byte("messages")
	appliedIndexKey    []byte = []byte("_index")
	encoding                  = binary.BigEndian
)

type RecordConsumer func(offset uint64, topic []byte, ts int64, payload []byte) error
type MessageLog interface {
	io.Closer
	Dump(w io.Writer, lastOffset uint64, whence int) error
	Load(w io.Reader) error
	LoadState(uint64, io.Reader) error
	PutRecords(stateOffset uint64, b []*api.Record) error
	GetRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error)
	CurrentStateOffset() uint64
	CurrentOffset() uint64
	SetCurrentStateOffset(v uint64)
}

type messageLog struct {
	restorelock   sync.RWMutex
	datadir       string
	stateOffset   gommap.MMap
	stateOffsetFd *os.File
	log           commitlog.CommitLog
}

type compatLogger struct {
	l *zap.Logger
}

func (c *compatLogger) Debugf(string, ...interface{})   {}
func (c *compatLogger) Infof(string, ...interface{})    {}
func (c *compatLogger) Warningf(string, ...interface{}) {}
func (c *compatLogger) Errorf(string, ...interface{})   {}

func NewMessageLog(datadir string) (MessageLog, error) {

	log, err := commitlog.Open(path.Join(datadir, "messages"), 250)
	if err != nil {
		return nil, err
	}
	statePath := path.Join(datadir, "messages.json")
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

	return &messageLog{
		datadir:       datadir,
		log:           log,
		stateOffset:   mmapedData,
		stateOffsetFd: fd,
	}, nil
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
		_, err := s.log.Write(payload)
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

func (s *messageLog) Dump(sink io.Writer, lastOffset uint64, whence int) error {
	encoder := json.NewEncoder(sink)
	r, err := s.log.ReaderFrom(0)
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
func (s *messageLog) LoadState(offset uint64, source io.Reader) error {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()

	err := s.load(source)
	if err != nil {
		return err
	}
	s.SetCurrentStateOffset(offset)
	return nil
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

func (s *messageLog) GetRecords(patterns [][]byte, fromOffset int64, f RecordConsumer) (int64, error) {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	r, err := s.log.ReaderFrom(0)
	if err != nil {
		return fromOffset, err
	}
	current, err := r.Seek(fromOffset, io.SeekStart)
	if err != nil {
		return current, err
	}
	buf := make([]byte, 200*1000*1000)
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
