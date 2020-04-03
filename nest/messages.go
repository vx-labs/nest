package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"github.com/golang/protobuf/proto"
	"github.com/vx-labs/nest/nest/api"
	"go.uber.org/zap"
)

var (
	messagesBucketName []byte = []byte("messages")
	encoding                  = binary.BigEndian
)

type RecordConsumer func(topic []byte, ts int64, payload []byte) error
type MessageLog interface {
	io.Closer
	Dump(w io.Writer) error
	Load(w io.Reader) error
	PutRecords(timestamp int64, b []*api.Record) error
	GetRecords(ctx context.Context, patterns [][]byte, fromTimestamp int64, f RecordConsumer) error
	GC()
}

type messageLog struct {
	restorelock sync.RWMutex
	db          *badger.DB
}

type compatLogger struct {
	l *zap.Logger
}

func (c *compatLogger) Debugf(string, ...interface{})   {}
func (c *compatLogger) Infof(string, ...interface{})    {}
func (c *compatLogger) Warningf(string, ...interface{}) {}
func (c *compatLogger) Errorf(string, ...interface{})   {}

func NewMessageLog(ctx context.Context, datadir string) (MessageLog, error) {
	opts := badger.DefaultOptions(path.Join(datadir, "badger"))
	opts.Logger = &compatLogger{l: L(ctx)}
	opts.NumVersionsToKeep = -1
	db, err := badger.OpenManaged(opts)
	if err != nil {
		return nil, err
	}
	return &messageLog{
		db: db,
	}, nil
}

func (s *messageLog) GC() {
again:
	err := s.db.RunValueLogGC(0.7)
	if err == nil {
		goto again
	}
}
func (s *messageLog) Close() error {
	return s.db.Close()
}

func int64ToBytes(u int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(u))
	return buf
}
func bytesToInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
func (s *messageLog) PutRecords(timestamp int64, b []*api.Record) error {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	ts := uint64(timestamp)
	tx := s.db.NewTransactionAt(ts, true)
	defer tx.Discard()
	for idx := range b {
		payload, err := proto.Marshal(b[idx])
		if err != nil {
			return err
		}
		entry := badger.NewEntry(int64ToBytes(b[idx].Timestamp), payload)
		err = tx.SetEntry(entry)
		if err != nil {
			return err
		}
	}
	return tx.CommitAt(ts, nil)
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

func (s *messageLog) Dump(sink io.Writer) error {
	stream := s.db.NewStreamAt(uint64(time.Now().UnixNano()))
	_, err := stream.Backup(sink, 0)
	return err
}
func (s *messageLog) Load(source io.Reader) error {
	s.restorelock.Lock()
	defer s.restorelock.Unlock()
	return s.db.Load(source, 15)
}
func (s *messageLog) GetRecords(ctx context.Context, patterns [][]byte, fromTimestamp int64, f RecordConsumer) error {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	stream := s.db.NewStreamAt(uint64(time.Now().UnixNano()))
	stream.ChooseKey = func(item *badger.Item) bool {
		return fromTimestamp < bytesToInt64(item.Key())
	}
	stream.KeyToList = func(key []byte, iterator *badger.Iterator) (*pb.KVList, error) {
		list := &pb.KVList{}
		for ; iterator.Valid(); iterator.Next() {
			item := iterator.Item()
			if item.IsDeletedOrExpired() {
				break
			}
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return nil, err
			}
			if item.Version() >= uint64(fromTimestamp) {
				kv := &pb.KV{
					Key:       item.KeyCopy(nil),
					Value:     valCopy,
					UserMeta:  []byte{item.UserMeta()},
					Version:   item.Version(),
					ExpiresAt: item.ExpiresAt(),
				}
				list.Kv = append(list.Kv, kv)
			}
		}
		return list, nil
	}
	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.Kv {
			record := &api.Record{}
			err := proto.Unmarshal(kv.Value, record)
			if err != nil {
				return err
			}
			matched := len(patterns) == 0
			if !matched {
				for _, pattern := range patterns {
					if match(pattern, record.Topic) {
						matched = true
						break
					}
				}
			}
			if matched {
				err = f(record.Topic, record.Timestamp, record.Payload)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
	return stream.Orchestrate(ctx)
}
