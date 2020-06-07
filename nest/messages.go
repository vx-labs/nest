package nest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
	"github.com/vx-labs/nest/nest/api"
	"github.com/vx-labs/nest/stream"
	"go.uber.org/zap"
)

var (
	messagesBucketName []byte = []byte("messages")
	appliedIndexKey    []byte = []byte("_index")
	encoding                  = binary.BigEndian
)

type RecordConsumer func(topic []byte, ts int64, payload []byte) error
type MessageLog interface {
	io.Closer
	Dump(w io.Writer) error
	Load(w io.Reader) error
	SetApplied(timestamp int64, index uint64) error
	AppliedIndex(timestamp int64) uint64
	PutRecords(index uint64, timestamp int64, b []*api.Record) error
	GetRecords(ctx context.Context, patterns [][]byte, fromTimestamp int64, f RecordConsumer) (int64, error)
	Consume(ctx context.Context, patterns [][]byte, fromTimestamp int64, f RecordConsumer) error
	GC()
}

type messageLog struct {
	restorelock sync.RWMutex
	db          *badger.DB
	stream      stream.Stream
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
	s, err := stream.Open("messages", datadir)
	return &messageLog{
		db:     db,
		stream: s,
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
	s.stream.Close()
	return s.db.Close()
}

func int64ToBytes(u int64) []byte {
	return uint64ToBytes(uint64(u))
}
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
func bytesToInt64(b []byte) int64 {
	return int64(bytesToUint64(b))
}
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func (s *messageLog) AppliedIndex(timestamp int64) uint64 {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	ts := uint64(timestamp)
	tx := s.db.NewTransactionAt(ts, false)
	defer tx.Discard()
	return s.appliedIndex(tx)
}

func (s *messageLog) appliedIndex(tx *badger.Txn) uint64 {
	item, err := tx.Get(appliedIndexKey)
	if err != nil {
		return 0
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		return 0
	}
	return bytesToUint64(value)
}
func (s *messageLog) PutRecords(index uint64, timestamp int64, b []*api.Record) error {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	ts := uint64(timestamp)
	tx := s.db.NewTransactionAt(ts, true)
	defer tx.Discard()
	if s.appliedIndex(tx) > index {
		return errors.New("outdated index")
	}
	err := tx.Set(appliedIndexKey, uint64ToBytes(index))
	if err != nil {
		return err
	}
	for idx := range b {
		entry := badger.NewEntry(b[idx].Topic, b[idx].Payload)
		err := tx.SetEntry(entry)
		if err != nil {
			return err
		}
		err = json.NewEncoder(s.stream.Writer(b[idx].Topic)).Encode(b[idx])
		if err != nil {
			log.Print(err)
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
func (s *messageLog) SetApplied(timestamp int64, index uint64) error {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	ts := uint64(timestamp)
	tx := s.db.NewTransactionAt(ts, true)
	defer tx.Discard()
	if s.appliedIndex(tx) > index {
		return errors.New("outdated index")
	}
	tx.Set(appliedIndexKey, uint64ToBytes(index))
	return tx.CommitAt(ts, nil)
}
func (s *messageLog) GetRecords(ctx context.Context, patterns [][]byte, fromTimestamp int64, f RecordConsumer) (int64, error) {
	s.restorelock.RLock()
	defer s.restorelock.RUnlock()
	stream := s.db.NewStreamAt(uint64(time.Now().UnixNano()))
	stream.ChooseKey = func(item *badger.Item) bool {
		if bytes.Equal(item.Key(), appliedIndexKey) {
			return false
		}
		matched := len(patterns) == 0
		if !matched {
			for _, pattern := range patterns {
				if match(pattern, item.Key()) {
					return true
				}
			}
		}
		return matched
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
			if fromTimestamp == -1 || item.Version() > uint64(fromTimestamp) {
				kv := &pb.KV{
					Key:       item.KeyCopy(nil),
					Value:     valCopy,
					UserMeta:  []byte{item.UserMeta()},
					Version:   item.Version(),
					ExpiresAt: item.ExpiresAt(),
				}

				list.Kv = append(list.Kv, kv)
				if fromTimestamp == -1 {
					break
				}
			}
		}
		return list, nil
	}
	var lastRecord int64 = fromTimestamp
	stream.Send = func(list *pb.KVList) error {
		length := len(list.Kv) - 1
		for idx := range list.Kv {
			kv := list.Kv[length-idx]
			record := &api.Record{
				Timestamp: int64(kv.Version),
				Payload:   kv.Value,
				Topic:     kv.Key,
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
				err := f(record.Topic, record.Timestamp, record.Payload)
				if err != nil {
					return err
				}
			}
			lastRecord = record.Timestamp
		}
		return nil
	}
	err := stream.Orchestrate(ctx)
	return lastRecord, err
}
func (s *messageLog) Consume(ctx context.Context, patterns [][]byte, fromTimestamp int64, f RecordConsumer) error {

	notifications := make(chan struct{}, 1)

	notifications <- struct{}{}
	defer close(notifications)

	go func() {
		var lastSeen int64 = fromTimestamp
		var err error
		for range notifications {
			lastSeen, err = s.GetRecords(ctx, patterns, lastSeen, f)
			if err != nil {
				log.Print(err) // TODO: use logger
			}
		}
	}()

	return s.db.Subscribe(ctx, func(list *pb.KVList) error {
		select {
		case notifications <- struct{}{}:
		default:
		}
		return nil
	}, nil)
}
