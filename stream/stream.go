package stream

import (
	"hash/fnv"
	"io"
	"sync"
)

type stream struct {
	ID     uint64
	Name   string
	Shards []Shard
	mtx    sync.Mutex
}

func hashShardKey(key []byte, shardCount int) int {
	hash := fnv.New32()
	hash.Write(key)
	return int(hash.Sum32()) % shardCount
}

type Stream interface {
	io.Closer
	Writer(shardKey []byte) io.Writer
	Reader(shardKey []byte, offset uint64) (io.Reader, error)
}

func (s *stream) Close() error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for idx := range s.Shards {
		err := s.Shards[idx].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *stream) Writer(shardKey []byte) io.Writer {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.Shards[hashShardKey(shardKey, len(s.Shards))]
}
func (s *stream) Reader(shardKey []byte, offset uint64) (io.Reader, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.Shards[hashShardKey(shardKey, len(s.Shards))].ReaderFrom(offset)
}
