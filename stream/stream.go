package stream

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/vx-labs/nest/commitlog"
)

type stream struct {
	name   string
	shards []Shard
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
	for idx := range s.shards {
		err := s.shards[idx].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *stream) Writer(shardKey []byte) io.Writer {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.shards[hashShardKey(shardKey, len(s.shards))]
}
func (s *stream) Reader(shardKey []byte, offset uint64) (io.Reader, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.shards[hashShardKey(shardKey, len(s.shards))].ReaderFrom(offset)
}

func createStream(name, datadir string, shardCount int) (Stream, error) {
	shardsPath := path.Join(datadir, "streams", name, "shards")
	err := os.MkdirAll(shardsPath, 0750)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create shards directory")
	}
	shards := make([]Shard, shardCount)
	for idx := range shards {
		shardPath := path.Join(shardsPath, fmt.Sprintf("%d", idx))
		err := os.Mkdir(shardPath, 0750)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create shard directory")
		}
		log, err := commitlog.Create(shardPath, 250)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create commitlog for shard")
		}
		shards[idx] = newShard(log)
	}
	return &stream{name: name, shards: shards}, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func openStream(name, datadir string) (Stream, error) {
	shardsPath := path.Join(datadir, "streams", name, "shards")

	if !fileExists(shardsPath) {
		return nil, errors.New("stream does not exist")
	}
	shardFolders, err := ioutil.ReadDir(shardsPath)
	if err != nil {
		return nil, errors.New("corrupted stream data directory")
	}
	shards := make([]Shard, len(shardFolders))
	for idx := range shards {
		log, err := commitlog.Open(path.Join(shardsPath, shardFolders[idx].Name()), 250)
		if err != nil {
			return nil, err
		}
		shards[idx] = newShard(log)
	}
	return &stream{name: name, shards: shards}, nil
}

func Open(name, datadir string) (Stream, error) {
	if fileExists(path.Join(datadir, "streams", name)) {
		return openStream(name, datadir)
	}
	return createStream(name, datadir, 3)
}
