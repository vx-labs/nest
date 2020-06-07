package stream

import "io"

type Shard interface {
	io.Writer
	io.Closer
	ReaderFrom(offset uint64) (io.ReadSeeker, error)
}

type shard struct {
	backend Log
}

func newShard(backend Log) Shard {
	return &shard{backend: backend}
}

func (s *shard) Close() error                                    { return s.backend.Close() }
func (s *shard) Write(p []byte) (int, error)                     { return s.backend.Write(p) }
func (s *shard) ReaderFrom(offset uint64) (io.ReadSeeker, error) { return s.backend.ReaderFrom(offset) }
