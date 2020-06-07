package stream

import "io"

type Log interface {
	io.WriteCloser
	ReaderFrom(offset uint64) (io.Reader, error)
}

type Entry struct {
	StreamID    uint64
	ShardID     uint64
	PayloadSize uint64
	Payload     []byte
}
