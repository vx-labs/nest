package commitlog

import (
	"io"
)

type ReadSeekCloser interface {
	io.ReadCloser
	io.Seeker
}

type readerAt struct {
	pos uint64
	r   io.ReaderAt
}

func (r *readerAt) Read(buf []byte) (int, error) {
	n, err := r.r.ReadAt(buf, int64(r.pos))
	r.pos += uint64(n)

	return n, err
}
