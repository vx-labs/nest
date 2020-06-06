package commitlog

import (
	"io"
)

type readerAt struct {
	pos uint64
	r   io.ReaderAt
}

func (r *readerAt) Read(buf []byte) (int, error) {
	n, err := r.r.ReadAt(buf, int64(r.pos))
	r.pos += uint64(n)

	return n, err
}

type writerAt struct {
	pos uint64
	w   io.WriterAt
}

func (r *writerAt) Write(buf []byte) (int, error) {
	n, err := r.w.WriteAt(buf, int64(r.pos))
	r.pos += uint64(n)
	return n, err
}
