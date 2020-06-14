package commitlog

import (
	"io"
	"sync"
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

type writerAt struct {
	pos uint64
	w   io.WriterAt
}

func (r *writerAt) Write(buf []byte) (int, error) {
	n, err := r.w.WriteAt(buf, int64(r.pos))
	r.pos += uint64(n)
	return n, err
}

type offsetReader struct {
	mtx     sync.Mutex
	offsets []uint64
	cursor  int
	backend io.ReadSeeker
}

func OffsetReader(offsets []uint64, r io.ReadSeeker) io.Reader {
	return &offsetReader{
		backend: r,
		offsets: offsets,
		cursor:  0,
	}
}

func (o *offsetReader) Read(p []byte) (int, error) {
	o.mtx.Lock()
	defer o.mtx.Unlock()
	if o.cursor >= len(o.offsets) {
		return 0, io.EOF
	}
	_, err := o.backend.Seek(int64(o.offsets[o.cursor]), io.SeekStart)
	if err != nil {
		return 0, err
	}
	n, err := o.backend.Read(p)
	if err == nil {
		o.cursor++
	}
	return n, err
}
