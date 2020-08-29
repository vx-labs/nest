package commitlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"
)

var (
	ErrSegmentAlreadyExists = errors.New("segment already exists")
	ErrSegmentDoesNotExist  = errors.New("segment does not exist")
	ErrSegmentFull          = errors.New("segment is full")
	ErrSegmentCorrupt       = errors.New("segment corrupted")
	ErrCorruptedEntry       = errors.New("entry corrupted")
)

type Segment interface {
	FilePath() string
	Name() string
	BaseOffset() uint64
	CurrentOffset() uint64
	Size() uint64
	Delete() error
	WriteEntry(ts uint64, buf []byte) (uint64, error)
	Earliest() uint64
	Latest() uint64
	LookupTimestamp(ts uint64) uint64
	TruncateAfter(offset uint64) error
	io.WriterTo
	io.Reader
	io.Closer
	io.Seeker
}

type segment struct {
	mtx             sync.Mutex
	baseOffset      uint64
	currentOffset   uint64
	currentPosition uint64
	fd              *os.File
	offsetIndex     Index
	timestampIndex  Index
	maxRecordCount  uint64
	path            string
}

func segmentName(datadir string, id uint64) string {
	return path.Join(datadir, fmt.Sprintf("%d.log", id))
}

func (s *segment) Close() error {
	err := s.offsetIndex.Close()
	if err != nil {
		return err
	}
	err = s.timestampIndex.Close()
	if err != nil {
		return err
	}
	return s.fd.Close()
}
func (s *segment) Delete() error {
	s.Close()
	err := os.Remove(s.offsetIndex.FilePath())
	if err != nil {
		return err
	}
	err = os.Remove(s.timestampIndex.FilePath())
	if err != nil {
		return err
	}
	return os.Remove(s.FilePath())
}

func (i *segment) FilePath() string {
	return i.path
}
func (i *segment) BaseOffset() uint64 {
	return i.baseOffset
}
func (i *segment) CurrentOffset() uint64 {
	return i.currentOffset
}
func (i *segment) Size() uint64 {
	return i.currentPosition
}
func (i *segment) Name() string {
	return i.fd.Name()
}
func (i *segment) Seek(offset int64, whence int) (n int64, err error) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	if uint64(offset) < i.baseOffset {
		return 0, io.EOF
	}
	relOffset := uint64(offset) - i.baseOffset
	if relOffset >= i.maxRecordCount {
		return 0, io.EOF
	}
	fileOffset, err := i.offsetIndex.readPosition(uint64(offset) - i.baseOffset)
	if err != nil {
		return 0, err
	}
	if relOffset != 0 && fileOffset == 0 {
		_, err = i.fd.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
	} else {
		_, err = i.fd.Seek(int64(fileOffset), io.SeekStart)
		if err != nil {
			return 0, err
		}
	}
	return offset, nil
}
func (i *segment) WriteTo(w io.Writer) (n int64, err error) {
	i.mtx.Lock()
	defer i.mtx.Unlock()
	return io.Copy(w, i.fd)
}

func createSegment(datadir string, id uint64, maxRecordCount uint64) (Segment, error) {
	filename := segmentName(datadir, id)
	if fileExists(filename) {
		return nil, ErrSegmentAlreadyExists
	}
	idx, err := createIndex(datadir, "offsets", id, maxRecordCount)
	if err != nil {
		return nil, err
	}
	tidx, err := createIndex(datadir, "timestamps", id, maxRecordCount)
	if err != nil {
		return nil, err
	}

	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0650)
	if err != nil {
		return nil, err
	}
	s := &segment{
		path:           filename,
		baseOffset:     id,
		currentOffset:  0,
		maxRecordCount: maxRecordCount,
		offsetIndex:    idx,
		timestampIndex: tidx,
		fd:             fd,
	}
	return s, nil
}

func openSegment(datadir string, id uint64, maxRecordCount uint64, write bool) (Segment, error) {
	filename := segmentName(datadir, id)
	if !fileExists(filename) {
		return nil, ErrSegmentDoesNotExist
	}
	idx, err := openIndex(datadir, "offsets", id, maxRecordCount)
	if err != nil {
		return nil, err
	}
	tidx, err := openIndex(datadir, "timestamps", id, maxRecordCount)
	if err != nil {
		return nil, err
	}
	perm := os.O_RDONLY
	if write {
		perm = os.O_RDWR
	}
	fd, err := os.OpenFile(filename, perm, 0650)
	if err != nil {
		return nil, err
	}
	position, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	offset, err := checkSegmentIntegrity(fd, maxRecordCount)
	if err != nil {
		return nil, err
	}
	s := &segment{
		path:            filename,
		baseOffset:      id,
		currentOffset:   offset,
		currentPosition: uint64(position),
		maxRecordCount:  maxRecordCount,
		offsetIndex:     idx,
		timestampIndex:  tidx,
		fd:              fd,
	}
	return s, nil
}

func checkSegmentIntegrity(r io.ReadSeeker, maxRecordCount uint64) (uint64, error) {
	_, err := r.Seek(0, io.SeekStart)
	if err != nil {
		return 0, ErrSegmentCorrupt
	}
	buf := make([]byte, EntryHeaderSize)
	var offset uint64
	for offset = 0; offset < maxRecordCount; offset++ {
		n, err := r.Read(buf)
		if err == io.EOF {
			return offset, nil
		}
		if n != EntryHeaderSize {
			return offset, ErrSegmentCorrupt
		}
		_, err = r.Seek(int64(encoding.Uint64(buf[0:8])), io.SeekCurrent)
		if err != nil {
			return offset, ErrSegmentCorrupt
		}
	}
	return offset, nil
}

func (e *segment) LookupTimestamp(ts uint64) uint64 {
	idx := sort.Search(int(e.CurrentOffset()), func(i int) bool {
		n, err := e.timestampIndex.readPosition(uint64(i))
		if err != nil {
			return false
		}
		return uint64(n) >= ts
	})
	return uint64(idx) + e.baseOffset
}
func (e *segment) Earliest() uint64 {
	n, err := e.timestampIndex.readPosition(0)
	if err != nil {
		return 0
	}
	return n
}
func (e *segment) Latest() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.CurrentOffset() == 0 {
		return 0
	}
	n, err := e.timestampIndex.readPosition(e.CurrentOffset() - 1)
	if err != nil {
		return 0
	}
	return n
}
func (e *segment) Read(p []byte) (int, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.fd.Read(p)
}

func (e *segment) WriteEntry(ts uint64, value []byte) (uint64, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.currentOffset >= e.maxRecordCount {
		return 0, ErrSegmentFull
	}
	entry := newEntry(ts, e.baseOffset+e.currentOffset, value)
	n, err := writeEntry(entry, e.fd)
	if err != nil {
		return 0, err
	}
	err = e.offsetIndex.writePosition(e.currentOffset, e.currentPosition)
	if err != nil {
		// Index update failed: return an error and do not update write cursor
		return 0, err
	}
	err = e.timestampIndex.writePosition(e.currentOffset, ts)
	if err != nil {
		// Index update failed: return an error and do not update write cursor
		return 0, err
	}
	writtenOffset := e.currentOffset
	e.currentOffset++
	e.currentPosition += uint64(n)
	return writtenOffset, nil
}

// Truncate the segment *after* the given offset. You must ensure no one is reading this segment before truncating it.
func (e *segment) TruncateAfter(offset uint64) error {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.currentOffset < offset {
		return nil
	}
	delta := e.currentOffset - offset
	e.currentOffset = offset + 1
	newPosition, err := e.offsetIndex.readPosition(e.currentOffset)
	if err != nil {
		return err
	}
	e.currentPosition = newPosition
	e.fd.Seek(io.SeekStart, int(newPosition))
	var i uint64
	for i = 0; i < delta; i++ {
		err := e.offsetIndex.writePosition(e.currentOffset+i, 0)
		if err != nil {
			// Index update failed: return an error and do not update write cursor
			return err
		}
		err = e.timestampIndex.writePosition(e.currentOffset+i, 0)
		if err != nil {
			// Index update failed: return an error and do not update write cursor
			return err
		}
	}
	err = e.fd.Truncate(int64(newPosition))
	if err != nil {
		return err
	}
	return e.fd.Sync()
}
