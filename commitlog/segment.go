package commitlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
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
	ReadEntryAt(buf []byte, logOffset int64) (Entry, error)
	ReaderFrom(offset uint64) io.ReadSeeker
	Delete() error
	io.Closer
	io.Writer
}

type segment struct {
	mtx             sync.Mutex
	baseOffset      uint64
	currentOffset   uint64
	currentPosition uint64
	fd              *os.File
	index           Index
	maxRecordCount  uint64
	path            string
}

func segmentName(datadir string, id uint64) string {
	return path.Join(datadir, fmt.Sprintf("%d.log", id))
}

func (s *segment) Close() error {
	err := s.index.Close()
	if err != nil {
		return err
	}
	return s.fd.Close()
}
func (s *segment) Delete() error {
	s.Close()
	err := os.Remove(s.index.FilePath())
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

func createSegment(datadir string, id uint64, maxRecordCount uint64) (Segment, error) {
	filename := segmentName(datadir, id)
	if fileExists(filename) {
		return nil, ErrSegmentAlreadyExists
	}
	idx, err := createIndex(datadir, id, maxRecordCount)
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
		index:          idx,
		fd:             fd,
	}
	return s, nil
}

func openSegment(datadir string, id uint64, maxRecordCount uint64, write bool) (Segment, error) {
	filename := segmentName(datadir, id)
	if !fileExists(filename) {
		return nil, ErrSegmentDoesNotExist
	}
	idx, err := openIndex(datadir, id, maxRecordCount)
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
		index:           idx,
		fd:              fd,
	}
	return s, nil
}

func checkSegmentIntegrity(r io.ReadSeeker, size uint64) (uint64, error) {
	_, err := r.Seek(0, io.SeekStart)
	if err != nil {
		return 0, ErrSegmentCorrupt
	}
	buf := make([]byte, entryHeaderSize)
	var offset uint64
	for offset = 0; offset < size; offset++ {
		n, err := r.Read(buf)
		if err == io.EOF {
			return offset, nil
		}
		if n != entryHeaderSize {
			return offset, ErrSegmentCorrupt
		}
		_, err = r.Seek(int64(encoding.Uint64(buf[0:8])), io.SeekCurrent)
		if err != nil {
			return offset, ErrSegmentCorrupt
		}
	}
	return offset, nil
}

func (e *segment) ReadEntryAt(buf []byte, logOffset int64) (Entry, error) {
	offset := uint64(logOffset) - e.baseOffset
	position, err := e.index.readPosition(offset)
	if err != nil {
		return nil, err
	}
	return readEntry(&readerAt{pos: position, r: e.fd}, buf)
}

func (e *segment) ReaderFrom(offset uint64) io.ReadSeeker {
	return &segmentReader{
		headerBuf: make([]byte, entryHeaderSize),
		offset:    offset,
		segment:   e,
	}
}
func (e *segment) Write(value []byte) (int, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.currentOffset >= e.maxRecordCount {
		return 0, ErrSegmentFull
	}
	entry := newEntry(e.currentOffset, value)
	n, err := writeEntry(entry, &writerAt{pos: e.currentPosition, w: e.fd})
	if err != nil {
		return n, err
	}
	err = e.index.writePosition(e.currentOffset, e.currentPosition)
	if err != nil {
		// Index update failed: retturn an error and do not update write cursor
		return 0, err
	}
	e.currentOffset++
	e.currentPosition += uint64(n)
	return len(value), nil
}

type segmentReader struct {
	mtx          sync.Mutex
	currentEntry Entry
	headerBuf    []byte
	offset       uint64
	entryPos     int
	segment      Segment
}

func (s *segmentReader) Seek(offset int64, whence int) (int64, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = int64(s.segment.BaseOffset()) + offset
	case io.SeekCurrent:
		newOffset = int64(s.offset) + offset
	case io.SeekEnd:
		newOffset = int64(s.segment.CurrentOffset()) + offset
	}
	if newOffset > int64(s.segment.CurrentOffset()) {
		s.offset = s.segment.CurrentOffset()
	} else if newOffset < int64(s.segment.BaseOffset()) {
		s.offset = s.segment.BaseOffset()
	} else {
		s.offset = uint64(newOffset)
	}
	s.entryPos = 0
	s.currentEntry = nil

	return int64(s.offset), nil
}
func (s *segmentReader) Read(p []byte) (int, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	var err error
	if s.currentEntry != nil && len(s.currentEntry.Payload()) == s.entryPos {
		s.offset++
		s.entryPos = 0
		s.currentEntry = nil
	}
	if s.currentEntry == nil {
		s.currentEntry, err = s.segment.ReadEntryAt(s.headerBuf, int64(s.offset))
		if err != nil {
			return 0, err
		}

		if !s.currentEntry.IsValid() {
			return 0, ErrCorruptedEntry
		}
	}

	n := copy(p, s.currentEntry.Payload()[s.entryPos:])
	s.entryPos += n
	return n, nil
}
