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
)

type Segment interface {
	FilePath() string
	Name() string
	BaseOffset() uint64
	CurrentOffset() uint64
	Size() uint64
	ReadEntryAt(buf []byte, logOffset int64) (Entry, error)
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

func openSegment(datadir string, id uint64, maxRecordCount uint64) (Segment, error) {
	filename := segmentName(datadir, id)
	if !fileExists(filename) {
		return nil, ErrSegmentDoesNotExist
	}
	idx, err := openIndex(datadir, id, maxRecordCount)
	if err != nil {
		return nil, err
	}
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	position, err := fd.Seek(0, 2)
	if err != nil {
		return nil, err
	}
	s := &segment{
		path:           filename,
		baseOffset:     id,
		currentOffset:  uint64(position),
		maxRecordCount: maxRecordCount,
		index:          idx,
		fd:             fd,
	}
	// TODO: check segment integrity ?
	return s, nil
}

func (e *segment) ReadEntryAt(buf []byte, logOffset int64) (Entry, error) {

	offset := uint64(logOffset) - e.baseOffset
	position, err := e.index.readPosition(offset)
	if err != nil {
		return nil, err
	}
	return readEntry(&readerAt{pos: position, r: e.fd}, buf)
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
