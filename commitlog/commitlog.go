package commitlog

import (
	"io"
	"os"
	"path"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrCorruptedLog = errors.New("corrupted commitlog")
)

type commitLog struct {
	datadir               string
	mtx                   sync.Mutex
	activeSegment         Segment
	segments              []uint64
	segmentMaxRecordCount uint64
}

type CommitLog interface {
	io.Closer
	WriteEntry(ts uint64, value []byte) (uint64, error)
	Delete() error
	Reader() ReadSeekCloser
	ResolveTimestamp(ts uint64) uint64
	Offset() uint64
	Datadir() string
	io.WriterTo
}

func Open(datadir string, segmentMaxRecordCount uint64) (CommitLog, error) {
	if fileExists(path.Join(datadir, "0.log")) {
		return open(datadir, segmentMaxRecordCount)
	}
	err := os.MkdirAll(datadir, 0750)
	if err != nil {
		return nil, err
	}
	return create(datadir, segmentMaxRecordCount)
}

func create(datadir string, segmentMaxRecordCount uint64) (CommitLog, error) {
	l := &commitLog{
		datadir:               datadir,
		segmentMaxRecordCount: segmentMaxRecordCount,
	}
	return l, l.appendSegment(0)
}

func open(datadir string, segmentMaxRecordCount uint64) (CommitLog, error) {
	l := &commitLog{
		datadir:               datadir,
		segmentMaxRecordCount: segmentMaxRecordCount,
	}
	var offset uint64 = 0
	for {
		segment, err := openSegment(datadir, offset, segmentMaxRecordCount, true)
		if err != nil {
			if err == ErrSegmentDoesNotExist {
				break
			}
			return nil, ErrCorruptedLog
		}
		l.segments = append(l.segments, offset)
		if l.activeSegment != nil {
			if l.activeSegment.BaseOffset() < segment.BaseOffset() {
				l.activeSegment.Close()
				l.activeSegment = segment
			} else {
				segment.Close()
			}
		} else {
			l.activeSegment = segment
		}
		offset += uint64(segmentMaxRecordCount)
	}
	return l, nil
}

func (e *commitLog) Offset() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.activeSegment.CurrentOffset() + e.activeSegment.BaseOffset()
}
func (e *commitLog) Close() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if e.activeSegment != nil {
		return e.activeSegment.Close()
	}
	return nil
}
func (e *commitLog) Datadir() string {
	return e.datadir
}
func (e *commitLog) Delete() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.activeSegment != nil {
		e.activeSegment.Close()
	}
	for _, idx := range e.segments {
		segment, err := openSegment(e.datadir, idx, e.segmentMaxRecordCount, false)
		if err == nil {
			err = segment.Delete()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *commitLog) appendSegment(offset uint64) error {
	segment, err := createSegment(e.datadir, offset, e.segmentMaxRecordCount)
	if err != nil {
		return errors.Wrap(err, "failed to create new segment")
	}
	e.segments = append(e.segments, offset)
	if e.activeSegment != nil {
		err = e.activeSegment.Close()
		if err != nil {
			return err
		}
	}
	e.activeSegment = segment
	return nil
}

// lookupOffset returns the baseOffset (and thus, the segment id) of the segment containing the provided offset
func (e *commitLog) lookupOffset(offset uint64) uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		return e.segments[i] > offset
	})
	return e.segments[idx-1]
}
func (e *commitLog) lookupTimestamp(ts uint64) uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		seg, err := e.readSegment(e.segments[i])
		if err != nil {
			return true
		}
		defer seg.Close()
		return seg.Earliest() >= ts
	})
	return e.segments[idx-1]
}

func (e *commitLog) currentOffset() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.activeSegment.CurrentOffset() + e.activeSegment.BaseOffset()
}

func (e *commitLog) readSegment(id uint64) (Segment, error) {
	return openSegment(e.datadir, id, e.segmentMaxRecordCount, false)
}

func (e *commitLog) Reader() ReadSeekCloser {
	return &commitlogReader{
		currentOffset:  0,
		log:            e,
		currentSegment: nil,
		segmentSize:    e.segmentMaxRecordCount,
		currentReader:  nil,
	}
}
func (e *commitLog) ResolveTimestamp(ts uint64) uint64 {
	idx := e.lookupTimestamp(ts)
	segment, err := e.readSegment(idx)
	if err != nil {
		return 0
	}
	defer segment.Close()
	r := segment.ReaderFromTimestamp(ts)
	n, _ := r.Seek(0, io.SeekCurrent)
	return uint64(n)
}

func (e *commitLog) WriteTo(w io.Writer) (n int64, err error) {
	var total int64 = 0
	for _, id := range e.segments {
		seg, err := e.readSegment(id)
		if err != nil {
			return total, err
		}
		n, err := seg.WriteTo(w)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
func (e *commitLog) ReaderFrom(offset uint64) (ReadSeekCloser, error) {
	idx := e.lookupOffset(offset)
	segment, err := e.readSegment(idx)
	if err != nil {
		return nil, err
	}
	return &commitlogReader{
		currentOffset:  offset,
		log:            e,
		currentSegment: segment,
		segmentSize:    e.segmentMaxRecordCount,
		currentReader:  segment.ReaderFrom(offset),
	}, nil
}

func (e *commitLog) WriteEntry(ts uint64, value []byte) (uint64, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if segmentEntryCount := e.activeSegment.CurrentOffset(); segmentEntryCount >= e.segmentMaxRecordCount {
		err := e.appendSegment(uint64(len(e.segments)) * e.segmentMaxRecordCount)
		if err != nil {
			return 0, errors.Wrap(err, "failed to extend log")
		}
	}
	n, err := e.activeSegment.WriteEntry(ts, value)
	return n + e.activeSegment.BaseOffset(), err
}

type commitlogReader struct {
	mtx            sync.Mutex
	currentReader  io.Reader
	currentSegment Segment
	segmentSize    uint64
	log            *commitLog
	currentOffset  uint64
}

func (c *commitlogReader) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.currentSegment != nil {
		err := c.currentSegment.Close()
		c.currentReader = nil
		c.currentSegment = nil
		return err
	}
	return nil
}
func (c *commitlogReader) Seek(offset int64, whence int) (int64, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.currentSegment != nil {
		err := c.currentSegment.Close()
		if err != nil {
			return 0, err
		}
	}
	currentLogOffset := c.log.currentOffset()
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = 0 + offset
	case io.SeekCurrent:
		newOffset = int64(c.currentOffset) + offset
	case io.SeekEnd:
		newOffset = int64(currentLogOffset) + offset
	}
	if newOffset > int64(currentLogOffset) {
		c.currentOffset = uint64(currentLogOffset)
	} else if newOffset < 0 {
		c.currentOffset = 0
	} else {
		c.currentOffset = uint64(newOffset)
	}

	idx := c.log.lookupOffset(c.currentOffset)
	segment, err := c.log.readSegment(uint64(idx))
	if err != nil {
		return int64(c.currentOffset), err
	}

	c.currentReader = segment.ReaderFrom(c.currentOffset)
	c.currentSegment = segment

	return int64(c.currentOffset), nil
}

func (c *commitlogReader) Read(p []byte) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.currentOffset == c.log.currentOffset() {
		return 0, io.EOF
	}
	if c.currentReader != nil {
		if c.currentOffset%c.log.segmentMaxRecordCount == 0 {
			err := c.currentSegment.Close()
			if err != nil {
				return 0, err
			}
			c.currentReader = nil
			c.currentSegment = nil
		}
	}
	if c.currentReader == nil {
		segment, err := c.log.readSegment(uint64(c.currentOffset))
		if err == ErrSegmentDoesNotExist {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		c.currentSegment = segment
		c.currentReader = segment.ReaderFrom(c.currentOffset)
	}

	n, err := c.currentReader.Read(p)
	if n > 0 {
		c.currentOffset++
	}
	return n, err
}
