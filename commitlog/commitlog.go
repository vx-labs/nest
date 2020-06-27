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
	Reader() Cursor
	Offset() uint64
	Datadir() string
	LookupTimestamp(ts uint64) uint64
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

// lookupOffset returns the segment index of the segment containing the provided offset
func (e *commitLog) lookupOffset(offset uint64) int {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		return e.segments[i] > offset
	})
	return idx - 1
}
func (e *commitLog) LookupTimestamp(ts uint64) uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		seg, err := e.readSegment(e.segments[i])
		if err != nil {
			return true
		}
		defer seg.Close()
		return seg.Earliest() > ts
	})
	if idx <= 0 {
		return 0
	}
	seg, err := e.readSegment(e.segments[idx-1])
	if err != nil {
		return 0
	}
	defer seg.Close()
	return seg.LookupTimestamp(ts)
}

func (e *commitLog) currentOffset() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.activeSegment.CurrentOffset() + e.activeSegment.BaseOffset()
}

func (e *commitLog) readSegment(id uint64) (Segment, error) {
	s, err := openSegment(e.datadir, id, e.segmentMaxRecordCount, false)
	if err != nil {
		return nil, err
	}
	_, err = s.Seek(int64(id), io.SeekStart)
	return s, err
}

func (e *commitLog) Reader() Cursor {
	return &cursor{
		log: e,
	}
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
