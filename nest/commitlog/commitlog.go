package commitlog

import (
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type commitlog struct {
	datadir               string
	mtx                   sync.Mutex
	activeSegment         Segment
	segments              []Segment
	segmentMaxRecordCount uint64
}

func createLog(datadir string, segmentMaxRecordCount uint64) (*commitlog, error) {
	l := &commitlog{
		datadir:               datadir,
		segmentMaxRecordCount: segmentMaxRecordCount,
	}
	return l, l.appendSegment(0)
}

func (e *commitlog) Delete() error {
	for idx := range e.segments {
		err := e.segments[idx].Delete()
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *commitlog) appendSegment(offset uint64) error {
	segment, err := createSegment(e.datadir, offset, e.segmentMaxRecordCount)
	if err != nil {
		return errors.Wrap(err, "failed to create new segment")
	}
	e.segments = append(e.segments, segment)
	if e.activeSegment != nil {
		err = e.activeSegment.Close()
		if err != nil {
			return err
		}
	}
	e.activeSegment = segment
	return nil
}

func (e *commitlog) lookupOffset(offset uint64) int {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		return e.segments[i].BaseOffset() > offset
	})
	return idx - 1
}

func (e *commitlog) Write(value []byte) (int, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if segmentEntryCount := e.activeSegment.CurrentOffset(); segmentEntryCount >= e.segmentMaxRecordCount {
		err := e.appendSegment(uint64(len(e.segments)) * e.segmentMaxRecordCount)
		if err != nil {
			return 0, errors.Wrap(err, "failed to extend log")
		}
	}
	return e.activeSegment.Write(value)
}
