package commitlog

import (
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

func (e *commitlog) Write(value []byte) (int, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if offset := e.activeSegment.CurrentOffset(); offset >= e.segmentMaxRecordCount {
		err := e.appendSegment(e.activeSegment.BaseOffset() + 1)
		if err != nil {
			return 0, errors.Wrap(err, "failed to extend log")
		}
	}
	return e.activeSegment.Write(value)
}
