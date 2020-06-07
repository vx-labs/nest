package commitlog

import (
	"io"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrCorruptedLog = errors.New("corrupted commitlog")
)

type commitlog struct {
	datadir               string
	mtx                   sync.Mutex
	activeSegment         Segment
	segments              []uint64
	segmentMaxRecordCount uint64
}

func createLog(datadir string, segmentMaxRecordCount uint64) (*commitlog, error) {
	l := &commitlog{
		datadir:               datadir,
		segmentMaxRecordCount: segmentMaxRecordCount,
	}
	return l, l.appendSegment(0)
}
func openLog(datadir string, segmentMaxRecordCount uint64) (*commitlog, error) {
	l := &commitlog{
		datadir:               datadir,
		segmentMaxRecordCount: segmentMaxRecordCount,
	}
	files, err := ioutil.ReadDir(datadir)
	if err != nil {
		return nil, ErrCorruptedLog
	}
	for _, file := range files {
		if offsetStr := strings.TrimSuffix(file.Name(), ".log"); offsetStr != file.Name() {
			offset, err := strconv.ParseUint(offsetStr, 10, 64)
			if err == nil {
				segment, err := openSegment(datadir, offset, segmentMaxRecordCount, false)
				if err != nil {
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
			}
		}
	}
	return l, nil
}

func (e *commitlog) Close() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if e.activeSegment != nil {
		return e.activeSegment.Close()
	}
	return nil
}
func (e *commitlog) Delete() error {
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
func (e *commitlog) appendSegment(offset uint64) error {
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

// lookupOffset eturns the baseOffset (and thus, the segment id) of the segment containing the provided offset
func (e *commitlog) lookupOffset(offset uint64) uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		return e.segments[i] > offset
	})
	return e.segments[idx-1]
}

func (e *commitlog) readSegment(id uint64) (Segment, error) {
	return openSegment(e.datadir, id, e.segmentMaxRecordCount, false)
}

func (e *commitlog) ReaderFrom(offset uint64) (io.Reader, error) {
	idx := e.lookupOffset(offset)
	segment, err := e.readSegment(uint64(idx))
	if err != nil {
		return nil, err
	}
	return &commitlogReader{
		currentOffset:  offset,
		log:            e,
		currentSegment: segment,
		currentReader:  segment.ReaderFrom(offset),
	}, nil
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

type commitlogReader struct {
	mtx            sync.Mutex
	currentReader  io.Reader
	currentSegment Segment
	log            *commitlog
	currentOffset  uint64
}

func (c *commitlogReader) Read(p []byte) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for {
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
		if err == io.EOF {
			c.currentOffset = c.currentSegment.BaseOffset() + c.currentSegment.CurrentOffset()
			log.Printf("segment %v consumed, moving offset to %d", c.currentSegment.FilePath(), c.currentOffset)
			err := c.currentSegment.Close()
			if err != nil {
				return 0, err
			}
			c.currentReader = nil
			c.currentSegment = nil
		} else {
			return n, err
		}
	}
}
