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
)

type Segment interface {
	io.Closer
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

func (i *segment) FilePath() string {
	return i.path
}
func (i *segment) Name() string {
	return i.fd.Name()
}

func createSegment(datadir string, id uint64, maxRecordCount uint64) (Segment, error) {
	filename := indexName(datadir, id)
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
	filename := indexName(datadir, id)
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
	s := &segment{
		path:           filename,
		baseOffset:     id,
		currentOffset:  0,
		maxRecordCount: maxRecordCount,
		index:          idx,
		fd:             fd,
	}
	// TODO: check segment integrity ?
	return s, nil
}
