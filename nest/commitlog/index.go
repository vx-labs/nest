package commitlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/tysontate/gommap"
)

const (
	indexValueSize = 8
)

var encoding = binary.BigEndian

var (
	ErrIndexAlreadyExists    = errors.New("index already exists")
	ErrIndexDoesNotExist     = errors.New("index does not exist")
	ErrInvalidIndexValue     = errors.New("value to index does not respect size limit")
	ErrInvalidOffsetTooShort = errors.New("invalid offset: offset is too short")
	ErrInvalidOffsetTooBig   = errors.New("invalid offset: offset is too big")
	ErrMMapFailed            = errors.New("mmap failed")
	ErrFSyncFailed           = errors.New("file sync failed")
	ErrMSyncFailed           = errors.New("mmap sync failed")
)

type Index interface {
	Sync() error
	FilePath() string
	Name() string
	io.Closer
	writePosition(offset, position uint64) error
	readPosition(offset uint64) (uint64, error)
}

type index struct {
	path        string
	baseOffset  uint64
	curOffset   uint64
	segmentSize uint64
	fd          *os.File
	data        gommap.MMap
}

func indexName(datadir string, id uint64) string {
	return path.Join(datadir, fmt.Sprintf("%d.index", id))
}

func createIndex(datadir string, id uint64, segmentSize uint64) (Index, error) {
	filename := indexName(datadir, id)
	if fileExists(filename) {
		return nil, ErrIndexAlreadyExists
	}
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0650)
	if err != nil {
		return nil, err
	}
	err = fd.Truncate(int64(segmentSize * indexValueSize))
	if err != nil {
		fd.Close()
		os.Remove(filename)
		return nil, err
	}
	idx := &index{baseOffset: segmentSize * id, segmentSize: segmentSize, fd: fd, path: filename}
	return idx, idx.mmap()
}
func openIndex(datadir string, id uint64, segmentSize uint64) (Index, error) {
	filename := indexName(datadir, id)
	if !fileExists(filename) {
		return nil, ErrIndexDoesNotExist
	}
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	idx := &index{baseOffset: segmentSize * id, segmentSize: segmentSize, fd: fd, path: filename}

	// TODO: check index integrity
	return idx, idx.mmap()
}

func (i *index) FilePath() string {
	return i.path
}
func (i *index) Name() string {
	return i.fd.Name()
}

func (i *index) mmap() error {
	mmapedData, err := gommap.Map(i.fd.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return ErrMMapFailed
	}
	i.data = mmapedData
	return nil
}

func (i *index) Sync() error {
	if err := i.fd.Sync(); err != nil {
		return ErrFSyncFailed
	}
	if err := i.data.Sync(gommap.MS_SYNC); err != nil {
		return ErrMMapFailed
	}
	return nil
}

func (i *index) Close() error {
	err := i.Sync()
	if err != nil {
		return err
	}
	err = i.data.UnsafeUnmap()
	if err != nil {
		return err
	}
	return i.fd.Close()
}
func (i *index) writePosition(offset, position uint64) error {
	writeOffset := offset * indexValueSize
	encoding.PutUint64(i.data[writeOffset:writeOffset+indexValueSize], position)
	return nil
}

func (i *index) readPosition(offset uint64) (uint64, error) {
	writeOffset := offset * indexValueSize
	return encoding.Uint64(i.data[writeOffset : writeOffset+indexValueSize]), nil
}