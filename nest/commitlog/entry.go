package commitlog

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
)

var (
	ErrInvalidBufferSize = errors.New("invalid buffer size")
)

const (
	checksumSize uint64 = 4
)

type Entry interface {
	Size() uint64
	Offset() uint64
	Checksum() []byte
	Payload() []byte
	IsValid() bool
}

type entry struct {
	size     uint64
	offset   uint64
	checksum []byte
	payload  []byte
}

func (e entry) Size() uint64     { return e.size }
func (e entry) Offset() uint64   { return e.offset }
func (e entry) Payload() []byte  { return e.payload }
func (e entry) Checksum() []byte { return e.checksum }
func (e entry) IsValid() bool    { return bytes.Equal(crc32.NewIEEE().Sum(e.payload), e.checksum) }

func newEntry(offset uint64, payload []byte) Entry {
	return entry{
		size:     uint64(8) + uint64(8) + uint64(len(payload)) + checksumSize,
		offset:   offset,
		checksum: crc32.NewIEEE().Sum(payload),
		payload:  payload,
	}
}
func readEntry(r io.Reader, buf []byte) (Entry, error) {
	if len(buf) < 8 {
		return nil, ErrInvalidBufferSize
	}
	_, err := r.Read(buf[0:8])
	if err != nil {
		return nil, err
	}
	size := encoding.Uint64(buf[0:8])
	if uint64(len(buf)) < size {
		return nil, ErrInvalidBufferSize
	}
	_, err = r.Read(buf[8:size])
	if err != nil {
		return nil, err
	}
	return decodeEntry(buf)
}

func decodeEntry(buf []byte) (Entry, error) {
	return &entry{
		size:     encoding.Uint64(buf[0:8]),
		offset:   encoding.Uint64(buf[8:16]),
		checksum: buf[16 : 16+checksumSize],
		payload:  buf[16+checksumSize:],
	}, nil
}
func encodeEntry(e Entry, buf []byte) error {
	if uint64(len(buf)) < e.Size() {
		return ErrInvalidBufferSize
	}
	encoding.PutUint64(buf[0:8], e.Size())
	encoding.PutUint64(buf[8:16], e.Offset())
	copy(buf[16:16+checksumSize], e.Checksum())
	copy(buf[16+checksumSize:], e.Payload())
	return nil
}
func writeEntry(e Entry, w io.Writer) (int, error) {
	buf := make([]byte, e.Size())
	err := encodeEntry(e, buf)
	if err != nil {
		return 0, err
	}
	return w.Write(buf)
}
