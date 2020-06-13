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
	checksumSize    int = 4
	entryHeaderSize int = 8 + 8 + checksumSize
)

type Entry interface {
	Size() uint64
	Offset() uint64
	Checksum() []byte
	Payload() []byte
	IsValid() bool
}

type entry struct {
	payloadSize uint64
	offset      uint64
	checksum    []byte
	payload     []byte
}

func hash(b []byte) []byte {
	crc := crc32.NewIEEE()
	crc.Write(b)
	return crc.Sum(nil)
}

func (e entry) Size() uint64     { return e.payloadSize }
func (e entry) Offset() uint64   { return e.offset }
func (e entry) Payload() []byte  { return e.payload }
func (e entry) Checksum() []byte { return e.checksum }
func (e entry) IsValid() bool    { return bytes.Equal(hash(e.payload), e.checksum) }

func newEntry(offset uint64, payload []byte) Entry {
	return entry{
		payloadSize: uint64(len(payload)),
		offset:      offset,
		checksum:    hash(payload),
		payload:     payload,
	}
}
func readEntry(r io.Reader, buf []byte) (Entry, error) {
	if len(buf) != entryHeaderSize {
		return nil, ErrInvalidBufferSize
	}
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	payloadSize := encoding.Uint64(buf[0:8])
	bodyBuf := make([]byte, payloadSize+uint64(entryHeaderSize))
	copy(bodyBuf[0:entryHeaderSize], buf)
	_, err = io.ReadFull(r, bodyBuf[entryHeaderSize:])
	if err != nil {
		return nil, err
	}
	return decodeEntry(bodyBuf)
}

func decodeEntry(buf []byte) (Entry, error) {
	return &entry{
		payloadSize: encoding.Uint64(buf[0:8]),
		offset:      encoding.Uint64(buf[8:16]),
		checksum:    buf[16 : 16+checksumSize],
		payload:     buf[16+checksumSize:],
	}, nil
}
func writeEntry(e Entry, w io.Writer) (int, error) {
	buf := make([]byte, entryHeaderSize)
	encoding.PutUint64(buf[0:8], e.Size())
	encoding.PutUint64(buf[8:16], e.Offset())
	copy(buf[16:20], e.Checksum())
	total, err := w.Write(buf)
	if err != nil {
		return total, err
	}
	n, err := w.Write(e.Payload())
	total += n
	if err != nil {
		return total, err
	}
	return total, nil
}
