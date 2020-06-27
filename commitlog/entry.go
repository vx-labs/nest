package commitlog

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
)

var (
	ErrInvalidBufferSize        = errors.New("invalid buffer size")
	ErrEntryTooBig              = errors.New("entry is too big")
	MaxEntrySize         uint64 = 20000000
)

const (
	checksumSize    int = 4
	EntryHeaderSize int = 8 + 8 + 8 + checksumSize
)

type Entry interface {
	Size() uint64
	Offset() uint64
	Timestamp() uint64
	Checksum() []byte
	Payload() []byte
	IsValid() bool
}

type entry struct {
	payloadSize uint64
	offset      uint64
	timestamp   uint64
	checksum    []byte
	payload     []byte
}

func hash(b []byte) []byte {
	crc := crc32.NewIEEE()
	crc.Write(b)
	return crc.Sum(nil)
}

func (e entry) Size() uint64      { return e.payloadSize }
func (e entry) Offset() uint64    { return e.offset }
func (e entry) Timestamp() uint64 { return e.timestamp }
func (e entry) Payload() []byte   { return e.payload }
func (e entry) Checksum() []byte  { return e.checksum }
func (e entry) IsValid() bool     { return bytes.Equal(hash(e.payload), e.checksum) }

func newEntry(ts uint64, offset uint64, payload []byte) Entry {
	return entry{
		payloadSize: uint64(len(payload)),
		offset:      offset,
		timestamp:   ts,
		checksum:    hash(payload),
		payload:     payload,
	}
}
func readEntry(r io.Reader, buf []byte) (Entry, error) {
	if len(buf) != EntryHeaderSize {
		return nil, ErrInvalidBufferSize
	}
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	payloadSize := encoding.Uint64(buf[0:8])
	if payloadSize > MaxEntrySize {
		return nil, ErrEntryTooBig
	}
	bodyBuf := make([]byte, payloadSize+uint64(EntryHeaderSize))
	copy(bodyBuf[0:EntryHeaderSize], buf)
	_, err = io.ReadFull(r, bodyBuf[EntryHeaderSize:])
	if err != nil {
		return nil, err
	}
	return decodeEntry(bodyBuf)
}

func decodeEntry(buf []byte) (Entry, error) {
	e := &entry{
		payloadSize: encoding.Uint64(buf[0:8]),
		offset:      encoding.Uint64(buf[8:16]),
		timestamp:   encoding.Uint64(buf[16:24]),
		checksum:    buf[24 : 24+checksumSize],
		payload:     buf[24+checksumSize:],
	}
	return e, nil
}
func writeEntry(e Entry, w io.Writer) (int, error) {
	if e.Size() > MaxEntrySize {
		return 0, ErrEntryTooBig
	}
	buf := make([]byte, EntryHeaderSize+int(e.Size()))
	encoding.PutUint64(buf[0:8], e.Size())
	encoding.PutUint64(buf[8:16], e.Offset())
	encoding.PutUint64(buf[16:24], e.Timestamp())
	copy(buf[24:28], e.Checksum())
	copy(buf[28:], e.Payload())
	return w.Write(buf)
}
