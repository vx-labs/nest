package commitlog

import (
	"io"
)

type Decoder interface {
	Decode() (Entry, error)
}

type decoder struct {
	headerBuf []byte
	r         io.Reader
}

func (d *decoder) Decode() (Entry, error) {
	e, err := readEntry(d.r, d.headerBuf)
	return e, err
}

func NewDecoder(r io.Reader) Decoder {
	return &decoder{r: r, headerBuf: make([]byte, EntryHeaderSize)}
}
