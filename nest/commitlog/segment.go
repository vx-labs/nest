package commitlog

import (
	"io"
	"os"
)

type Segment interface {
	io.Closer
}

type segment struct {
	startOffset uint64
	fd          *os.File
	index       Index
}
