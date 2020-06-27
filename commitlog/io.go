package commitlog

import (
	"io"
)

type ReadSeekCloser interface {
	io.ReadCloser
	io.Seeker
}
