package commitlog

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

type Cursor interface {
	io.Seeker
	io.WriterTo
	io.Reader
	io.Closer
}

type cursor struct {
	currentIdx     int
	mtx            sync.Mutex
	currentSegment Segment
	log            *commitLog
}

func (c *cursor) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.currentSegment != nil {
		err := c.currentSegment.Close()
		c.currentSegment = nil
		return err
	}
	return nil
}
func (c *cursor) Seek(offset int64, whence int) (int64, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.seek(offset, whence)
}
func (c *cursor) seekFromStart(offset int64) (int64, error) {
	idx := c.log.lookupOffset(uint64(offset))
	if idx != c.currentIdx || c.currentSegment == nil {
		if c.currentSegment != nil {
			err := c.currentSegment.Close()
			if err != nil {
				return 0, err
			}
			c.currentSegment = nil
		}
		segment, err := c.log.readSegment(c.log.segments[idx])
		if err != nil {
			return 0, err
		}
		c.currentIdx = idx
		c.currentSegment = segment
	}
	return c.currentSegment.Seek(offset, io.SeekStart)
}

func (c *cursor) seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return c.seekFromStart(offset)
	case io.SeekCurrent:
		return 0, errors.New("SeekCurrent unsupported when reading the log")
	default:
		return 0, errors.New("invalid whence")
	}
}

func (c *cursor) doesSegmentExists(idx int) bool {
	return idx < len(c.log.segments)
}
func (c *cursor) openCurrentSegment() error {
	if c.doesSegmentExists(c.currentIdx) {
		segment, err := c.log.readSegment(c.log.segments[c.currentIdx])
		if err != nil {
			return err
		}
		c.currentSegment = segment
		return nil
	}
	return io.EOF
}
func (c *cursor) Read(p []byte) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var total int

	for {
		if c.currentSegment == nil {
			if err := c.openCurrentSegment(); err != nil {
				return total, err
			}
		}
		n, err := c.currentSegment.Read(p[total:])
		total += n
		if err == io.EOF {
			if c.doesSegmentExists(c.currentIdx + 1) {
				if err := c.currentSegment.Close(); err != nil {
					return total, err
				}
				c.currentIdx++
				c.currentSegment = nil
				continue
			}
		}
		return total, err
	}
}

func (c *cursor) WriteTo(w io.Writer) (int64, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var total int64

	for {
		if c.currentSegment == nil {
			if err := c.openCurrentSegment(); err != nil {
				return total, err
			}
		}
		n, err := c.currentSegment.WriteTo(w)
		total += n
		if err == nil {
			if c.doesSegmentExists(c.currentIdx + 1) {
				if err := c.currentSegment.Close(); err != nil {
					return total, err
				}
				c.currentIdx++
				c.currentSegment = nil
				continue
			}
		}
		return total, err
	}
}
