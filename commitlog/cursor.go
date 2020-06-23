package commitlog

import (
	"io"
	"sync"
)

type Cursor interface {
	io.Seeker
	io.WriterTo
	io.Reader
	io.Closer
}

type cursor struct {
	mtx            sync.Mutex
	currentSegment Segment
	log            *commitLog
	currentOffset  uint64
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
func (c *cursor) seek(offset int64, whence int) (int64, error) {
	if c.currentSegment != nil {
		err := c.currentSegment.Close()
		if err != nil {
			return 0, err
		}
		c.currentSegment = nil
	}
	currentLogOffset := c.log.currentOffset()
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = 0 + offset
	case io.SeekCurrent:
		newOffset = int64(c.currentOffset) + offset
	case io.SeekEnd:
		newOffset = int64(currentLogOffset) + offset
	}
	if newOffset > int64(currentLogOffset) {
		c.currentOffset = uint64(currentLogOffset)
	} else if newOffset < 0 {
		c.currentOffset = 0
	} else {
		c.currentOffset = uint64(newOffset)
	}
	idx := c.log.lookupOffset(c.currentOffset)
	segment, err := c.log.readSegment(uint64(idx))
	if err != nil {
		return int64(c.currentOffset), err
	}
	_, err = segment.Seek(int64(c.currentOffset), io.SeekStart)
	if err != nil {
		return 0, err
	}
	c.currentSegment = segment

	return int64(c.currentOffset), nil
}

func (c *cursor) Read(p []byte) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	total := 0
	for {
		if c.currentSegment == nil {
			_, err := c.seek(int64(c.currentOffset), io.SeekStart)
			if err != nil {
				return 0, err
			}
		}

		n, err := c.currentSegment.Read(p[total:])
		total += n
		if err == io.EOF {
			if c.log.segments[len(c.log.segments)-1] == c.currentSegment.BaseOffset() {
				if total > 0 {
					return total, nil
				}
				return total, io.EOF
			}
			c.currentOffset = c.log.segmentMaxRecordCount + c.currentSegment.BaseOffset()
			err := c.currentSegment.Close()
			if err != nil {
				return total, err
			}
			c.currentSegment = nil
			return total, nil
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
			_, err := c.seek(int64(c.currentOffset), io.SeekStart)
			if err != nil {
				return 0, err
			}
		}
		n, err := c.currentSegment.WriteTo(w)
		total += n
		if err != nil {
			return total, err
		}
		if c.log.segments[len(c.log.segments)-1] == c.currentSegment.BaseOffset() {
			return total, nil
		}
		c.currentOffset = c.log.segmentMaxRecordCount + c.currentSegment.BaseOffset()
		err = c.currentSegment.Close()
		if err != nil {
			return total, err
		}
		c.currentSegment = nil
	}
}
