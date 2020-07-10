package commitlog

import (
	"io/ioutil"
	"strings"
)

type Statistics struct {
	SegmentCount  uint64
	CurrentOffset uint64
	StoredBytes   uint64
}

func (c *commitLog) GetStatistics() Statistics {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var size int64
	files, err := ioutil.ReadDir(c.datadir)
	if err == nil {
		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".log") {
				size += file.Size()
			}
		}
	}
	return Statistics{
		CurrentOffset: c.activeSegment.CurrentOffset() + c.activeSegment.BaseOffset(),
		SegmentCount:  uint64(len(c.segments)),
		StoredBytes:   uint64(size),
	}
}
