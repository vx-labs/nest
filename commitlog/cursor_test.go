package commitlog

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursor(t *testing.T) {
	datadir := "/tmp/"
	clog, err := create(datadir, 10)
	require.NoError(t, err)
	defer clog.Delete()
	value := []byte("test")

	for i := 0; i < 50; i++ {
		n, err := clog.WriteEntry(uint64(i), value)
		require.NoError(t, err)
		require.Equal(t, uint64(i), n)
	}
	l := clog.(*commitLog)
	require.Equal(t, 5, len(l.segments))

	t.Run("should allow being written to an io.Writer", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		c := clog.Reader()
		defer c.Close()
		n, err := c.WriteTo(buf)
		require.NoError(t, err)
		require.Equal(t, int64(1600), n)
	})
	t.Run("should allow reading the log", func(t *testing.T) {
		buf := make([]byte, 1600)
		r := clog.Reader()
		defer r.Close()
		n, err := io.ReadFull(r, buf)
		require.NoError(t, err)
		require.Equal(t, 1600, n)
	})
	t.Run("should allow seeking position in cursor", func(t *testing.T) {
		cReader := clog.Reader()
		r := cReader.(*cursor)
		t.Run("start", func(t *testing.T) {
			r.Seek(1, io.SeekStart)
			require.Equal(t, uint64(1), r.currentOffset)
			r.Seek(2, io.SeekStart)
			require.Equal(t, uint64(2), r.currentOffset)
			r.Seek(30, io.SeekStart)
			require.Equal(t, uint64(30), r.currentOffset)
			r.Seek(0, io.SeekStart)
			require.Equal(t, uint64(0), r.currentOffset)
		})
		t.Run("current", func(t *testing.T) {
			r.Seek(1, io.SeekCurrent)
			require.Equal(t, uint64(1), r.currentOffset)
			r.Seek(1, io.SeekCurrent)
			require.Equal(t, uint64(2), r.currentOffset)
			r.Seek(15, io.SeekCurrent)
			require.Equal(t, uint64(17), r.currentOffset)
			r.Seek(-1, io.SeekCurrent)
			require.Equal(t, uint64(16), r.currentOffset)
		})
		t.Run("end", func(t *testing.T) {
			r.Seek(0, io.SeekEnd)
			require.Equal(t, uint64(50), r.currentOffset)
			r.Seek(-1, io.SeekEnd)
			require.Equal(t, uint64(49), r.currentOffset)
			r.Seek(-5, io.SeekEnd)
			require.Equal(t, uint64(45), r.currentOffset)
		})
	})

}
