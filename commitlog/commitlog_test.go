package commitlog

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitLog(t *testing.T) {
	datadir := "/tmp"
	clog, err := Create(datadir, 10)
	require.NoError(t, err)
	defer clog.Delete()
	value := []byte("test")
	t.Run("should allow reading from empty log", func(t *testing.T) {
		r, err := clog.ReaderFrom(0)
		require.NoError(t, err)
		buf := make([]byte, len(value))
		n, err := r.Read(buf)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)
	})

	for i := 0; i < 50; i++ {
		n, err := clog.Write(value)
		require.NoError(t, err)
		require.Equal(t, len(value), n)
	}
	l := clog.(*commitLog)
	require.Equal(t, 5, len(l.segments))
	t.Run("should close then reopen without error", func(t *testing.T) {
		require.NoError(t, clog.Close())
		clog, err = Open(datadir, 10)
		require.NoError(t, err)
	})
	t.Run("should allow looking up for offset", func(t *testing.T) {
		l := clog.(*commitLog)
		require.Equal(t, uint64(20), l.lookupOffset(27))
		require.Equal(t, uint64(0), l.lookupOffset(9))
		require.Equal(t, uint64(10), l.lookupOffset(10))
	})
	t.Run("should allow reading from log", func(t *testing.T) {
		r, err := clog.ReaderFrom(0)
		require.NoError(t, err)
		buf := make([]byte, len(value))
		for i := 0; i < 50; i++ {
			n, err := r.Read(buf)
			require.NoError(t, err, fmt.Sprintf("index: %d", i))
			require.Equal(t, len(value), n)
		}
	})
	t.Run("should allow seeking position in reader", func(t *testing.T) {
		cReader, err := clog.ReaderFrom(0)
		require.NoError(t, err)
		r := cReader.(*commitlogReader)
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
			r.Seek(1, io.SeekEnd)
			require.Equal(t, uint64(50), r.currentOffset)
			r.Seek(-1, io.SeekEnd)
			require.Equal(t, uint64(49), r.currentOffset)
		})
	})

}

func BenchmarkLog(b *testing.B) {
	datadir := "/tmp"
	s, err := Create(datadir, 500)
	require.NoError(b, err)
	defer s.Delete()
	value := []byte("test")
	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = s.Write(value)
			if err != nil {
				b.Fatalf("segment write failed: %v", err)
			}
		}
	})
}
