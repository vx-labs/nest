package commitlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitLog(t *testing.T) {
	datadir := "/tmp"
	clog, err := Create(datadir, 10)
	require.NoError(t, err)
	defer clog.Delete()
	value := []byte("test")

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
