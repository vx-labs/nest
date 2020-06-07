package stream

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashShardey(t *testing.T) {
	require.Equal(t, 1, hashShardKey([]byte("test"), 2))
	require.Equal(t, 0, hashShardKey([]byte("testa"), 2))
}

func TestStream(t *testing.T) {
	datadir := fmt.Sprintf("/tmp/stream_test_%d", os.Getpid())
	defer func() {
		os.RemoveAll(datadir)
	}()
	s, err := createStream("test", datadir, 1)
	require.NoError(t, err)
	value := []byte("test")

	t.Run("write", func(t *testing.T) {
		for i := 0; i < 500; i++ {
			n, err := s.Writer([]byte("a")).Write(value)
			require.NoError(t, err)
			require.Equal(t, len(value), n)
		}
	})
	t.Run("close then open", func(t *testing.T) {
		s.Close()
		s, err = openStream("test", datadir)
		require.NoError(t, err)
	})
	t.Run("read from start", func(t *testing.T) {
		r, err := s.Reader(0, 0)
		require.NoError(t, err)

		buf := make([]byte, len(value))
		for i := 0; i < 500; i++ {
			n, err := r.Read(buf)
			require.NoError(t, err)
			require.Equal(t, len(value), n)
			require.Equal(t, value, buf)
		}
		n, err := r.Read(buf)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)

	})
	t.Run("read from offset", func(t *testing.T) {
		r, err := s.Reader(0, 250)
		require.NoError(t, err)

		buf := make([]byte, len(value))
		for i := 0; i < 250; i++ {
			n, err := r.Read(buf)
			require.NoError(t, err)
			require.Equal(t, len(value), n)
			require.Equal(t, value, buf)
		}
		n, err := r.Read(buf)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	})

	require.NoError(t, s.Close())
}
