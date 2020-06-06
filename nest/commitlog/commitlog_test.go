package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitLog(t *testing.T) {
	datadir := "/tmp"
	s, err := createLog(datadir, 10)
	require.NoError(t, err)
	defer s.Delete()
	value := []byte("test")

	for i := 0; i < 50; i++ {
		n, err := s.Write(value)
		require.NoError(t, err)
		require.Equal(t, len(value), n)
	}
	require.Equal(t, 5, len(s.segments))
	t.Run("should allow looking up for offset", func(t *testing.T) {
		require.Equal(t, 2, s.lookupOffset(27))
		require.Equal(t, 0, s.lookupOffset(9))
		require.Equal(t, 1, s.lookupOffset(10))
	})
}

func BenchmarkLog(b *testing.B) {
	datadir := "/tmp"
	s, err := createLog(datadir, 500)
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
