package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	datadir := "/tmp"
	s, err := createSegment(datadir, 0, 200)
	require.NoError(t, err)
	defer s.Delete()
	t.Run("should not allow creating an existing segment", func(t *testing.T) {
		_, err := createSegment(datadir, 0, 200)
		require.Error(t, err)
	})
	t.Run("should write provided value", func(t *testing.T) {
		value := []byte("test")
		n, err := s.Write(value)
		require.NoError(t, err)
		require.Equal(t, len(value), n)
		entry, err := s.ReadEntryAt(make([]byte, entryHeaderSize), 0)
		require.NoError(t, err)
		require.Equal(t, value, entry.Payload())
	})

}

func BenchmarkSegment(b *testing.B) {
	datadir := "/tmp"
	s, err := createSegment(datadir, 0, 20000000)
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
	b.Run("read", func(b *testing.B) {
		buf := make([]byte, entryHeaderSize)
		for i := 0; i < b.N; i++ {
			s.ReadEntryAt(buf, 0)
		}
	})

}
