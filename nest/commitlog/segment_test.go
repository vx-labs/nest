package commitlog

import (
	"io"
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
	t.Run("should close then reopen without error", func(t *testing.T) {
		value := []byte("test")
		err := s.Close()
		require.NoError(t, err)
		s, err = openSegment(datadir, 0, 200, true)
		require.NoError(t, err)
		require.Equal(t, uint64(1), s.CurrentOffset())
		require.Equal(t, uint64(entryHeaderSize+len(value)), s.Size())
		entry, err := s.ReadEntryAt(make([]byte, entryHeaderSize), 0)
		require.NoError(t, err)
		require.Equal(t, value, entry.Payload())
	})
	t.Run("should allow reading binary data", func(t *testing.T) {
		r := s.ReaderFrom(0)
		buf := make([]byte, 4)
		n, err := r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, []byte("test"), buf)
	})
	t.Run("should allow reading partial binary data", func(t *testing.T) {
		r := s.ReaderFrom(0)
		buf := make([]byte, 2)
		n, err := r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte("te"), buf)

		n, err = r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte("st"), buf)
	})
	t.Run("should allow reading multiple entries payload", func(t *testing.T) {
		value := []byte("test")
		n, err := s.Write(value)
		require.NoError(t, err)
		require.Equal(t, len(value), n)

		r := s.ReaderFrom(0)
		buf := make([]byte, 4)

		n, err = r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, value, buf)

		n, err = r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, value, buf)

		n, err = r.Read(buf)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)
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
	b.Run("open", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := s.Close()
			require.NoError(b, err)
			s, err = openSegment(datadir, 0, 20000000, true)
			require.NoError(b, err)
		}
	})
	b.Run("scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			r := s.ReaderFrom(0)
			var err error
			buf := make([]byte, 4)
			for err == nil {
				_, err = r.Read(buf)
			}
		}
	})

}
