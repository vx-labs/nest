package commitlog

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	datadir := "/tmp/"
	s, err := createSegment(datadir, 0, 200)
	require.NoError(t, err)
	defer s.Delete()
	t.Run("should not allow creating an existing segment", func(t *testing.T) {
		_, err := createSegment(datadir, 0, 200)
		require.Error(t, err)
	})
	t.Run("should write provided value", func(t *testing.T) {
		value := []byte("test")
		n, err := s.WriteEntry(2, value)
		require.NoError(t, err)
		require.Equal(t, uint64(0), n)
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
		m, err := s.WriteEntry(10, value)
		require.NoError(t, err)
		require.Equal(t, uint64(1), m)

		r := s.ReaderFrom(0)
		buf := make([]byte, 4)

		n, err := r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, value, buf)

		n, err = r.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 4, n)
		require.Equal(t, value, buf)
	})
	t.Run("should allow seeking position in reader", func(t *testing.T) {
		r := s.ReaderFrom(0).(*segmentReader)
		t.Run("start", func(t *testing.T) {
			r.Seek(1, io.SeekStart)
			require.Equal(t, uint64(1), r.offset)
			r.Seek(2, io.SeekStart)
			require.Equal(t, uint64(2), r.offset)
			r.Seek(3, io.SeekStart)
			require.Equal(t, uint64(2), r.offset)
			r.Seek(0, io.SeekStart)
			require.Equal(t, uint64(0), r.offset)
		})
		t.Run("current", func(t *testing.T) {
			r.Seek(1, io.SeekCurrent)
			require.Equal(t, uint64(1), r.offset)
			r.Seek(1, io.SeekCurrent)
			require.Equal(t, uint64(2), r.offset)
			r.Seek(1, io.SeekCurrent)
			require.Equal(t, uint64(2), r.offset)
			r.Seek(-1, io.SeekCurrent)
			require.Equal(t, uint64(1), r.offset)
		})
		t.Run("end", func(t *testing.T) {
			r.Seek(1, io.SeekEnd)
			require.Equal(t, uint64(2), r.offset)
			r.Seek(-1, io.SeekEnd)
			require.Equal(t, uint64(1), r.offset)
		})
	})
	t.Run("should allow seeking timestamp", func(t *testing.T) {
		require.Equal(t, uint64(0), s.(*segment).seekTimestamp(0))
	})
	t.Run("should allow seeking timestamp", func(t *testing.T) {
		require.Equal(t, uint64(1), s.(*segment).seekTimestamp(10))
	})
	t.Run("should allow seeking timestamp", func(t *testing.T) {
		require.Equal(t, uint64(2), s.(*segment).seekTimestamp(100))
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
			_, err = s.WriteEntry(0, value)
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
