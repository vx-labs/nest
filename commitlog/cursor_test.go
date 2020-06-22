package commitlog

import (
	"bytes"
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
		n, err := c.WriteTo(buf)
		require.NoError(t, err)
		require.Equal(t, int64(1600), n)
	})
}
