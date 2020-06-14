package commitlog

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOffsetReader(t *testing.T) {
	backend := bytes.NewReader([]byte{0, 1, 2, 3, 4})
	r := OffsetReader([]uint64{0, 2, 4}, backend)
	buf := make([]byte, 1)

	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{0}, buf)

	n, err = r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{2}, buf)

	n, err = r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, []byte{4}, buf)

	n, err = r.Read(buf)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}
