package stream

import (
	"fmt"
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
	s, err := createStream("test", datadir, 5)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}
