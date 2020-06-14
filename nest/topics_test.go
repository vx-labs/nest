package nest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_topicsState(t *testing.T) {
	store := NewTopicState()
	require.NoError(t, store.Insert([]byte("devices/a"), 2))
	require.NoError(t, store.Insert([]byte("devices/a"), 3))
	out := store.Match([]byte("devices/a"))
	require.Equal(t, 1, len(out))
	require.Equal(t, []uint64{2, 3}, out[0].Messages)
	require.Equal(t, []byte("devices/a"), out[0].Name)
}
