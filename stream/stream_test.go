package stream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHashShardey(t *testing.T) {
	require.Equal(t, 1, hashShardKey([]byte("test"), 2))
	require.Equal(t, 0, hashShardKey([]byte("testa"), 2))
}
