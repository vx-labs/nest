package nest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_match(t *testing.T) {
	require.True(t, match([]byte("#"), []byte("a/a")))
	require.True(t, match([]byte("a/a"), []byte("a/a")))
	require.False(t, match([]byte("a/b"), []byte("a/a")))
	require.False(t, match([]byte("a/b"), []byte("a/a")))
	require.True(t, match([]byte("a/b/+"), []byte("a/b/c")))
	require.True(t, match([]byte("a/+/c"), []byte("a/b/c")))
	require.True(t, match([]byte("a/#"), []byte("a/b/c")))
	require.False(t, match([]byte("a/+/d"), []byte("a/b/c")))
	require.False(t, match([]byte("a/+/c/c"), []byte("a/b/c")))
	require.False(t, match([]byte("a/+"), []byte("a/b/c/d/d")))
	a := []byte("a/b/c/d/d")
	require.True(t, match([]byte("a/b/c/#"), a))
	require.Equal(t, []byte("a/b/c/d/d"), a)
}

func Benchmark_match(b *testing.B) {
	for i := 0; i < b.N; i++ {
		match([]byte("a/+/c/c"), []byte("a/b/c"))
	}
}
