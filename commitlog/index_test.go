package commitlog

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	datadir := "/tmp"
	index, err := createIndex(datadir, "", 0, 200)
	require.NoError(t, err)
	defer os.Remove(index.FilePath())
	t.Run("should not allow creating an existing index", func(t *testing.T) {
		_, err := createIndex(datadir, "", 0, 200)
		require.Error(t, err)
	})
	t.Run("should write provided value", func(t *testing.T) {
		require.NoError(t, index.writePosition(2, 254))
		val, err := index.readPosition(2)
		require.NoError(t, err)
		require.Equal(t, uint64(254), val)
	})
	t.Run("should close then re-open without error nor lossing data", func(t *testing.T) {
		require.NoError(t, index.Close())
		index, err := openIndex(datadir, "", 0, 200)
		require.NoError(t, err)
		val, err := index.readPosition(2)
		require.NoError(t, err)
		require.Equal(t, uint64(254), val)
	})
}

func BenchmarkIndex(b *testing.B) {
	datadir := "/tmp"
	index, err := createIndex(datadir, "", 0, 200)
	require.NoError(b, err)
	defer os.Remove(index.FilePath())
	require.NoError(b, index.writePosition(2, 254))

	for i := 0; i < b.N; i++ {
		index.readPosition(2)
	}
}
