package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkLog(b *testing.B) {
	datadir := "/tmp"
	s, err := createLog(datadir, 500)
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
}
