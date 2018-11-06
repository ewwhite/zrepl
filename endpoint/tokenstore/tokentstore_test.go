package tokenstore

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func init() {
	// make tests run faster
	ExpirationScanInterval = 100*time.Millisecond
}

func TestFull(t *testing.T) {

	key := []byte("foobar2342")

	store, stop, err := New(key)
	assert.NoError(t, err)
	require.NotNil(t, store)
	require.NotNil(t, stop)

	t.Run("normal", func(t *testing.T) {
		tok, err := store.Add(int(23), time.Now().Add(2*ExpirationScanInterval))
		assert.NotZero(t, tok)
		require.NoError(t, err)

		data, err := store.Take(tok)
		require.NoError(t, err)
		require.NotNil(t, data)
		require.NotPanics(t, func() {
			i := data.(int)
			assert.Equal(t, i, int(23))
		})
	})

	t.Run("expiration", func(t *testing.T) {

		tok, err := store.Add(int(42), time.Now().Add(ExpirationScanInterval))
		assert.NotZero(t, tok)
		require.NoError(t, err)

		time.Sleep(3*ExpirationScanInterval)

		data, err := store.Take(tok)
		assert.Nil(t, data)
		assert.Error(t, err)
	})

	// this sub-test must be last because it stops
	t.Run("a", func(t *testing.T) {

		tok, err := store.Add(int(23+42), time.Now().Add(ExpirationScanInterval))
		assert.NotZero(t, tok)
		require.NoError(t, err)

		stop()

		time.Sleep(3*ExpirationScanInterval)

		data, err := store.Take(tok)
		require.NoError(t, err)
		require.NotNil(t, data)
		require.NotPanics(t, func() {
			i := data.(int)
			assert.Equal(t, i, int(23+42))
		})
	})

}
