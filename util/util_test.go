package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	s := NewSet()
	t.Run("added values are present in the set", func(t *testing.T) {
		val := []byte("hello")
		s.Set(val)
		assert.True(t, s.Has(val))
	})

	t.Run("deleting added values", func(t *testing.T) {
		val := []byte("hello")
		assert.True(t, s.Has(val))
		s.Del(val)
		assert.False(t, s.Has(val))
	})
}
