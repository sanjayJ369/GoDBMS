package kv

import (
	"dbms/btree"
	"dbms/util"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {

	count := 50
	size := 500

	loc := util.NewTempFileLoc()
	store, err := NewKv(loc)
	kv := NewKVTX()
	store.Begin(kv)
	require.NoError(t, err, "creating new kv")

	require.NoError(t, err, "opening kv")
	t.Run("set and get the kv pair", func(t *testing.T) {
		kv.Set([]byte("hello"), []byte("world"))

		val, err := kv.Get([]byte("hello"))
		require.NoError(t, err, "getting value")
		assert.Equal(t, "world", string(val), "getting value")
	})

	t.Run("set and get the 5000 kv pairs", func(t *testing.T) {

		startTime := time.Now()
		for i := 0; i < count; i++ {
			val := make([]byte, size)
			copy(val, fmt.Sprintf("this is val%d", i))
			key := []byte(fmt.Sprintf("key%d", i))

			kv.Set(key, val)
			btree.PrintTree(kv.pending, kv.pending.Get(kv.pending.Root))
		}
		endTime := time.Now()
		for i := 0; i < count; i++ {
			val := make([]byte, size)
			copy(val, fmt.Sprintf("this is val%d", i))
			key := []byte(fmt.Sprintf("key%d", i))
			got, err := kv.Get(key)
			require.NoError(t, err, "getting value")
			assert.Equal(t, val, got, "getting value")
		}
		fmt.Printf("\ntime taken(%d): %ds\n", count, endTime.Second()-startTime.Second())
		err = store.Commit(kv)
		require.NoError(t, err, "comitting transaction")
	})

	t.Run("data is persisted to the disk", func(t *testing.T) {

		store, err = NewKv(loc)
		require.NoError(t, err, "opening kv")
		kv := NewKVTX()
		store.Begin(kv)
		store.PrintTree()
		key := []byte("key14")
		key = []byte("key1")
		got, err := kv.Get(key)
		fmt.Println(got, err)
		for i := 0; i < count; i++ {
			val := make([]byte, size)
			copy(val, fmt.Sprintf("this is val%d", i))
			key := []byte(fmt.Sprintf("key%d", i))
			got, err := kv.Get(key)
			require.NoError(t, err, "getting value")
			assert.Equal(t, val, got, "getting value")
		}
	})
}
