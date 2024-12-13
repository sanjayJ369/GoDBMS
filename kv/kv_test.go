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

func TestConcurrentTransactions(t *testing.T) {
	loc := util.NewTempFileLoc()
	store, err := NewKv(loc)
	require.NoError(t, err)

	t.Run("transaction version is monotonically increasing", func(t *testing.T) {
		kv1 := NewKVTX()
		kv2 := NewKVTX()
		store.Begin(kv1)
		store.Begin(kv2)

		assert.Equal(t, kv1.version+1, kv2.version)
	})

	t.Run("kv version of transactions remains same, if there are created by same kv store version", func(t *testing.T) {
		kv1 := NewKVTX()
		kv2 := NewKVTX()
		store.Begin(kv1)
		store.Begin(kv2)
		assert.Equal(t, kv1.kvVersion, kv2.kvVersion)
	})

	t.Run("kv store does not allow older transactions to commit, if there are conflicts", func(t *testing.T) {
		tx := NewKVTX()
		store.Begin(tx)
		tx.Set([]byte("key1"), []byte("val1"))

		newtx := NewKVTX()
		oldtx := NewKVTX()
		store.Begin(newtx)
		store.Begin(oldtx)

		newtx.Set([]byte("key1"), []byte("updated val"))
		oldtx.Get([]byte("key1"))
		err := store.Commit(newtx)
		require.NoError(t, err)

		err = store.Commit(oldtx)
		require.Error(t, err)
	})

	t.Run("kv store does allows older transactions to commit, if there are no conflicts", func(t *testing.T) {
		tx := NewKVTX()
		store.Begin(tx)
		tx.Set([]byte("key1"), []byte("val1"))
		tx.Set([]byte("key2"), []byte("val2"))

		newtx := NewKVTX()
		oldtx := NewKVTX()
		store.Begin(newtx)
		store.Begin(oldtx)

		newtx.Set([]byte("key1"), []byte("updated val"))
		oldtx.Set([]byte("key2"), []byte("updated val"))
		err := store.Commit(newtx)
		require.NoError(t, err)

		err = store.Commit(oldtx)
		require.NoError(t, err)
	})

	t.Run("reader can still read even if the writer deletes the keys", func(t *testing.T) {
		tx := NewKVTX()
		store.Begin(tx)
		var keys [][]byte
		var values [][]byte
		count := 100

		// generate key value pairs
		for i := 0; i < count; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			// size of the value is 500 bytes
			value := make([]byte, 500)
			copy(value, []byte(fmt.Sprintf("value: %d", i)))
			keys = append(keys, key)
			values = append(values, value)
		}

		// insert kv pairs
		for i := range count {
			tx.Set(keys[i], values[i])
		}

		// commit
		store.Commit(tx)

		readtx := NewKVTX()
		writetx := NewKVTX()

		store.Begin(readtx)
		store.Begin(writetx)

		// delete all the kv pairs
		for i := range count {
			ok := writetx.Del(keys[i])
			require.True(t, ok)
		}
		store.Commit(writetx)
		// read deleted kv pairs
		for i := range count {
			val, err := readtx.Get(keys[i])
			require.NoError(t, err, "getting key")
			assert.Equal(t, values[i], val)
		}
	})
}
