package kv

import (
	"dbms/btree"
	"dbms/util"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {

	loc := util.NewTempFileLoc()
	kv, err := NewKv(loc)
	require.NoError(t, err, "creating new kv")

	require.NoError(t, err, "opening kv")
	t.Run("set and get the kv pair", func(t *testing.T) {
		err := kv.Set([]byte("hello"), []byte("world"))
		require.NoError(t, err, "setting new kvpair")

		val, err := kv.Get([]byte("hello"))
		require.NoError(t, err, "getting value")
		assert.Equal(t, "world", string(val), "getting value")
	})

	t.Run("set and get the 100000 kv pairs", func(t *testing.T) {
		for i := 0; i < 5000; i++ {
			val := make([]byte, 500)
			copy(val, fmt.Sprintf("this is val%d", i))
			key := []byte(fmt.Sprintf("key%d", i))

			err := kv.Set(key, val)
			require.NoError(t, err, "setting new kvpair")

			got, err := kv.Get(key)
			require.NoError(t, err, "getting value")
			assert.Equal(t, val, got, "getting value")
		}
		fmt.Println("root node:")
		btree.PrintNode(kv.tree.Get(kv.tree.Root))

		fmt.Println("\ntree:")
		btree.PrintTree(kv.tree, kv.tree.Get(kv.tree.Root))
		fmt.Println(kv.tree.Root)
		kv.Close()
	})

	t.Run("data is persisted to the disk", func(t *testing.T) {

		kv, err = NewKv(loc)
		require.NoError(t, err, "opening kv")
		fmt.Println("root node:")
		btree.PrintNode(kv.tree.Get(kv.tree.Root))

		fmt.Println("\ntree:")
		btree.PrintTree(kv.tree, kv.tree.Get(kv.tree.Root))
		fmt.Println(kv.tree.Root)
		for i := 0; i < 5000; i++ {
			val := make([]byte, 500)
			copy(val, fmt.Sprintf("this is val%d", i))
			key := []byte(fmt.Sprintf("key%d", i))

			got, err := kv.Get(key)
			require.NoError(t, err, "getting value")
			assert.Equal(t, val, got, "getting value")
		}
	})
}