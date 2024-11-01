package kv

import (
	"bufio"
	"dbms/btree"
	"dbms/util"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {

	count := 10
	size := 500

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

	t.Run("set and get the 5000 kv pairs", func(t *testing.T) {

		startTime := time.Now()
		for i := 0; i < count; i++ {
			val := make([]byte, size)
			copy(val, fmt.Sprintf("this is val%d", i))
			key := []byte(fmt.Sprintf("key%d", i))

			err := kv.Set(key, val)
			require.NoError(t, err, "setting new kvpair")
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

		fmt.Println("root node:", kv.tree.Root)
		btree.PrintNode(kv.tree.Get(kv.tree.Root))
		printPages(kv.Path)

		// fmt.Println("\ntree:")
		// btree.PrintTree(kv.tree, kv.tree.Get(kv.tree.Root))

		fmt.Printf("\ntime taken(%d): %ds\n", count, endTime.Second()-startTime.Second())
		kv.Close()
	})

	t.Run("data is persisted to the disk", func(t *testing.T) {

		kv, err = NewKv(loc)
		require.NoError(t, err, "opening kv")

		fmt.Println("root node:", kv.tree.Root)
		btree.PrintNode(kv.tree.Get(kv.tree.Root))
		printPages(kv.Path)

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

func printPages(file string) {
	fp, err := os.Open(file)
	if err != nil {
		log.Fatalf("opening file: %s", err.Error())
	}

	fmt.Println("\nfile pages:")
	buf := make([]byte, btree.PAGE_SIZE)
	fi, err := fp.Stat()
	if err != nil {
		log.Fatalf("getting file stats: %s", err.Error())
	}

	if fi.Size()%btree.PAGE_SIZE != 0 {
		log.Fatal("file size of not a multiple of page size")
	}

	npages := fi.Size() / btree.PAGE_SIZE

	reader := bufio.NewReader(fp)
	for i := 0; i < int(npages); i++ {
		_, err := reader.Read(buf)
		if err != nil {
			log.Fatalf("reading data from file into buffer: %s", err.Error())
		}

		btree.PrintNode(btree.BNode(buf))
		fmt.Println()
	}
}
