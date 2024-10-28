package freelist

import (
	"dbms/btree"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

// how to test freelist
// freelist just keeps track of a list
// of pages and stored the unused pages
// and return the untraked file when
// asked for a page

// create a in memory cache

type C struct {
	Get func(uint64) []byte
	New func([]byte) uint64
	Use func(uint64, []byte)

	ref   map[string]string
	pages map[uint64][]byte
}

func newC() *C {
	pages := map[uint64][]byte{}
	return &C{
		Get: func(u uint64) []byte {
			p, ok := pages[u]
			if !ok {
				return nil
			}
			return p
		},

		New: func(b []byte) uint64 {
			if len(b) > btree.PAGE_SIZE {
				log.Fatalf("\nnode size greater then page, %d", len(b))
			}
			ptr := rand.Uint64()
			if pages[ptr] != nil {
				log.Fatalln("invalid page pointer, page exists")
			}
			pages[ptr] = b
			return ptr
		},

		Use: func(ptr uint64, data []byte) {
			pages[ptr] = data
		},

		ref:   map[string]string{},
		pages: pages,
	}
}

func TestFreeList(t *testing.T) {
	cache := newC()
	FREELIST_CAP = 3

	fl := NewFl(cache.Get, cache.Use, cache.New)

	t.Run("Update add freed pages to the list", func(t *testing.T) {

		// add new pages to the cache
		freed := []uint64{}
		pageSizeBytes := make([]byte, btree.PAGE_SIZE)
		for i := 0; i < 20; i++ {
			ptr := cache.New(pageSizeBytes)
			freed = append(freed, ptr)
		}

		// mark them as freed
		fl.Update(0, freed)

		assert.Equal(t, len(freed), int(fl.Total()))

		assert.Subset(t, freed, getFreeListPtrs(fl), "all pointers are stored in the freelist")
	})

	t.Run("Update pops first popn pages", func(t *testing.T) {
		// store the pointers to the first n pages
		toPop := []uint64{}
		toFree := []uint64{}
		pageSizeBytes := make([]byte, btree.PAGE_SIZE)
		for i := 0; i < 20; i++ {
			ptr := cache.New(pageSizeBytes)
			toFree = append(toFree, ptr)
		}
		popn := 10
		node := fl.get(fl.head)

		for len(toPop) < popn {
			size := flnSize(node)
			for i := 0; i < size; i++ {
				ptr := flnPtr(node, i)
				toPop = append(toPop, ptr)
			}
			node = fl.get(flnNext(node))
		}

		fmt.Println("popping nodes")
		fl.Update(popn, toFree)

		assert.NotSubset(t, getFreeListPtrs(fl), toPop)
	})
}

func getFreeListPtrs(fl *Fl) []uint64 {
	freePtrs := []uint64{}
	node := fl.get(fl.head)
	for node != nil {
		size := flnSize(node)

		for i := 0; i < size; i++ {
			ptr := flnPtr(node, i)
			freePtrs = append(freePtrs, ptr)
		}
		node = fl.get(flnNext(node))
	}
	return freePtrs
}
