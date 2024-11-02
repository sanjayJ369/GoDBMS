package freelist

import (
	"dbms/btree"
	"log"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type C struct {
	ref   map[string]string
	pages map[uint64][]byte

	Get func(uint64) []byte
	New func([]byte) uint64
	Set func(uint64) []byte
}

func newC() *C {
	pages := map[uint64][]byte{}
	return &C{

		Get: func(u uint64) []byte {
			p, ok := pages[u]
			if !ok {
				log.Fatalln("invalid page pointer, page does not exist")
			}
			return p
		},

		New: func(b []byte) uint64 {
			if len(b) > btree.PAGE_SIZE {
				log.Fatalf("\nnode size greater then page, %d", len(b))
			}
			ptr := uint64(uintptr(unsafe.Pointer(&b[0])))
			if pages[ptr] != nil {
				log.Fatalln("invalid page pointer, page exists")
			}
			pages[ptr] = b
			return ptr
		},

		Set: func(u uint64) []byte {
			if node, ok := pages[u]; ok {
				return node
			}
			node := make([]byte, btree.PAGE_SIZE)
			pages[u] = node
			return node
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func TestFreelist(t *testing.T) {

	cache := newC()
	t.Run("test push tail", func(t *testing.T) {

		fl := Fl{}
		fl.Get = cache.Get
		fl.Set = cache.Set
		fl.New = cache.New

		ptrs := []uint64{}
		// create a new a few new pages
		for i := 0; i < 1000; i++ {
			page := make([]byte, btree.PAGE_SIZE)
			ptr := cache.New(page)
			ptrs = append(ptrs, ptr)
			fl.PushTail(ptr)
		}

		freePages := GetUnusedPages(&fl)

		// first page is popped from the freelist to be reused
		// for new page
		assert.Equal(t, ptrs[1:], freePages, "pointers are not added")
	})

	t.Run("test pop head", func(t *testing.T) {
		fl := Fl{}
		fl.Get = cache.Get
		fl.Set = cache.Set
		fl.New = cache.New

		ptrs := []uint64{}
		// insert 10 pages
		for i := 0; i < 10; i++ {
			page := make([]byte, btree.PAGE_SIZE)
			ptr := cache.New(page)
			ptrs = append(ptrs, ptr)
			fl.PushTail(ptr)
		}

		// pop 10 pages
		for i := 0; i < 10; i++ {
			ptr := fl.PopHead()
			assert.Contains(t, ptrs, ptr)
		}

		head := fl.HeadPage
		ptr := fl.PopHead()
		assert.Equal(t, head, ptr)
	})
}
