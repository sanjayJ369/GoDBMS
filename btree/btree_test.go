package btree

import (
	"log"
	"unsafe"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(u uint64) []byte {
				p, ok := pages[u]
				if !ok {
					log.Fatalln("invalid page pointer")
				}
				return p
			},

			new: func(b []byte) uint64 {
				if len(b) > PAGE_SIZE {
					log.Fatalf("\nnode size greater then page, %d", len(b))
				}
				ptr := uint64(uintptr(unsafe.Pointer(&b[0])))
				if pages[ptr] != nil {
					log.Fatalln("invalid page pointer")
				}
				pages[ptr] = b
				return ptr
			},

			del: func(ptr uint64) {
				if pages[ptr] != nil {
					delete(pages, ptr)
				}
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}
