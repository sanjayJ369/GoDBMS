package freelist

import (
	"dbms/btree"
	"encoding/binary"
	"fmt"
)

const (
	BNODE_FREE_LIST   = 3
	FREE_LIST_HEADER  = 4 + 8 + 8
	FREE_LIST_CAP     = (btree.PAGE_SIZE - FREE_LIST_HEADER) / 8
	FREE_LIST_TYPE    = 2
	FREE_LIST_SIZE    = 2
	FREE_LIST_TOTAL   = 8
	FREE_LIST_NEXT    = 8
	FREE_LIST_POINTER = 8
)

type BNode btree.BNode

// FreeList Node Format
// | type | size | total | next | pointer |
// |  2B  |  2B  |   8B  |  8B  | size*8B |
type FreeList struct {
	head uint64

	// function for managing on-disk page
	get func(uint64) BNode  // dereference a pointer
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a pointer
}

func flnSize(node BNode) int {
	spos := FREE_LIST_TYPE
	return int(binary.LittleEndian.Uint16(node[spos:]))
}

func flnNext(node BNode) uint64 {
	spos := FREE_LIST_TYPE + FREE_LIST_SIZE + FREE_LIST_TOTAL
	return binary.LittleEndian.Uint64(node[spos:])
}

func flnPtr(node BNode, idx int) uint64 {
	spos := FREE_LIST_TYPE + FREE_LIST_SIZE +
		FREE_LIST_TOTAL + FREE_LIST_NEXT + FREE_LIST_POINTER*(idx)
	return binary.LittleEndian.Uint64(node[spos:])
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	spos := FREE_LIST_TYPE + FREE_LIST_SIZE +
		FREE_LIST_TOTAL + FREE_LIST_NEXT + FREE_LIST_POINTER*(idx)
	binary.LittleEndian.PutUint64(node[spos:], ptr)
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	// set size
	spos := FREE_LIST_TYPE
	binary.LittleEndian.PutUint16(node[spos:], size)

	// set next
	spos = FREE_LIST_TYPE + FREE_LIST_SIZE + FREE_LIST_TOTAL
	binary.LittleEndian.PutUint64(node[spos:], next)
}

func flnSetTotal(node BNode, total uint64) {
	spos := FREE_LIST_TYPE + FREE_LIST_SIZE
	binary.LittleEndian.PutUint64(node[spos:], total)
}

// number of pages in the list
func (fl *FreeList) Total() int {
	return 0
}

// get nth pointer
func (fl *FreeList) Get(topn int) uint64 {
	if topn < 0 || topn > fl.Total()-1 {
		panic(fmt.Sprintf("out of bounds, topn: %d, size: %d", topn, fl.Total()))
	}

	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		if next == 0 {
			panic("node does not exists")
		}
		node = fl.get(next)
	}

	return flnPtr(node, flnSize(node)-topn)
}

// remove topn pointers and add some new pointers
func (fl *FreeList) Update(popn int, freed []uint64) {

	if popn == 0 && len(freed) == 0 {
		return
	}

	total := fl.Total()
	reuse := []uint64{}

	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head)

		if popn >= flnSize(node) {
			// still need to pop the nodes
			popn -= flnSize(node)
		} else {
			// remove some pointers to account for updated nodes
			remain := flnSize(node) - popn
			popn = 0

			// if there are still pointers remaining in this node
			// and if current reuse pages are not enough to store
			// the pointers keep on poping the new pages and add it
			// to the reuse, until we have enough space
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}

			// move the freed nodes to the freelist
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}

		total -= flnSize(node)
		fl.head = flnNext(node)
	}

	if len(reuse)*FREE_LIST_CAP < len(freed) || fl.head == 0 {
		panic("not enough space store freed pages")
	}

	flPush(fl, freed, reuse)

	flnSetTotal(fl.get(fl.head), uint64(total+len(freed)))
}

func flPush(fl *FreeList, freed, reuse []uint64) {
	for len(freed) > 0 {
		// there are sill pages left to be added
		// new a new inmemory node
		new := BNode(make([]byte, btree.PAGE_SIZE))

		// manimum number of pointers that can be stored in the page
		size := len(freed)
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}

		// set the pointers
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}

		flnSetHeader(new, uint16(size), fl.head)

		freed = freed[size:]

		if len(reuse) > 0 {
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			fl.head = fl.new(new)
		}
	}

	if len(reuse) != 0 {
		panic("free nodes are not properly reused")
	}
}
