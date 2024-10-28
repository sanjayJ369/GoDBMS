package freelist

import (
	"dbms/btree"
	"encoding/binary"
	"fmt"
)

const (
	FREELIST_TYPE    = 2
	FREELIST_SIZE    = 2
	FREELIST_TOTAL   = 8
	FREELIST_NEXT    = 8
	FREELIST_HEADER  = 2 + 2 + 8 + 8
	FREELIST_POINTER = 8
)

var FREELIST_CAP = (btree.PAGE_SIZE - FREELIST_HEADER) / 8

// node write frame
// \ type | size | total |  next  |    pointers     |
// |  2B  |  2B  |   8B  |   8B   |    size * 8B    |

type Fl struct {
	head uint64

	// callback funcs
	get func(uint64) []byte
	use func(uint64, []byte)
	new func([]byte) uint64
}

func NewFl(get func(uint64) []byte, use func(uint64,
	[]byte), new func([]byte) uint64) *Fl {
	fl := &Fl{
		get: get,
		use: use,
		new: new,
	}

	head := make([]byte, btree.PAGE_SIZE)
	flnSetHeader(head, 0, 0)
	flnSetTotal(head, 0)
	headptr := fl.new(head)

	fl.head = headptr

	return fl
}

func (f *Fl) Get(topn int) uint64 {
	node := f.get(f.head)
	for flnSize(node) < topn {
		next := flnNext(node)
		if next == 0 {
			panic("no next node")
		}
		nextNode := f.get(next)
		topn -= flnSize(node)
		node = nextNode
	}
	ptr := flnPtr(node, topn)
	return ptr
}

func (f *Fl) Total() uint64 {
	head := f.get(f.head)
	return flnGetTotal(head)
}

func printNode(fl *Fl) {
	node := fl.get(fl.head)
	fmt.Println(fl.head)
	for node != nil {
		size := flnSize(node)

		for i := 0; i < size; i++ {
			ptr := flnPtr(node, i)
			fmt.Println("\t", ptr)
		}
		fmt.Println(flnNext(node))
		node = fl.get(flnNext(node))
	}
}

func (f *Fl) Update(popn int, freed []uint64) {
	if popn == 0 && len(freed) == 0 {
		return
	}

	total := int(f.Total())
	reuse := []uint64{}

	// until there is enough space to store
	// pointers to all the freed pages
	for f.head != 0 && len(reuse)*FREELIST_CAP < len(freed) {

		node := f.get(f.head)

		// pop nodes
		if flnSize(node) < popn {
			popn -= flnSize(node)
		} else {
			remain := flnSize(node) - popn
			popn = 0
			/*
				here, the pointers in the page are handled a bit
				differently, inside a new it does not matter in which order
				we remove the pointers and add them to the feed or reused
				to make things easy, we are adding the first n pointers to the free
				then some of the pointers to the reuse and the rest of the pointers
				are popped
			*/

			/*
				pop remaining pointers in the node
				keep on adding extra(reuse) pointer until there
				is enough space to store freed node/pages
				here rest of the remaining pointer in the node
				is added to the freed slice
			*/
			for remain > 0 && len(reuse)*FREELIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}

			// add rest of the pointer in the node
			// to the freed list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}

		total -= flnSize(node)
		f.head = flnNext(node)
	}

	if len(reuse)*FREELIST_CAP < len(freed) {
		// add additional pages to stored the freed pointer
		for len(reuse)*FREELIST_CAP < len(freed) {
			newNode := f.new(make([]byte, btree.PAGE_SIZE))
			reuse = append(reuse, newNode)
		}
	}

	flPush(f, freed, reuse)
	flnSetTotal(f.get(f.head), uint64(total)+uint64(len(freed)))
}

func flPush(f *Fl, freed []uint64, reuse []uint64) {
	// there still some elements to insert
	for len(freed) > 0 {
		new := make([]byte, btree.PAGE_SIZE)

		size := len(freed)
		if size > FREELIST_CAP {
			size = FREELIST_CAP
		}

		flnSetHeader(new, uint16(size), f.head)

		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}

		freed = freed[size:]

		if len(reuse) > 0 {
			f.head, reuse = reuse[0], reuse[1:]
			f.use(f.head, new)
		} else {
			f.head = f.new(new)
		}
	}

	if len(reuse) > 0 {
		panic("reuse pages are not completely used")
	}
}

// functions to handle the node values

// flnSize returns the size of the node
// size refers to the number of pointers to
// the unused pages stored in this node
func flnSize(node []byte) int {
	spos := FREELIST_TYPE
	return int(binary.LittleEndian.Uint16(node[spos:]))
}

func flnNext(node []byte) uint64 {
	spos := FREELIST_TYPE + FREELIST_SIZE +
		FREELIST_TOTAL
	return binary.LittleEndian.Uint64(node[spos:])
}

func flnPtr(node []byte, idx int) uint64 {
	spos := FREELIST_HEADER + idx*FREELIST_POINTER
	return binary.LittleEndian.Uint64(node[spos:])
}

func flnSetPtr(node []byte, idx int, ptr uint64) {
	spos := FREELIST_HEADER + idx*FREELIST_POINTER
	binary.LittleEndian.PutUint64(node[spos:], ptr)
}

func flnSetHeader(node []byte, size uint16, next uint64) {
	// set size
	spos := FREELIST_TYPE
	binary.LittleEndian.PutUint16(node[spos:], size)

	// set next
	spos = FREELIST_TYPE + FREELIST_SIZE + FREELIST_TOTAL
	binary.LittleEndian.PutUint64(node[spos:], next)
}

func flnSetTotal(node []byte, total uint64) {
	spos := FREELIST_TYPE + FREELIST_SIZE
	binary.LittleEndian.PutUint64(node[spos:], total)
}

func flnGetTotal(node []byte) uint64 {
	spos := FREELIST_TYPE + FREELIST_SIZE
	return binary.LittleEndian.Uint64(node[spos:])
}
