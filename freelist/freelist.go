package freelist

import (
	"dbms/btree"
	"encoding/binary"
)

const (
	FREELIST_NEXT    = 8
	FREELIST_HEADER  = 8
	FREELIST_POINTER = 8
)

var FREELIST_CAP = (btree.PAGE_SIZE - FREELIST_HEADER) / 8

// node write frame
// |  next  |    pointers     |
// |   8B   |    size * 8B    |

type FlNode []byte

func (node FlNode) getNext() uint64 {
	spos := 0
	return binary.LittleEndian.Uint64(node[spos:])
}

func (node FlNode) setNext(next uint64) {
	spos := 0
	binary.LittleEndian.PutUint64(node[spos:], next)
}

func (node FlNode) getPtr(idx uint64) uint64 {
	spos := FREELIST_NEXT + (FREELIST_POINTER * idx)
	return binary.LittleEndian.Uint64(node[spos:])
}

func (node FlNode) setPtr(idx int, ptr uint64) {
	spos := FREELIST_NEXT + (FREELIST_POINTER * idx)
	binary.LittleEndian.PutUint64(node[spos:], ptr)
}

type Fl struct {
	// callback funcs
	Get func(uint64) []byte
	New func([]byte) uint64
	Set func(uint64) []byte

	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
}

func (fl *Fl) PopHead() uint64 {
	ptr, head := flpop(fl)
	// if the head is empty recycle the head node
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

func (fl *Fl) PushTail(ptr uint64) {
	// freelist is empty
	// add a new page as head
	if fl.tailPage == 0 {
		newNode := FlNode(make([]byte, btree.PAGE_SIZE))
		newptr := fl.New(newNode)
		fl.headPage = newptr
		fl.tailPage = newptr
		fl.tailSeq = 0
		fl.headSeq = 0
		newNode.setPtr(int(fl.tailSeq), ptr)
		fl.tailSeq++
		return
	}

	node := FlNode(fl.Set(fl.tailPage))
	node.setPtr(int(seq2Idx(fl.tailSeq)), ptr)
	fl.tailSeq++

	// if current tailnode is full
	// add a new page
	if seq2Idx(fl.tailSeq) == 0 {
		next, head := flpop(fl)
		if next == 0 {
			// no new pages
			// append a new page
			next = fl.New(make([]byte, btree.PAGE_SIZE))
		}
		node := FlNode(fl.Get(fl.tailPage))
		node.setNext(next)
		fl.tailPage = next

		// if head node is not null
		// add the node to the lsit
		if head != 0 {
			FlNode(fl.Get(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

func (fl *Fl) getUnusedPages() []uint64 {
	node := FlNode(fl.Get(fl.headPage))
	seq := fl.headSeq
	freePages := []uint64{}

	for seq < fl.tailSeq {
		idx := seq2Idx(seq)
		seq += 1

		ptr := node.getPtr(idx)
		freePages = append(freePages, ptr)
		if seq2Idx(seq) == 0 {
			node = fl.Get(node.getNext())
		}
	}

	return freePages
}

func (fl *Fl) reset() {
	fl.headPage = 0
	fl.tailPage = 0
	fl.tailSeq = 0
	fl.headSeq = 0
}

func flpop(fl *Fl) (ptr uint64, head uint64) {
	if fl.headSeq == fl.tailSeq {
		ptr = fl.headPage
		head = ptr
		fl.reset()
		return
	}
	node := FlNode(fl.Get(fl.headPage))
	ptr = node.getPtr(seq2Idx(fl.headSeq))
	fl.headSeq++

	// if at end of node
	// set head to next node
	if seq2Idx(fl.headSeq) == 0 {
		head = fl.headPage
		fl.headPage = node.getNext()
	}
	return
}

func seq2Idx(seq uint64) uint64 {
	return seq % uint64(FREELIST_CAP)
}
