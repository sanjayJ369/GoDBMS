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
	get func(uint64) []byte
	new func([]byte) uint64
	set func(uint64) []byte

	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64
	maxSeq   uint64
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
	node := FlNode(fl.set(fl.tailPage))
	node.setPtr(int(seq2Idx(fl.tailSeq)), ptr)
	fl.tailSeq += 1

	// if current tailnode is full
	// add a new page
	if seq2Idx(fl.tailSeq) != 0 {
		next, head := flpop(fl)
		if next == 0 {
			// no new pages
			// append a new page
			next = fl.new(make([]byte, btree.PAGE_SIZE))
		}
		node := FlNode(fl.get(fl.tailPage))
		node.setNext(next)
		fl.tailPage = next

		// if head node is not null
		// add the node to the lsit
		if head != 0 {
			FlNode(fl.get(fl.tailPage)).setPtr(0, head)
			fl.tailSeq += 1
		}
	}
}

func (fl *Fl) SetMaxSeq() {
	fl.maxSeq = fl.tailSeq
}

func flpop(fl *Fl) (ptr uint64, head uint64) {
	if fl.headSeq == fl.tailSeq {
		return 0, 0
	}
	node := FlNode(fl.get(fl.headPage))
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
