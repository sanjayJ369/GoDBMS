package freelist

import (
	"dbms/btree"
	"encoding/binary"
	"fmt"
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

	HeadPage uint64
	HeadSeq  uint64
	TailPage uint64
	TailSeq  uint64

	MaxSeq uint64
}

func (fl *Fl) PopHead() uint64 {
	ptr, head := flpop(fl)
	// if the head is empty recycle the head node
	if head != 0 {
		fl.PushTail(head)
	}
	if ptr != 0 {
		fmt.Println("pointer returned: ", ptr, " max: ", fl.MaxSeq, " head: ", fl.HeadSeq)
	}
	return ptr
}

func (fl *Fl) PushTail(ptr uint64) {
	// freelist is empty
	// add a new page as head
	if fl.TailPage == 0 {
		newNode := FlNode(make([]byte, btree.PAGE_SIZE))
		newptr := fl.New(newNode)
		fl.HeadPage = newptr
		fl.TailPage = newptr
		fl.TailSeq = 0
		fl.HeadSeq = 0
		newNode.setPtr(int(fl.TailSeq), ptr)
		fl.TailSeq++
		return
	}

	node := FlNode(fl.Set(fl.TailPage))
	node.setPtr(int(seq2Idx(fl.TailSeq)), ptr)
	fl.TailSeq++

	// if current tailnode is full
	// add a new page
	if seq2Idx(fl.TailSeq) == 0 {
		next, head := flpop(fl)
		if next == 0 {
			// no new pages
			// append a new page
			next = fl.New(make([]byte, btree.PAGE_SIZE))
		}
		node := FlNode(fl.Get(fl.TailPage))
		node.setNext(next)
		fl.TailPage = next

		// if head node is not null
		// add the node to the lsit
		if head != 0 {
			FlNode(fl.Get(fl.TailPage)).setPtr(0, head)
			fl.TailSeq++
		}
	}
}

func GetUnusedPages(fl *Fl) []uint64 {
	if fl.HeadPage == 0 {
		return nil
	}
	node := FlNode(fl.Get(fl.HeadPage))
	seq := fl.HeadSeq
	freePages := []uint64{}

	for seq < fl.TailSeq {
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
	fl.HeadPage = 0
	fl.TailPage = 0
	fl.TailSeq = 0
	fl.HeadSeq = 0
}

func flpop(fl *Fl) (ptr uint64, head uint64) {
	// check if head seq is less then max seq
	// to not give always pages that are currently
	// been read by older transactions
	if fl.HeadSeq > fl.MaxSeq {
		return
	}

	if fl.HeadSeq == fl.TailSeq {
		ptr = fl.HeadPage
		fl.reset()
		return
	}
	node := FlNode(fl.Get(fl.HeadPage))
	ptr = node.getPtr(seq2Idx(fl.HeadSeq))
	fl.HeadSeq++

	// if at end of node
	// set head to next node
	if seq2Idx(fl.HeadSeq) == 0 {
		head = fl.HeadPage
		fl.HeadPage = node.getNext()
	}
	return
}

func seq2Idx(seq uint64) uint64 {
	return seq % uint64(FREELIST_CAP)
}
