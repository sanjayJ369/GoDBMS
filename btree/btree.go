package btree

import (
	"dbms/util"
	"encoding/binary"
	"log"
)

const (
	PAGE_SIZE = 4096
	HEADER    = 4
	POINTER   = 8
	OFFSET    = 2
	KLEN      = 2
	VLEN      = 2
	KVHEADER  = 4 // keylen = 2, vallen = 2
	MAXKEYLEN = 1000
	MAXVALLEN = 3000

	// header
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

// the data is written and read in littleEndian format
type BNode []byte

type BTree struct {
	root uint64              // pointer to page
	get  func(uint64) []byte // get the page
	new  func() uint64       // allocate a new page
	del  func(uint64)        // allocate87 a page
}

func init() {
	// max size of node
	node1max := HEADER + POINTER + OFFSET + (KVHEADER + MAXKEYLEN + MAXVALLEN)

	// check if the max node fits in a page
	util.Assert(node1max <= PAGE_SIZE)
}

// functions to decode node formats
// header
func (b BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(b[0:2])
}

func (b BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(b[2:4])
}

func (b BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(b[0:2], btype)
	binary.LittleEndian.PutUint16(b[2:4], nkeys)
}

// handle child pointers
func (b BNode) getptr(idx uint16) uint16 {
	if idx >= b.nkeys() {
		log.Fatalf("out of bounds, index: %d, nkeys: %d", idx, b.nkeys())
	}
	sbyte := HEADER + (idx * 8)
	return uint16(binary.LittleEndian.Uint64(b[sbyte:]))
}

func (b BNode) setptr(idx uint16, ptr uint64) {
	if idx >= b.nkeys() {
		log.Fatalf("out of bounds, index: %d, nkeys: %d", idx, b.nkeys())
	}
	sbyte := HEADER + (idx * 8)
	binary.LittleEndian.PutUint64(b[sbyte:], ptr)
}

// handle offset
// offset stores the distance between first KV node to given KV pair
// offset of 1st KV is 0
// offset of 2nd KV is the value pointed by 1st offset
// because each offset points to the end of each KV pair
// 1st offset points to the end of 1st KV pair
// 2nd offset points to the end of 2nd KV pair
// and so on

// offsets are stored back to back as an array
// offsetPos function returns offset at a given index
// offsetPos(idx) is same as offsets[idx]
func (b BNode) offsetPos(idx uint16) uint16 {
	if idx > b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}

	sbyte := HEADER + (b.nkeys() * POINTER) + ((idx - 1) * OFFSET)
	return sbyte
}

func (b BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(b[b.offsetPos(idx):])
}

func (b BNode) setOffset(idx uint16, val uint16) {
	if idx > b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	sbyte := b.getOffset(idx)
	binary.LittleEndian.PutUint16(b[sbyte:], val)
}

func (b BNode) kvPos(idx uint16) uint16 {
	if idx > b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	offset := b.getOffset(idx)
	sbyte := offset + HEADER + (b.nkeys() * POINTER) + (b.nkeys() * OFFSET)
	return sbyte
}

func (b BNode) getKey(idx uint16) []byte {
	if idx >= b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	kvpos := b.kvPos(idx)
	klen := binary.LittleEndian.Uint16(b[kvpos:])
	kpos := KVHEADER + klen
	// b[kpos:][:klen] same as b[kpos:(kpos + klen)]
	return b[kpos:][:klen]
}

func (b BNode) getVal(idx uint16) []byte {
	if idx >= b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	kvpos := b.kvPos(idx)
	klen := binary.LittleEndian.Uint16(b[kvpos:])
	vlen := binary.LittleEndian.Uint16(b[kvpos+KLEN:])
	vpos := KVHEADER + klen
	return b[vpos:][:vlen]
}

func (b BNode) nbytes() uint16 {
	return b.kvPos(b.nkeys())
}
