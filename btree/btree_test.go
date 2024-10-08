package btree

import (
	"encoding/binary"
	"log"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// format used to store bytes is 	LITTLE ENDIAN

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

// node's wire frame
// | header | pointer1 | pointer2 | ..... |pointerN| offset1 |
// | offset2 | ..... | offsetN | kv-pair1 | kv-pair2 | ..... | kv-pairN |
//
// header - 4bytes
// | node type (2) | number of keys in node (2) \
//              ^ represents the size is 2 bytes
//
// pointer - 8 bytes -> stored a pointer to the node
// | pointer to another node (8) |
//
// offset - 2bytes -> stores the offset of the kv pair relative to the first node
// offset can also be though as pointer to the end of node, offset[0] -> points to the end of kv[0]
// offset[1] -> points to the end of kv[1]
// | offset (2) |
//
// kv-pair
// | length of key(2) | length of value(2) | key (x) | val(y) |
// size of key and value may vary so the number of bytes required to then is a variable

func TestBNode(t *testing.T) {

	t.Run("btype returns the type of the node as per the header", func(t *testing.T) {
		want := BNODE_LEAF
		var b BNode = make([]byte, PAGE_SIZE)
		binary.LittleEndian.PutUint16(b[0:], uint16(want))
		assert.Equal(t, uint16(want), b.btype(), "getting node type")
	})

	t.Run("nkeys returns the number of keys as per the header", func(t *testing.T) {
		want := 2
		var b BNode = make([]byte, PAGE_SIZE)
		binary.LittleEndian.PutUint16(b[2:], uint16(want))
		assert.Equal(t, uint16(want), b.nkeys(), "getting node type")
	})

	t.Run("setheader sets the headers ntype, nkeys", func(t *testing.T) {
		var b BNode = make([]byte, 0, PAGE_SIZE)
		btype := uint16(BNODE_LEAF)
		nkeys := uint16(4)
		b.setHeader(btype, nkeys)
		assert.Equal(t, btype, b.btype(), "checking btype of node")
		assert.Equal(t, nkeys, b.nkeys(), "checking nkeys of node")
	})

	t.Run("getptr retreves the pointer at given idx", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 5)
		ptr := uint64(456456456) // any number
		idx := uint16(3)
		// pointers start from 4th byte
		// length of each pointer is 8 bytes
		binary.LittleEndian.PutUint64(b[HEADER+idx*POINTER:], ptr)
		assert.Equal(t, uint64(ptr), b.getptr(idx), "getting pointer")
	})

	t.Run("setptr sets the pointer at given idx", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 5)
		ptr := uint64(123456)
		idx := uint16(3)
		b.setptr(idx, ptr)
		assert.Equal(t, ptr, b.getptr(idx), "setting pointer")
	})

	t.Run("offsetPos returns the offsetPos relative to the start of node", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 5)
		idx := 3
		// index is subtracted by one because
		// offset[0] gives the offset of kv-pair[1]
		want := HEADER + int(b.nkeys())*POINTER + OFFSET*(idx-1)
		assert.Equal(t, uint16(want), b.offsetPos(uint16(idx)), "getting offset position")
	})

	t.Run("getOffset returns the offset stored at given index", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_NODE, 3)
		val := uint16(123)
		idx := uint16(2)
		offsetStartPos := b.offsetPos(idx)
		binary.LittleEndian.PutUint16(b[offsetStartPos:], val)
		assert.Equal(t, val, b.getOffset(idx), "getting offset ")
	})

	t.Run("setOffset sets the offset at a given index to the given value", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_NODE, 5)
		want := uint16(1256)
		b.setOffset(3, want)
		assert.Equal(t, want, b.getOffset(3), "setting offset")
	})

	t.Run("kvPos returns kv-pair offset from starting of node", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 5)

		spos := HEADER + POINTER*b.nkeys() + OFFSET*b.nkeys()
		b.setOffset(1, 500)
		assert.Equal(t, spos, b.kvPos(0), "getting kvpos 0")
		assert.Equal(t, spos+500, b.kvPos(1), "getting kvpos 1")
	})

	t.Run("getKey returns key at a given index", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 5)
		// set stub kv-pairs
		spos := HEADER + POINTER*b.nkeys() + OFFSET*b.nkeys()
		kv := getKVpair([]byte("hello"), []byte("world"))
		copy(b[spos:], kv)
		assert.Equal(t, []byte("hello"), b.getKey(0))
	})

	t.Run("getVal returns val at a given index", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 5)
		// set stub kv-pairs
		spos := HEADER + POINTER*b.nkeys() + OFFSET*b.nkeys()
		kv1 := getKVpair([]byte("hello"), []byte("world"))
		kv2 := getKVpair([]byte("world"), []byte("hello's back"))
		copy(b[spos:], kv1)
		b.setOffset(1, uint16(len(kv1)))
		copy(b[int(spos)+len(kv1):], kv2)
		assert.Equal(t, []byte("world"), b.getVal(0))
	})

	t.Run("nbytes returns total number of bytes in a node", func(t *testing.T) {
		var b BNode = make([]byte, PAGE_SIZE)
		b.setHeader(BNODE_LEAF, 2)
		// set stub kv-pairs
		spos := HEADER + POINTER*b.nkeys() + OFFSET*b.nkeys()
		kv1 := getKVpair([]byte("hello"), []byte("world"))
		kv2 := getKVpair([]byte("world"), []byte("hello's back"))

		copy(b[spos:], kv1)
		b.setOffset(1, uint16(len(kv1)))

		copy(b[int(spos)+len(kv1):], kv2)
		b.setOffset(2, uint16(len(kv2)+len(kv1)))
		nodeLen := HEADER + POINTER*b.nkeys() + OFFSET*b.nkeys() +
			uint16(len(kv1)) + uint16(len(kv2))
		assert.Equal(t, nodeLen, b.nbytes(), "node length")
	})
}

func getKVpair(key, val []byte) []byte {
	var kv []byte = make([]byte, KVHEADER+len(key)+len(val))
	binary.LittleEndian.PutUint16(kv, uint16(len(key)))
	binary.LittleEndian.PutUint16(kv[2:], uint16(len(val)))
	copy(kv[KVHEADER:], key)
	copy(kv[KVHEADER+len(key):], val)
	return kv
}
