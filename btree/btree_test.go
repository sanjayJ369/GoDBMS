package btree

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			Get: func(u uint64) []byte {
				p, ok := pages[u]
				if !ok {
					log.Fatalln("invalid page pointer, page does not exist")
				}
				return p
			},

			New: func(b []byte) uint64 {
				if len(b) > PAGE_SIZE {
					log.Fatalf("\nnode size greater then page, %d", len(b))
				}
				ptr := uint64(uintptr(unsafe.Pointer(&b[0])))
				if pages[ptr] != nil {
					log.Fatalln("invalid page pointer, page exists")
				}
				pages[ptr] = b
				return ptr
			},

			Del: func(ptr uint64) {
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

func TestBNodeHelperFuncs(t *testing.T) {

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
		b.setOffset(0, want)
		assert.Equal(t, want, b.getOffset(3), "setting offset 3")
		assert.Equal(t, uint16(0), b.getOffset(0), "setting offset 0")
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

func TestBNodeManipulationFuncs(t *testing.T) {
	// create a stub key
	kvpairs := make([][]byte, 0)
	for i := 0; i < 4; i++ {
		k := []byte(fmt.Sprintf("this is key%d", i))
		v := []byte(fmt.Sprintf("this is val%d", i))
		kvp := getKVpair(k, v)
		kvpairs = append(kvpairs, kvp)
	}
	stubNode := getStubInternalNode(PAGE_SIZE, kvpairs, []uint64{1, 2, 3, 4})

	t.Run("nodeLookupLE returns the index of first key <= given key", func(t *testing.T) {
		got := nodeLookupLE(stubNode, []byte("this is key3"))
		assert.Equal(t, uint16(3), got, "node lookup less then")
	})

	t.Run("nodeAppendRange copies kvpairs from old to new node upto given index", func(t *testing.T) {
		var newNode BNode = make([]byte, PAGE_SIZE)
		idx := 2
		newNode.setHeader(BNODE_NODE, stubNode.nkeys())

		nodeAppendRange(newNode, stubNode, 0, 0, uint16(idx))

		// check header
		assert.Equal(t, stubNode.btype(), newNode.btype(), "comparing node type")
		assert.Equal(t, stubNode.nkeys(), newNode.nkeys(), "comparing number of keys")

		// check pointers
		for i := uint16(0); i < uint16(idx); i++ {
			assert.Equal(t, stubNode.getptr(i), newNode.getptr(i), "comparing pointers")
		}

		// check offsets
		for i := uint16(0); i < uint16(idx); i++ {
			assert.Equal(t, stubNode.getOffset(i), newNode.getOffset(i), "comparing offsets")
		}

		// check kvpairs
		for i := uint16(0); i < uint16(idx); i++ {
			assert.Equal(t, stubNode.getKey(i), newNode.getKey(i), "comparing keys")
			assert.Equal(t, stubNode.getVal(i), newNode.getVal(i), "comparing vals")
		}
	})

	t.Run("nodeAppendKV appends a new kv pair to the node", func(t *testing.T) {
		var newNode BNode = make([]byte, PAGE_SIZE)
		newNode.setHeader(BNODE_LEAF, 3)
		idx := uint16(2)
		key := []byte("this is new key")
		val := []byte("this is new value")

		nodeAppendKV(newNode, idx, 0, key, val)

		assert.Equal(t, key, newNode.getKey(idx), "checking appended key")
		assert.Equal(t, val, newNode.getVal(idx), "checking appended val")
	})

	t.Run("nodeReplaceKidN replaces one kv with multiple kv's", func(t *testing.T) {
		var newNode BNode = make([]byte, PAGE_SIZE)
		cache := newC()
		tree := cache.tree
		idx := uint16(2)

		stubNode1 := make([]byte, PAGE_SIZE)
		stubNode2 := make([]byte, PAGE_SIZE)
		stubNode3 := make([]byte, PAGE_SIZE)
		copy(stubNode1, stubNode)
		copy(stubNode2, stubNode)
		copy(stubNode3, stubNode)

		nodeReplaceKidN(&tree, newNode, stubNode, idx, stubNode, stubNode1, stubNode2, stubNode3)

		// check header
		assert.Equal(t, uint16(BNODE_NODE), newNode.btype(), "comparing header")
		// here 3 is added because in the above nodeReplaceKidN function call we are passing
		// 4 stubNodes
		assert.Equal(t, stubNode.nkeys()+3, newNode.nkeys(), "comparing keys")

		// check if the kvpair at idx is replaced by pointer to the new kids
		for i := idx; i < idx+4; i++ {
			assert.Contains(t, cache.pages, newNode.getptr(i))
		}

		compareNodes(t, newNode, stubNode, 0, 0, idx)
		compareNodes(t, newNode, stubNode, idx+4, idx+1, stubNode.nkeys()-(idx+1))
	})

	t.Run("nodeReplace2Kid replaces pointers to 2 nodes with a merged pointers", func(t *testing.T) {
		// create new stub node
		kvpairs := make([][]byte, 0)
		for i := 0; i < 4; i++ {
			k := []byte(fmt.Sprintf("this is key%d", i))
			v := []byte(fmt.Sprintf("this is val%d", i))
			kvp := getKVpair(k, v)
			kvpairs = append(kvpairs, kvp)
		}
		idx := uint16(2)
		mergedPtr := uint64(2525)
		old := getStubInternalNode(PAGE_SIZE, kvpairs, []uint64{1, 2, 3, 4})
		new := BNode(make([]byte, PAGE_SIZE))
		key := []byte("this is new key")
		nodeReplace2Kid(new, old, idx, mergedPtr, key)

		// check header
		assert.Equal(t, old.btype(), new.btype(), "checking header")
		assert.Equal(t, old.nkeys()-1, new.nkeys(), "checking header")

		// compare values at the given index
		keyAtIdx := new.getKey(idx)
		assert.Equal(t, key, keyAtIdx, "comparing key at given index")

		compareNodes(t, new, old, 0, 0, idx)
		compareNodes(t, new, old, idx+1, idx+2, new.nkeys()-(idx+1))
	})
}

func TestNodeLookups(t *testing.T) {
	cache := newC()
	tree := cache.tree

	size := 20
	count := 30
	keys, vals := getOrderedKeyValuePairs(size, count)
	for i := 0; i < count; i++ {
		tree.Insert(keys[i], vals[i])
	}

	t.Run("nodeLookupCMP (LE) returns index of key, which is less then or equal to the given key", func(t *testing.T) {
		k, v := getStubKeyValue(size, 15)
		node := BNode(tree.Get(tree.Root))
		idx := nodeLookupCmp(node, k, CMP_LE)
		gotval := node.getVal(idx)
		assert.Equal(t, v, gotval, "comparing values")
	})

	t.Run("nodeLookupCMP (LT) returns index of key, which is less then the given key", func(t *testing.T) {
		k, _ := getStubKeyValue(size, 15)
		_, wantval := getStubKeyValue(size, 14)
		node := BNode(tree.Get(tree.Root))
		idx := nodeLookupCmp(node, k, CMP_LT)
		gotval := node.getVal(idx)
		assert.Equal(t, wantval, gotval, "comparing values")
	})

	t.Run("nodeLookupCMP (GT) returns index of key, which is greater then the given key", func(t *testing.T) {
		k, _ := getStubKeyValue(size, 15)
		_, wantval := getStubKeyValue(size, 16)
		node := BNode(tree.Get(tree.Root))
		idx := nodeLookupCmp(node, k, CMP_GT)
		gotval := node.getVal(idx)
		assert.Equal(t, wantval, gotval, "comparing values")
	})
}

func TestNodeUpdate(t *testing.T) {
	t.Run("nodeInsertInNode inserts kv pair at the given index", func(t *testing.T) {
		kvpairs := make([][]byte, 0)
		for i := 0; i < 4; i++ {
			k := []byte(fmt.Sprintf("this is key%d", i))
			v := []byte(fmt.Sprintf("this is val%d", i))
			kvp := getKVpair(k, v)
			kvpairs = append(kvpairs, kvp)
		}
		idx := uint16(2)
		stubNode1 := getStubInternalNode(PAGE_SIZE, kvpairs, []uint64{1, 2, 3, 4})
		stubNode2 := getStubInternalNode(PAGE_SIZE, kvpairs, []uint64{1, 2, 3, 4})
		key := []byte("this is new key")
		val := []byte("this is new val")
		nodeInsertInNode(stubNode1, idx, 0, key, val)

		// check header
		assert.Equal(t, uint16(BNODE_NODE), stubNode1.btype(), "checking node type")
		assert.Equal(t, uint16(5), stubNode1.nkeys(), "checking number of keys in node")

		// compare nodes up to the given index
		compareNodes(t, stubNode1, stubNode2, 0, 0, idx)

		// compare nodes at the given idx
		assert.Equal(t, uint64(0), stubNode1.getptr(idx))
		newkvlen := KVHEADER + len(key) + len(val)
		assert.Equal(t, uint16(newkvlen), stubNode1.getOffset(idx+1)-stubNode1.getOffset(idx))

		keyAtIdx := stubNode1.getKey(idx)
		valAtIdx := stubNode1.getVal(idx)
		assert.Equal(t, key, keyAtIdx, "comparing keys at given idx")
		assert.Equal(t, val, valAtIdx, "comparing values at given idx")

		// compare node from index to the rest of the index
		compareNodes(t, stubNode1, stubNode2, idx+1, idx, stubNode2.nkeys()-idx)
	})

	t.Run("leafInsert inserts given kv pair at the given index", func(t *testing.T) {
		kvpairs := make([][]byte, 0)
		for i := 0; i < 4; i++ {
			k := []byte(fmt.Sprintf("this is key%d", i))
			v := []byte(fmt.Sprintf("this is val%d", i))
			kvp := getKVpair(k, v)
			kvpairs = append(kvpairs, kvp)
		}
		idx := uint16(2)
		stubNode1 := BNode(make([]byte, PAGE_SIZE))
		stubNode2 := getStubLeafNode(PAGE_SIZE, kvpairs)
		key := []byte("this is new key")
		val := []byte("this is new val")
		leafInsert(stubNode1, stubNode2, idx, key, val)

		// check header
		assert.Equal(t, uint16(BNODE_LEAF), stubNode1.btype(), "checking node type")
		assert.Equal(t, uint16(5), stubNode1.nkeys(), "checking number of keys in node")

		PrintNode(stubNode1)
		fmt.Println()
		PrintNode(stubNode2)

		compareNodes(t, stubNode1, stubNode2, 0, 0, idx)

		// compare values at given index
		newkvlen := KVHEADER + len(key) + len(val)
		assert.Equal(t, uint16(newkvlen), stubNode1.getOffset(idx+1)-stubNode1.getOffset(idx))
		keyAtIdx := stubNode1.getKey(idx)
		valAtIdx := stubNode1.getVal(idx)
		assert.Equal(t, key, keyAtIdx, "comparing keys at given idx")
		assert.Equal(t, val, valAtIdx, "comparing values at given idx")

		compareNodes(t, stubNode1, stubNode2, idx+1, idx, stubNode2.nkeys()-idx)
	})

	t.Run("leafDelete deletes given kv pair at the given index", func(t *testing.T) {
		kvpairs := make([][]byte, 0)
		for i := 0; i < 4; i++ {
			k := []byte(fmt.Sprintf("this is key%d", i))
			v := []byte(fmt.Sprintf("this is val%d", i))
			kvp := getKVpair(k, v)
			kvpairs = append(kvpairs, kvp)
		}
		old := getStubLeafNode(PAGE_SIZE, kvpairs)

		new := BNode(make([]byte, PAGE_SIZE))
		idx := uint16(2)

		leafDelete(new, old, idx)

		// check header
		assert.Equal(t, uint16(BNODE_LEAF), new.btype(), "checking node type")
		assert.Equal(t, old.nkeys()-1, new.nkeys(), "checking number of keys in node")

		compareNodes(t, new, old, 0, 0, idx)
		compareNodes(t, new, old, idx, idx+1, new.nkeys()-idx)
	})

	t.Run("leafUpdate updates the value of the given key", func(t *testing.T) {
		//  creating a stub node
		kvpairs := make([][]byte, 0)
		for i := 0; i < 4; i++ {
			k := []byte(fmt.Sprintf("this is key%d", i))
			v := []byte(fmt.Sprintf("this is val%d", i))
			kvp := getKVpair(k, v)
			kvpairs = append(kvpairs, kvp)
		}
		old := getStubInternalNode(PAGE_SIZE, kvpairs, []uint64{1, 2, 3, 4})

		new := BNode(make([]byte, PAGE_SIZE))

		idx := uint16(2)
		// set header of new node
		new.setHeader(old.btype(), old.nkeys())
		leafUpdate(new, old, idx, []byte("this is new key"), []byte("this is updated value"))

		// check header
		assert.Equal(t, old.btype(), new.btype(), "comparing headers")
		assert.Equal(t, old.nkeys(), new.nkeys(), "comparing number of keys")

		// compare nodes upto index
		compareNodes(t, new, old, 0, 0, idx)
		// compare updated values at the given index
		assert.Equal(t, []byte("this is new key"), new.getKey(idx))
		assert.Equal(t, []byte("this is updated value"), new.getVal(idx))
		// compare rest of the nodes
		compareNodes(t, new, old, idx+1, idx+1, old.nkeys()-idx-1)
	})
}

func TestNodeSplit(t *testing.T) {
	t.Run("nodeSplit2 splits the node of size 2 * PAGE_SIZE into two nodes", func(t *testing.T) {
		// create a new node whose size spans two pages
		kvpairs := make([][]byte, 0)
		size := HEADER * 2
		count := 0
		for size < 1.5*PAGE_SIZE {
			key := make([]byte, 250)
			copy(key, []byte(fmt.Sprintf("this is key%d", count)))

			val := make([]byte, 250)
			copy(val, []byte(fmt.Sprintf("this is val%d", count)))

			size += len(key) + len(val) + KVHEADER + POINTER + OFFSET
			newkv := getKVpair(key, val)
			kvpairs = append(kvpairs, newkv)
			count += 1
		}
		// remove the last kv pair
		kvpairs = kvpairs[:len(kvpairs)-1]
		old := getStubLeafNode(2*PAGE_SIZE, kvpairs)
		left := BNode(make([]byte, 2*PAGE_SIZE))
		right := BNode(make([]byte, PAGE_SIZE))

		midIdx := nodeSplit2(left, right, old)

		compareNodes(t, left, old, 0, 0, midIdx)
		compareNodes(t, right, old, 0, midIdx, old.nkeys()-midIdx)
		assert.Equal(t, left.nkeys()+right.nkeys(), old.nkeys())
	})

	t.Run("nodeSplit2 splits the node of size 3 * PAGE_SIZE into two nodes", func(t *testing.T) {
		// create a new node whose size spans two pages
		// it is assumed that the node size is not greater then 2.5*PAGE_SIZE
		kvpairs := make([][]byte, 0)
		size := HEADER * 2
		count := 0
		for size < 2.5*PAGE_SIZE {
			key := make([]byte, 250)
			copy(key, []byte(fmt.Sprintf("this is key%d", count)))

			val := make([]byte, 250)
			copy(val, []byte(fmt.Sprintf("this is val%d", count)))

			size += len(key) + len(val) + KVHEADER + POINTER + OFFSET
			newkv := getKVpair(key, val)

			kvpairs = append(kvpairs, newkv)
			count += 1
		}
		// remove the last kv pair
		kvpairs = kvpairs[:len(kvpairs)-1]
		old := getStubLeafNode(3*PAGE_SIZE, kvpairs)
		left := BNode(make([]byte, 2*PAGE_SIZE))
		right := BNode(make([]byte, PAGE_SIZE))

		midIdx := nodeSplit2(left, right, old)

		compareNodes(t, left, old, 0, 0, midIdx)
		compareNodes(t, right, old, 0, midIdx, old.nkeys()-midIdx)
		assert.Equal(t, left.nkeys()+right.nkeys(), old.nkeys())
	})

	t.Run("nodeSplit3 splits single node of size greater then 2*PAGE_SIZE into three nodes", func(t *testing.T) {
		// create a new node whose size spans two pages
		// it is assumed that the node size is not greater then 2.5*PAGE_SIZE
		kvpairs := make([][]byte, 0)
		size := HEADER * 2
		count := 0
		for size < 2.5*PAGE_SIZE {
			key := make([]byte, 250)
			copy(key, []byte(fmt.Sprintf("this is key%d", count)))

			val := make([]byte, 250)
			copy(val, []byte(fmt.Sprintf("this is val%d", count)))

			size += len(key) + len(val) + KVHEADER + POINTER + OFFSET
			newkv := getKVpair(key, val)

			kvpairs = append(kvpairs, newkv)
			count += 1
		}
		// remove the last kv pair
		kvpairs = kvpairs[:len(kvpairs)-1]
		old := getStubLeafNode(3*PAGE_SIZE, kvpairs)
		Nnodes, Nodes, mid1, mid2 := nodeSplit3(old)
		assert.Equal(t, uint16(3), Nnodes)

		left := Nodes[0]
		mid := Nodes[1]
		right := Nodes[2]
		compareNodes(t, left, old, 0, 0, mid1)
		compareNodes(t, mid, old, 0, mid1, mid2-mid1)
		compareNodes(t, right, old, 0, mid2, old.nkeys()-mid2)
	})

	t.Run("nodeSplit3 splits single node of size less then 2*PAGE_SIZE into two nodes", func(t *testing.T) {
		// create a new node whose size spans two pages
		// it is assumed that the node size is not greater then 2.5*PAGE_SIZE
		kvpairs := make([][]byte, 0)
		size := HEADER * 2
		count := 0
		for size < int(math.Round(1.8*PAGE_SIZE)) {
			key := make([]byte, 250)
			copy(key, []byte(fmt.Sprintf("this is key%d", count)))

			val := make([]byte, 250)
			copy(val, []byte(fmt.Sprintf("this is val%d", count)))

			size += len(key) + len(val) + KVHEADER + POINTER + OFFSET
			newkv := getKVpair(key, val)

			kvpairs = append(kvpairs, newkv)
			count += 1
		}
		// remove the last kv pair
		kvpairs = kvpairs[:len(kvpairs)-1]
		old := getStubLeafNode(2*PAGE_SIZE, kvpairs)
		Nnodes, Nodes, _, mid2 := nodeSplit3(old)
		assert.Equal(t, uint16(2), Nnodes)

		left := Nodes[0]
		right := Nodes[1]
		compareNodes(t, left, old, 0, 0, mid2)
		compareNodes(t, right, old, 0, mid2, old.nkeys()-mid2)
	})
}

func TestInsert(t *testing.T) {
	t.Run("Insert insert's new kv pair into tree", func(t *testing.T) {
		// insert 100 kvpairs
		cache := newC()
		tree := cache.tree

		for i := 0; i < 100; i++ {
			key, val := getStubKeyValue(500, i)
			tree.Insert(key, val)
			PrintTree(tree, tree.Get(tree.Root))
		}

		for i := 0; i < 100; i++ {
			key, val := getStubKeyValue(500, i)
			got, err := tree.GetVal(key)
			require.NoError(t, err)
			assert.Equal(t, val, got)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Run("Delete the key from the Btree", func(t *testing.T) {
		// insert 100 kvpairs
		cache := newC()
		tree := cache.tree

		for i := 0; i < 20; i++ {
			key, val := getStubKeyValue(500, i)
			tree.Insert(key, val)
		}

		for i := 0; i < 20; i++ {
			key, _ := getStubKeyValue(500, i)
			assert.True(t, tree.Delete(key))
		}
	})
}

func TestIterator(t *testing.T) {
	cache := newC()
	tree := cache.tree

	// create 100 kv pairs
	keyValSize := 200 // size of key = 200bytes and val = 200bytes
	keys := make([][]byte, 0)
	vals := make([][]byte, 0)
	for i := 0; i < 100; i++ {
		k, v := getStubKeyValue(keyValSize, i)
		keys = append(keys, k)
		vals = append(vals, v)
	}
	// insert 100 kv pairs
	for i := 0; i < 100; i++ {
		tree.Insert(keys[i], vals[i])
	}

	getkey := make([]byte, keyValSize)
	binary.BigEndian.PutUint64(getkey, 50)
	iter := tree.SeekLE(getkey)

	t.Run("Deref returns seeked kv pair", func(t *testing.T) {
		gotkey, gotval := iter.Deref()
		wantkey, wantval := getStubKeyValue(keyValSize, 50)
		assert.Equal(t, wantkey, gotkey)
		assert.Equal(t, wantval, gotval)
	})

	t.Run("Next returns the next kv in order", func(t *testing.T) {
		// values are stored in the decereasing order
		// so the next value give key less then current value
		for i := 50; i >= 0; i-- {
			gotkey, gotval := iter.Deref()
			wantkey, wantval := getStubKeyValue(keyValSize, i)
			assert.Equal(t, wantkey, gotkey)
			assert.Equal(t, wantval, gotval)
			iter.Prev()
		}
		// check if valid for last returns false
		assert.False(t, iter.Valid(), "validator is returning true")

		// iterate back to 99 from 0
		iter.Next()
		for i := 0; i < 100; i++ {
			gotkey, gotval := iter.Deref()
			wantkey, wantval := getStubKeyValue(keyValSize, i)
			assert.Equal(t, wantkey, gotkey)
			assert.Equal(t, wantval, gotval)
			iter.Next()
		}
	})
}

func getOrderedKeyValuePairs(size, count int) ([][]byte, [][]byte) {
	// size of key = 200bytes and val = 200bytes
	keys := make([][]byte, 0)
	vals := make([][]byte, 0)
	for i := 0; i < count; i++ {
		k, v := getStubKeyValue(size, i)
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals
}

func getStubKeyValue(size, id int) ([]byte, []byte) {
	if size == 0 {
		key := make([]byte, size)
		binary.BigEndian.PutUint64(key, uint64(id))
		val := []byte(fmt.Sprintf("this is val%d", id))
		return key, val
	}

	key := make([]byte, size)
	binary.BigEndian.PutUint64(key, uint64(id))
	val := make([]byte, size)
	copy(val, []byte(fmt.Sprintf("this is val:%d", id)))

	return key, val
}

func getStubInternalNode(size int,
	kvparis [][]byte, pointers []uint64) BNode {

	var stubNode BNode = make([]byte, size)
	stubNode.setHeader(BNODE_NODE, uint16(len(kvparis)))

	// set pointers if it's internal node
	for i := uint16(0); i < stubNode.nkeys(); i++ {
		stubNode.setptr(i, pointers[i])
	}

	// set offsets
	acc := uint16(0)
	for i := uint16(1); i <= stubNode.nkeys(); i++ {
		acc += uint16(len(kvparis[i-1]))
		stubNode.setOffset(i, acc)
	}

	// set kv pairs
	for i := uint16(0); i < stubNode.nkeys(); i++ {
		spos := stubNode.kvPos(i)
		copy(stubNode[spos:], kvparis[i])
	}
	return stubNode
}

func getStubLeafNode(size int, kvparis [][]byte) BNode {

	var stubNode BNode = make([]byte, size)
	stubNode.setHeader(BNODE_LEAF, uint16(len(kvparis)))

	// set pointers if it's internal node
	for i := uint16(0); i < stubNode.nkeys(); i++ {
		stubNode.setptr(i, 0)
	}

	// set offsets
	acc := uint16(0)
	for i := uint16(1); i <= stubNode.nkeys(); i++ {
		acc += uint16(len(kvparis[i-1]))
		stubNode.setOffset(i, acc)
	}

	// set kv pairs
	for i := uint16(0); i < stubNode.nkeys(); i++ {
		spos := stubNode.kvPos(i)
		copy(stubNode[spos:], kvparis[i])
	}
	return stubNode
}

// compareNodes compares two node for their equality
// here len is the length more specifically number of values
// to be compared from the start of the nodes
func compareNodes(t testing.TB, new BNode, old BNode,
	newStart uint16, oldStart uint16, len uint16) {
	t.Helper()

	// check pointers
	n, o := newStart, oldStart
	for i := uint16(0); i < len; i++ {
		assert.Equal(t, old.getptr(o+i), new.getptr(n+i), "comparing pointers")
	}

	// check kvpairs
	for i := uint16(0); i < len; i++ {
		sn1key := old.getKey(o + i)
		sn2key := new.getKey(n + i)
		assert.Equal(t, sn2key, sn1key, "comparing keys")

		sn1val := old.getVal(o + i)
		sn2val := new.getVal(n + i)
		assert.Equal(t, sn2val, sn1val, "comparing vals")
	}

	// check offsets
	for i := uint16(1); i <= len; i++ {
		reloffset1 := old.getOffset(i+o) - old.getOffset(i-1+o)
		reloffset2 := new.getOffset(i+n) - new.getOffset(i-1+n)

		assert.Equal(t, reloffset2, reloffset1, "comparing offsets")
	}
}
