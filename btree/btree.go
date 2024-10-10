package btree

import (
	"bytes"
	"encoding/binary"
	"log"
	"math"
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

// format used to store bytes is 	LITTLE ENDIAN

type BNode []byte

type BTree struct {
	root uint64              // pointer to page
	get  func(uint64) []byte // get the page
	new  func([]byte) uint64 // allocate a new page
	del  func(uint64)        // allocate87 a page
}

// checks if max node size can fit into a page
func init() {
	// size of largest possible node
	node1max := HEADER + POINTER + OFFSET + (KVHEADER + MAXKEYLEN + MAXVALLEN)

	// check if the max node fits in a page
	if node1max > PAGE_SIZE {
		log.Fatalln("size of node exceeds the size of page")
	}
}

// functions to decode node formats
// btype returns the type of BNode
func (b BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(b[0:2])
}

// nkeys returns number of keys in the node
func (b BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(b[2:4])
}

// setHeader sets the header(btype, nkeys) to given values
func (b BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(b[0:2], btype)
	binary.LittleEndian.PutUint16(b[2:4], nkeys)
}

// handle child pointers

// getptr returns pointer to node at a given index
func (b BNode) getptr(idx uint16) uint64 {
	if idx >= b.nkeys() {
		log.Fatalf("out of bounds, index: %d, nkeys: %d", idx, b.nkeys())
	}
	sbyte := HEADER + (idx * 8)
	return binary.LittleEndian.Uint64(b[sbyte:])
}

// setptr set the poiter at a given index to the specified value
func (b BNode) setptr(idx uint16, ptr uint64) {
	if idx >= b.nkeys() {
		log.Fatalf("set pointer out of bounds , index: %d, nkeys: %d", idx, b.nkeys())
	}
	sbyte := HEADER + (idx * 8)
	binary.LittleEndian.PutUint64(b[sbyte:], ptr)
}

// handle offset
// offset stores the distance between first KV pair to given KV pair
// offset of 1st KV is 0
// offset of 2nd KV is the value pointed by 1st offset
// because each offset points to the end of each KV pair
// 1st offset points to the end of 1st KV pair
// 2nd offset points to the end of 2nd KV pair
// and so on

// offsets are stored back to back as an array
// offset are structured in such a way that offset[idx] gives
// offest of KV-Pair [idx+1]
// offsetPos function returns the position of the offset with in the node
func (b BNode) offsetPos(idx uint16) uint16 {
	if idx > b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}

	sbyte := HEADER + (b.nkeys() * POINTER) + ((idx - 1) * OFFSET)
	return sbyte
}

// getOffset returns offset of a kv parir relative to the 1st kv pair
func (b BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(b[b.offsetPos(idx):])
}

func (b BNode) setOffset(idx uint16, val uint16) {
	if idx > b.nkeys() {
		log.Fatalf("out of bounds setting offset, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	sbyte := b.offsetPos(idx)
	binary.LittleEndian.PutUint16(b[sbyte:], val)
}

// kvPos returns starting index of the kv pair in node
func (b BNode) kvPos(idx uint16) uint16 {
	if idx > b.nkeys() {
		log.Fatalf("out of bounds kvpos, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	offset := b.getOffset(idx)
	sbyte := offset + HEADER + (b.nkeys() * POINTER) + (b.nkeys() * OFFSET)
	//  	 ^offset relative to the first kv pair
	return sbyte
}

func (b BNode) getKey(idx uint16) []byte {
	if idx >= b.nkeys() {
		log.Fatalf("out of bounds, idx: %d, nkeys: %d", idx, b.nkeys())
	}
	kvpos := b.kvPos(idx)
	klen := binary.LittleEndian.Uint16(b[kvpos:])
	kpos := KVHEADER + kvpos
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
	vpos := kvpos + KVHEADER + klen
	return b[vpos:][:vlen]
}

func (b BNode) nbytes() uint16 {
	return b.kvPos(b.nkeys())
}

// returns the first key node whose range intersects the key. (kid[i] <= key)
// LE = less then or equal to operator
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	// first key in the node is always less then or equal to the search key
	// because first key is the smallest in a B+Tree Node
	for i := uint16(1); i < nkeys; i++ {
		k := node.getKey(i)
		cmp := bytes.Compare(key, k)
		if cmp <= 0 {
			found = i
		}
		if cmp > 0 {
			break
		}
	}
	return found
}

// leaf insert function inserts key and value at a given index
// it will just update the new BNode with all the values of the old Node
// along with a new kv pair inserted at a given location
func leafInsert(new BNode, old BNode, idx uint16, key, val []byte) {
	// increment number of key count in header
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// leafUpdate updates the value of a key at a given index
func leafUpdate(new BNode, old BNode, idx uint16, key, val []byte) {
	// copy upto index
	nodeAppendRange(new, old, 0, 0, idx)

	// update the kv pair at index
	nodeAppendKV(new, idx, 0, key, val)

	// copy from index + 1 till end
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// nodeAppendRange copies kv pairs from old BNode to new BNode
// from srcOld(included) till srcOld + n(not included) will be copied to
// dstNew(included) till dstNew + n(not included)
// dstNew starting index in new BNode
// srcOld starting index in old BNode
// n is number of kv pairs to be copied from old to new BNode
// starting from the indexes
func nodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	// check if the number of node are within the range
	if dstNew+n > new.nkeys() {
		log.Fatalf("out of bounds new node, idx: %d, nkeys: %d", dstNew+n, new.nkeys())
	}
	if srcOld+n > old.nkeys() {
		log.Fatalf("out of bounds old node, idx: %d, nkeys: %d", srcOld+n, old.nkeys())
	}

	if n == 0 {
		return
	}
	// copy pointer from src to dst
	// src, starts from index srcOld
	// dst, starts from index dstNew
	for i := uint16(0); i < n; i++ {
		new.setptr(dstNew+i, uint64(old.getptr(srcOld+i)))
	}

	// set offsets for new pointer
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ {
		relOffset := old.getOffset(srcOld+i) - srcBegin
		offset := dstBegin + relOffset
		new.setOffset(dstNew+i, offset)
	}

	oldOffset := HEADER + POINTER*old.nkeys() + OFFSET*old.nkeys()
	begin := old.getOffset(srcOld) + oldOffset
	end := old.getOffset(srcOld+n) + oldOffset
	newBegin := new.getOffset(dstNew) + HEADER + POINTER*new.nkeys() + OFFSET*new.nkeys()
	copy(new[newBegin:], old[begin:end])
}

// nodeInsertDynamic inserts new key value pair at the given index
// along with modifying the pointers, offsets and header
func nodeInsertDynamic(new BNode, idx uint16, ptr uint64, key, val []byte) {

	// create a temp node to store data
	var temp BNode
	size := math.Ceil(float64(new.nbytes() / PAGE_SIZE))
	temp = make([]byte, PAGE_SIZE*int(size))

	// check if kv-pairs fits in the page
	newSize := new.nbytes() + uint16(POINTER+OFFSET+KVHEADER+len(key)+len(val))
	if newSize > uint16(PAGE_SIZE*int(size)) {
		log.Fatalln("node is full")
	}

	copy(temp, new)

	// set header
	new.setHeader(temp.btype(), temp.nkeys()+1)

	// copy pointers upto index
	for i := uint16(0); i < idx; i++ {
		ptr := temp.getptr(i)
		new.setptr(i, ptr)
	}
	new.setptr(idx, ptr)
	for i := uint16(idx); i < temp.nkeys(); i++ {
		ptr := temp.getptr(i)
		new.setptr(i+1, ptr)
	}

	// copy offsets into the new node
	// kvposPtr := HEADER + new.nkeys()*POINTER + new.nkeys()*OFFSET
	// spostemp := HEADER + temp.nkeys()*POINTER
	// for i := uint16(0); i < idx; i++ {
	// 	// length of kv pair
	// 	reloffset := temp.getOffset(i) - temp.getOffset(i-1)
	// }
}

// nodeAppendKV adds a new kv pair to the node at a given index,
// it does not incerease the nkeys of new node by one
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// set ptr
	new.setptr(idx, ptr)

	// set kv
	pos := new.kvPos(idx)
	// set kv headers
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// set kv data
	copy(new[pos+KVHEADER:], key)
	copy(new[pos+KVHEADER+uint16(len(key)):], val)

	// set offset off next key
	new.setOffset(idx+1, new.getOffset(idx)+KVHEADER+uint16(len(key)+len(val)))
}

// nodeReplaceKidN copies the keys that are in old internal node to
// new internal node along with appending keys to the new child nodes
// at a given index
// this function should be used with internodes
// here the value will be nil as the values will be stored in only leaf nodes
// here the while creating a new node can lead to spliting
// so we use multiple nodes(kids)
// it is very similar to insert a new element in an array at a given index
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	// number of child nodes
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)

	// append kv's from 0 to given index
	nodeAppendRange(new, old, 0, 0, idx)

	// append new kv or multiple keys at give index
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	// append kv's from index + 1 to all of the indexes
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// nodeReplace2Kid replaces pointers to 2 nodes with a merged pointers
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16,
	mergedPtr uint64, key []byte,
) {
	new.setHeader(old.btype(), old.nkeys()-1)

	// copy kv's upto index
	nodeAppendRange(new, old, 0, 0, idx)

	// add pointer to new merged node page
	nodeAppendKV(new, idx, mergedPtr, key, nil)

	// copy remaining kv's
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
	//                               ^skip the merged node
}

// split a oversized node into 2 so that the 2nd node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// split the node into half
	if old.nbytes() <= PAGE_SIZE*2 {
		idx := uint16(0)
		for old.kvPos(idx) <= PAGE_SIZE {
			idx++
		}
		nodeAppendRange(left, old, 0, 0, idx)
		nodeAppendRange(right, old, 0, idx, old.nkeys()-idx)
	} else {
		idx := uint16(0)
		for old.kvPos(idx) <= 2*PAGE_SIZE {
			idx++
		}
		nodeAppendRange(left, old, 0, 0, idx)
		nodeAppendRange(right, old, 0, idx, old.nkeys()-idx)
	}
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	// check if the node is small
	if old.nbytes() <= PAGE_SIZE {
		return 1, [3]BNode{old}
	}

	// allocate in-memory pages
	left := BNode(make([]byte, 2*PAGE_SIZE)) // may be split later
	right := BNode(make([]byte, PAGE_SIZE))
	nodeSplit2(left, right, old)

	// if left fits in a page return 2 nodes
	if left.nbytes() <= PAGE_SIZE {
		left = left[:PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// if size of left is more split left node
	leftleft := BNode(make([]byte, PAGE_SIZE))
	middle := BNode(make([]byte, PAGE_SIZE))

	nodeSplit2(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right}
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// BNode size can be more then one page
	new := BNode(make([]byte, 2*PAGE_SIZE))

	// check where to insert
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if bytes.Equal(key, node.getKey(idx)) {
			// if both keys are same, update value
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert a new kv pair into node
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		nodeInsert(tree, new, node, idx, key, val)
	default:
		log.Panicln("invalid node header(bad node)!")
	}
	return new
}

func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	// get child node
	kptr := node.getptr(idx)

	// reccursive call to traverse through the tree
	// and insert into child
	knode := treeInsert(tree, tree.get(kptr), key, val)

	// split the result
	nsplit, split := nodeSplit3(knode)

	// deallocate the child node
	tree.del(kptr)

	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

func (t *BTree) Insert(key, val []byte) {
	// if there is no root node
	if t.root == 0 {
		// create a new node
		new := BNode(make([]byte, PAGE_SIZE))
		new.setHeader(BNODE_NODE, 2)

		// add fake KV pair to hold the constrain of
		// node having more then 2 values
		nodeAppendKV(new, 0, 0, nil, nil)
		nodeAppendKV(new, 1, 0, key, val)

		// assign new node as root of tree
		t.root = t.new(new)
		return
	}

	node := treeInsert(t, t.get(t.root), key, val)
	nsplit, split := nodeSplit3(node)
	t.del(t.root)

	if nsplit > 1 {
		// root node is split
		// create new root node
		newRoot := BNode(make([]byte, PAGE_SIZE))
		newRoot.setHeader(BNODE_NODE, nsplit)
		for i, childNode := range split {
			ptr, key := t.new(childNode), childNode.getKey(0)
			nodeAppendKV(newRoot, uint16(i), ptr, key, nil)
		}
		t.root = t.new(newRoot)
	} else {
		// root node is not split
		t.root = t.new(split[0])
	}
}

// updated refers to the updated BNode
// node refers to the parent node of updated BNode
// idx refers to the index of the updated node in the parent node
// 0 -> no need to merge, -1 -> merge with left sibiling, 1 -> merge with right sibiling
func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	// check if the size of updated node is greater then PAGE_SIZE / 4
	// it acts as a minimum threshold required to merge nodes
	if updated.nbytes() > PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		// get the left sibiling of the updated node
		leftSib := BNode(tree.get(node.getptr(idx - 1)))

		// size of the merged node
		merged := leftSib.nbytes() + updated.nbytes() - HEADER
		if merged <= PAGE_SIZE {
			return -1, leftSib
		}
	}

	// check if right sibiling exists
	if idx+1 < node.nkeys() {
		// get the right sibiling of the updated node
		rightSib := BNode(tree.get(node.getptr(idx + 1)))

		// size of the merged node
		merged := rightSib.nbytes() + updated.nbytes() - HEADER
		if merged <= PAGE_SIZE {
			return 1, rightSib
		}
	}

	return 0, BNode{}
}

// nodeMerge merges left and right node into new node
func nodeMerge(new, left, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())

	// copy left node to new node
	nodeAppendRange(new, left, 0, 0, left.nkeys())

	// append right node to new node
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	// check where to delete
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		// key not found
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{}
		}
		new := BNode(make([]byte, PAGE_SIZE))
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		log.Panicln("invalid node header(bad node)!")
	}
	return BNode{}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// get to the leaf node using ressursion
	kptr := node.getptr(idx)

	// delete the leaf node
	updated := treeDelete(tree, tree.get(kptr), key)
	// node not found
	if len(updated) == 0 {
		return BNode{}
	}

	new := BNode(make([]byte, PAGE_SIZE))
	mergeDir, sibiling := shouldMerge(tree, node, idx, updated)
	switch {
	// merge with left sibiling
	case mergeDir < 0:
		// merge two nodes
		merged := BNode(make([]byte, PAGE_SIZE))
		nodeMerge(merged, sibiling, updated)

		// delete old sibiling node
		tree.del(node.getptr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))

	case mergeDir > 0:
		// merge two nodes
		merged := BNode(make([]byte, PAGE_SIZE))
		nodeMerge(merged, sibiling, updated)

		// delete old sibiling node
		tree.del(node.getptr(idx + 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))

	case mergeDir == 0 && updated.nkeys() == 0:
		// child node is empty
		if node.nkeys() == 1 && idx == 0 {
			new.setHeader(BNODE_NODE, 0)
		}

	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

func leafDelete(new BNode, old BNode, idx uint16) {
	// set header
	new.setHeader(BNODE_LEAF, old.nkeys()-1)

	// copy the kv in old node to new node
	// skip the node at the idx
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}
