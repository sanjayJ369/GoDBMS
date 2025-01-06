package kv

import (
	"bufio"
	"bytes"
	"dbms/btree"
	"dbms/mmap"
	"dbms/util"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
)

const DB_SIG = "Thanks,BuildYourOwnDbFromScratch"

type CommitedTX struct {
	version   uint64
	kvVersion uint64
	writes    map[string]bool
	deletes   map[string]bool
}

type KV struct {
	Path        string       // path to the db file
	rootVersion uint64       // previous version of the current kv version
	version     uint64       // version of the kv store, persisted to the disk
	ongoing     []uint64     // currently ongoing tx (sorted)
	tree        btree.BTree  // Btree
	fp          *os.File     // pointer to file
	mmap        mmap.Mmap    // mmap of the file
	history     []CommitedTX // commit history
	mutex       sync.Mutex   // mutex to handle concurrencys
}

type Iterator interface {
	Deref() ([]byte, []byte)
	Next()
	Prev()
	Valid() bool
}

type CombinedIterator struct {
	kvtx  *KVTX
	store Iterator // iteratoes through the commit values
	tx    Iterator // iteratoes though the newly inserted value (not yet commited)
}

func (c *CombinedIterator) Next() {
	k1, _ := c.store.Deref()
	k2, _ := c.tx.Deref()
	cmp := bytes.Compare(k1, k2)

	if k1 == nil {
		c.tx.Next()
		return
	} else if k2 == nil {
		c.store.Next()
		return
	}

	if cmp <= 0 || !c.tx.Valid() {
		c.store.Next()
	} else {
		c.tx.Next()
	}
}

func (c *CombinedIterator) Prev() {
	c.store.Prev()
	c.tx.Prev()
	if !c.store.Valid() || !c.tx.Valid() {
		return
	}

	k1, _ := c.store.Deref()
	k2, _ := c.tx.Deref()
	cmp := bytes.Compare(k1, k2)

	if k1 == nil {
		c.tx.Next()
	} else if k2 == nil {
		c.store.Next()
	}

	if cmp > 0 {
		c.tx.Next()
	} else {
		c.store.Next()
	}
}

func (c *CombinedIterator) Deref() ([]byte, []byte) {
	k1, _ := c.store.Deref()
	k2, _ := c.tx.Deref()

	if c.kvtx.delSet.Has(k1) {
		k1 = nil
	}
	if c.kvtx.delSet.Has(k2) {
		k2 = nil
	}
	// of one of the tree is empty return other key
	if k1 == nil {
		return c.tx.Deref()
	} else if k2 == nil {
		return c.store.Deref()
	}

	cmp := bytes.Compare(k1, k2)
	// fmt.Println(string(k1), "\t", string(k2), "\t", c.tx.Valid())
	if cmp <= 0 || !c.tx.Valid() {
		return c.store.Deref()
	} else {
		return c.tx.Deref()
	}
}

func (c *CombinedIterator) Valid() bool {
	return c.store.Valid() || c.tx.Valid()
}

// kv interface
type KVInterface interface {
	Abort(tx *KVTX)
	Begin(tx *KVTX)
	Close()
	Commit(tx *KVTX) error
	Open() error
	PrintTree()
}

// kv transaction
type KVTX struct {
	db       *KV
	snapshot btree.BTree // snapshot of current btree
	pending  btree.BTree // to store updates made from transaction

	delSet   *util.Set // to store deleted keys
	writeSet *util.Set // to store modified keys
	readSet  *util.Set // to store read keys

	writes  map[string]bool
	deletes map[string]bool

	kvVersion uint64 // to store on which KV verstion the transaction is based on
	version   uint64 // version of the transaction

	free struct {
		headpage uint64
		headseq  uint64
		tailpage uint64
		tailseq  uint64
		maxseq   uint64
	}
}

func NewKVTX() *KVTX {
	return &KVTX{
		delSet:   util.NewSet(),
		writeSet: util.NewSet(),
		readSet:  util.NewSet(),
		writes:   make(map[string]bool),
		deletes:  make(map[string]bool),
	}
}

func (kv *KV) Begin(tx *KVTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	tx.snapshot.Root = kv.tree.Root
	chunks := kv.mmap.Chunks
	tx.snapshot.Get = func(ptr uint64) []byte {
		return mmap.PageReadFile(ptr, chunks)
	}
	tx.delSet = util.NewSet()
	tx.db = kv

	tx.kvVersion = kv.version
	if len(kv.ongoing) == 0 {
		tx.version = kv.version + 1
	} else {
		// ongoing is sorted slice ordered according to trasaction number
		tx.version = kv.ongoing[len(kv.ongoing)-1] + 1
	}
	kv.ongoing = append(kv.ongoing, tx.version)

	pages := [][]byte(nil)
	tx.pending.New = func(node []byte) uint64 {
		pages = append(pages, node)
		return uint64(len(pages))
	}
	tx.pending.Get = func(ptr uint64) []byte {
		if int(ptr-1) >= len(pages) {
			return nil
		}
		return pages[ptr-1]
	}
	tx.pending.Del = func(u uint64) {}

	tx.free.headpage = kv.mmap.FreeList.HeadPage
	tx.free.headseq = kv.mmap.FreeList.HeadSeq
	tx.free.tailpage = kv.mmap.FreeList.TailPage
	tx.free.tailseq = kv.mmap.FreeList.TailSeq
	tx.free.maxseq = kv.mmap.FreeList.MaxSeq
}

func rollback(kv *KV, tx *KVTX) {
	kv = tx.db
	kv.tree.Root = tx.snapshot.Root

	kv.mmap.FreeList.HeadPage = tx.free.headpage
	kv.mmap.FreeList.HeadSeq = tx.free.headseq
	kv.mmap.FreeList.TailPage = tx.free.tailpage
	kv.mmap.FreeList.TailSeq = tx.free.tailseq
	kv.mmap.FreeList.MaxSeq = tx.free.maxseq

	kv.mmap.NAppend = 0
	kv.mmap.Updates = map[uint64][]byte{}

	util.IntSliceRemove(tx.version, kv.ongoing)
}

func (kv *KV) Abort(tx *KVTX) {
	kv.mutex.Lock()
	defer kv.mutex.Unlock()
	rollback(kv, tx)
}

func (kv *KV) Commit(tx *KVTX) error {
	kv.mutex.Lock()
	ok := detectConflicts(tx, kv)
	if ok {
		kv.mutex.Unlock()
		kv.Abort(tx)
		return fmt.Errorf("conflicts with other trnsactiosn")
	}

	// persist pending changes into the kv store
	for key := range tx.writes {
		val, err := tx.pending.GetVal([]byte(key))
		if err != nil {
			return fmt.Errorf("getting pending writes: %w", err)
		}
		kv.tree.Insert([]byte(key), val)
	}
	for key := range tx.deletes {
		kv.tree.Delete([]byte(key))
	}
	// write pages to disk
	if err := writePages(kv); err != nil {
		rollback(kv, tx)
		kv.mutex.Unlock()
		return fmt.Errorf("writing pages: %w", err)
	}

	// fsync pages so that the
	// data is persisted to the disk
	if err := syncPages(kv); err != nil {
		rollback(kv, tx)
		kv.mutex.Unlock()
		return fmt.Errorf("fsync: %w", err)
	}

	// changeing the root node pointer
	// which makes the entire process atomic
	if err := masterStore(kv); err != nil {
		kv.mutex.Unlock()
		return fmt.Errorf("storing master page: %w", err)
	}
	if err := kv.fp.Sync(); err != nil {
		kv.mutex.Unlock()
		return fmt.Errorf("fsync master page: %w", err)
	}

	kv.mmap.Flushed += kv.mmap.NAppend
	kv.mmap.NAppend = 0
	kv.mmap.Updates = map[uint64][]byte{}
	kv.mmap.FreeList.MaxSeq = kv.mmap.FreeList.HeadSeq

	if kv.version < tx.version {
		kv.version = tx.version
		kv.rootVersion = tx.kvVersion
	}
	// delete transaction from ongoing transactions
	util.IntSliceRemove(tx.version, kv.ongoing)

	// append commit hisotry
	kv.history = append(kv.history, CommitedTX{
		version:   tx.version,
		kvVersion: tx.kvVersion,
		writes:    tx.writes,
		deletes:   tx.deletes,
	})
	kv.mutex.Unlock()
	return nil
}

func detectConflicts(tx *KVTX, kv *KV) bool {
	// trasaction is old..!
	if tx.kvVersion < kv.rootVersion {
		return true
	}
	// check if the transactions are spinned off from the
	// same version of kv store
	if tx.kvVersion == kv.rootVersion {
		// check if there is a conflict between reads and writes of trasaction
		if len(kv.history) > 0 {
			old := kv.history[len(kv.history)-1]
			return checkConflictTXHistory(tx, old)
		}
	}

	return false
}

func checkConflictTXHistory(tx *KVTX, history CommitedTX) bool {
	// check writes
	for i := range history.writes {
		if tx.readSet.Has([]byte(i)) {
			return true
		}
		if tx.writeSet.Has([]byte(i)) {
			return true
		}
		if tx.delSet.Has([]byte(i)) {
			return true
		}
	}

	// check deletes
	for i := range history.deletes {
		if tx.readSet.Has([]byte(i)) {
			return true
		}
		if tx.writeSet.Has([]byte(i)) {
			return true
		}
		if tx.delSet.Has([]byte(i)) {
			return true
		}
	}
	return false
}

func NewKv(loc string) (KVInterface, error) {

	newfile := false
	if _, err := os.Stat(loc); errors.Is(err, os.ErrNotExist) {
		newfile = true
	}

	fp, err := os.OpenFile(loc, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	var k *KV
	if newfile {
		k = &KV{
			Path: loc,
			tree: btree.BTree{},
			fp:   fp,
			mmap: mmap.Mmap{
				Fp:      fp,
				Flushed: 1, // masterpage
				Updates: map[uint64][]byte{},
			},
		}
	} else {
		k = &KV{
			Path: loc,
		}
		err := k.Open()
		if err != nil {
			return nil, fmt.Errorf("opening kv Store: %w", err)
		}
	}

	// initalize btree functions
	k.tree.Get = k.mmap.PageGet
	k.tree.Del = k.mmap.PageDel
	k.tree.New = k.mmap.PageAlloc

	// initalize freelist functions
	k.mmap.FreeList.Get = k.mmap.PageGet
	k.mmap.FreeList.New = k.mmap.PageNew
	k.mmap.FreeList.Set = k.mmap.PageWrite

	return k, nil
}

// used only for debugging
func (k *KV) PrintTree() {
	btree.PrintTree(k.tree, k.tree.Get(k.tree.Root))
}

func (k *KV) Open() error {
	// create/open the file
	fp, err := os.OpenFile(k.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("opening file(%s): %w", k.Path, err)
	}
	k.fp = fp

	// inital mapping of the file
	sz, chunk, err := mmap.MmapInit(k.Path)
	if err != nil {
		return fmt.Errorf("initalizing mmap: %w", err)
	}

	k.mmap = mmap.Mmap{
		Fp:      fp,
		Total:   len(chunk),
		Chunks:  [][]byte{chunk},
		File:    sz,
		Updates: map[uint64][]byte{},
	}

	err = masterLoad(k)
	if err != nil {
		goto fail
	}

	return nil

fail:
	k.Close()
	return fmt.Errorf("masterLoad: %w", err)
}

func (k *KV) Close() {
	k.mmap.Close()
	k.fp.Close()
}

func (k *KVTX) Seek(key []byte) Iterator {
	iter := &CombinedIterator{
		kvtx:  k,
		store: k.snapshot.SeekLE(key),
		tx:    k.pending.SeekLE(key),
	}
	return iter
}

func (k *KVTX) Get(key []byte) ([]byte, error) {
	// check if the key has been delted
	if k.delSet.Has(key) {
		return nil, fmt.Errorf("getting deleted key")
	}
	k.readSet.Set(key)

	val, err := k.pending.GetVal(key)
	if err == nil {
		return val, nil
	}
	return k.snapshot.GetVal(key)
}

func (k *KVTX) Set(key []byte, val []byte) {
	if k.delSet.Has(key) {
		log.Println("setting deleted key")
		return
	}
	k.writeSet.Set(key)
	k.writes[string(key)] = true
	k.pending.Insert(key, val)
}

func (k *KVTX) Del(key []byte) bool {
	if k.delSet.Has(key) {
		log.Println("deleting non exisiting key")
		return false
	}
	k.delSet.Set(key)
	k.deletes[string(key)] = true
	if k.writeSet.Has(key) {
		return k.pending.Delete(key)
	}
	return true
}

func flushpages(k *KV) error {
	err := writePages(k)
	if err != nil {
		return fmt.Errorf("writing pages to disk: %w", err)
	}
	return syncPages(k)
}

// writePages extends the file
// and extends the mmap and copies the temp
// page into the disk
func writePages(k *KV) error {
	npages := k.mmap.Flushed + k.mmap.NAppend

	err := k.mmap.ExtendFile(int(npages))
	if err != nil {
		return fmt.Errorf("extending file: %w", err)
	}

	err = k.mmap.ExtendMmap(int(npages))
	if err != nil {
		return fmt.Errorf("extending mmap: %w", err)
	}

	// copy the temp pages to the disk
	for ptr, page := range k.mmap.Updates {
		dskpage := k.mmap.PageReadFile(ptr)
		copy(dskpage, page)
		delete(k.mmap.Updates, ptr)
	}

	return nil
}

// syncPages flushes the data to the disk
// and them updates the master page
func syncPages(k *KV) error {
	// flush the data to the disk
	err := k.fp.Sync()
	if err != nil {
		return fmt.Errorf("flushing the data to disk: %w", err)
	}

	// update Flushed and Temp
	k.mmap.Flushed += k.mmap.NAppend
	k.mmap.NAppend = 0

	err = masterStore(k)
	if err != nil {
		return fmt.Errorf("stored the master page: %w", err)
	}

	err = k.fp.Sync()
	if err != nil {
		return fmt.Errorf("flushing the data to disk: %w", err)
	}

	return nil
}

// master page format
// |   signal  | ptr (root)| page used | fl-headpage | fl-headSeq | ...
// | sig (32B) | root (8B) |    8B     |    8B       |    8B      |
//
// ... | fl-tailPage |   fl-tailSeq |  rootVersion | version |
//     | 	8B	     | 		8B	    |      8B      |   8B

func masterLoad(db *KV) error {
	// file is not present
	// allocate a page for master on the first write
	if db.mmap.File == 0 {
		db.mmap.Flushed = 1
		return nil
	}

	// load master page
	data := make([]byte, btree.PAGE_SIZE)
	_, err := db.fp.Read(data)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}
	sig := data[:32]
	root := binary.LittleEndian.Uint64(data[32:])
	used := binary.LittleEndian.Uint64(data[40:])

	// load freelist data
	headPage := binary.LittleEndian.Uint64(data[48:])
	headSeq := binary.LittleEndian.Uint64(data[56:])
	tailPage := binary.LittleEndian.Uint64(data[64:])
	tailSeq := binary.LittleEndian.Uint64(data[72:])
	rootVersion := binary.LittleEndian.Uint64(data[80:])
	version := binary.LittleEndian.Uint64(data[88:])

	db.mmap.FreeList.HeadPage = headPage
	db.mmap.FreeList.HeadSeq = headSeq
	db.mmap.FreeList.TailPage = tailPage
	db.mmap.FreeList.TailSeq = tailSeq
	db.rootVersion = rootVersion
	db.version = version

	// check signature
	if !bytes.Equal(sig, []byte(DB_SIG)) {
		return fmt.Errorf("invalid database signature")
	}

	// check if pages used is valid
	if used < 1 || used > uint64(db.mmap.File)/btree.PAGE_SIZE {
		return fmt.Errorf("invalid master page, incorrect page_used")
	}

	db.tree.Root = root
	db.mmap.Flushed = used
	return nil
}

func masterStore(db *KV) error {
	var data [96]byte

	copy(data[:32], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[32:], db.tree.Root)
	binary.LittleEndian.PutUint64(data[40:], db.mmap.Flushed)

	// write freelist data
	headPage := db.mmap.FreeList.HeadPage
	headSeq := db.mmap.FreeList.HeadSeq
	tailPage := db.mmap.FreeList.TailPage
	tailSeq := db.mmap.FreeList.TailSeq

	binary.LittleEndian.PutUint64(data[48:], headPage)
	binary.LittleEndian.PutUint64(data[56:], headSeq)
	binary.LittleEndian.PutUint64(data[64:], tailPage)
	binary.LittleEndian.PutUint64(data[72:], tailSeq)

	// write version data
	binary.LittleEndian.PutUint64(data[80:], db.rootVersion)
	binary.LittleEndian.PutUint64(data[88:], db.version)

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("writing header to the file")
	}

	return nil
}

func PrintPages(file string) {
	fp, err := os.Open(file)
	if err != nil {
		log.Fatalf("opening file: %s", err.Error())
	}

	fmt.Println("\nfile pages:")
	buf := make([]byte, btree.PAGE_SIZE)
	fi, err := fp.Stat()
	if err != nil {
		log.Fatalf("getting file stats: %s", err.Error())
	}

	if fi.Size()%btree.PAGE_SIZE != 0 {
		log.Fatal("file size of not a multiple of page size")
	}

	npages := fi.Size() / btree.PAGE_SIZE

	reader := bufio.NewReader(fp)
	for i := 0; i < int(npages); i++ {
		if i == 0 {
			fmt.Println("master page")
			reader.Read(buf)
			continue
		}
		_, err := reader.Read(buf)
		if err != nil {
			log.Fatalf("reading data from file into buffer: %s", err.Error())
		}

		btree.PrintNode(btree.BNode(buf))
		fmt.Println()
	}
}
