package kv

import (
	"bufio"
	"bytes"
	"dbms/btree"
	"dbms/mmap"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
)

const DB_SIG = "Thanks,BuildYourOwnDbFromScratch"

type KV struct {
	Path string // path to the db file

	tree btree.BTree // Btree
	fp   *os.File    // pointer to file
	mmap mmap.Mmap   // mmap of the file
}

type Iterator interface {
	Deref() ([]byte, []byte)
	Next()
	Prev()
	Valid() bool
}

// type KVTX interface {
// 	Seek(key []byte) Iterator
// 	Get(key []byte) ([]byte, error)
// 	Set(key []byte, val []byte)
// 	Del(key []byte) bool
// }

// kv transaction
type KVTX struct {
	db   *KV
	tree struct {
		root uint64
	}
	free struct {
		headpage uint64
		headseq  uint64
		tailpage uint64
		tailseq  uint64
	}
}

func (kv *KV) Begin(tx *KVTX) {
	tx.db = kv
	tx.tree.root = kv.tree.Root

	tx.free.headpage = kv.mmap.FreeList.HeadPage
	tx.free.headseq = kv.mmap.FreeList.HeadSeq
	tx.free.tailpage = kv.mmap.FreeList.TailPage
	tx.free.tailseq = kv.mmap.FreeList.TailSeq
}

func rollback(kv *KV, tx *KVTX) {
	kv = tx.db
	kv.tree.Root = tx.tree.root

	kv.mmap.FreeList.HeadPage = tx.free.headpage
	kv.mmap.FreeList.HeadSeq = tx.free.headseq
	kv.mmap.FreeList.TailPage = tx.free.tailpage
	kv.mmap.FreeList.TailSeq = tx.free.tailseq

	kv.mmap.NAppend = 0
	kv.mmap.Updates = map[uint64][]byte{}
}

func (kv *KV) Abort(tx *KVTX) {
	rollback(kv, tx)
}

func (kv *KV) Commit(tx *KVTX) error {
	if kv.tree.Root == tx.tree.root {
		return nil
	}

	// write pages to disk
	if err := writePages(kv); err != nil {
		rollback(kv, tx)
		return fmt.Errorf("writing pages: %w", err)
	}

	// fsync pages so that the
	// data is persisted to the disk
	if err := syncPages(kv); err != nil {
		rollback(kv, tx)
		return fmt.Errorf("fsync: %w", err)
	}

	kv.mmap.Flushed += kv.mmap.NAppend
	kv.mmap.NAppend = 0
	kv.mmap.Updates = map[uint64][]byte{}

	// changeing the root node pointer
	// which makes the entire process atomic
	if err := masterStore(kv); err != nil {
		return fmt.Errorf("storing master page: %w", err)
	}
	if err := kv.fp.Sync(); err != nil {
		return fmt.Errorf("fsync master page: %w", err)
	}
	return nil
}

func NewKv(loc string) (*KV, error) {

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
	return k.db.tree.SeekLE(key)
}

func (k *KVTX) Get(key []byte) ([]byte, error) {
	return k.db.tree.GetVal(key)
}

func (k *KVTX) Set(key []byte, val []byte) {
	k.db.tree.Insert(key, val)
}

func (k *KVTX) Del(key []byte) bool {
	return k.db.tree.Delete(key)
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
// ... | fl-tailPage |   fl-tailSeq |
//     | 	8B	     | 		8B	    |

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

	db.mmap.FreeList.HeadPage = headPage
	db.mmap.FreeList.HeadSeq = headSeq
	db.mmap.FreeList.TailPage = tailPage
	db.mmap.FreeList.TailSeq = tailSeq

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
	var data [80]byte

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
