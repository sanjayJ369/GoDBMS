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

type Kv struct {
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

func NewKv(loc string) (*Kv, error) {

	newfile := false
	if _, err := os.Stat(loc); errors.Is(err, os.ErrNotExist) {
		newfile = true
	}

	fp, err := os.OpenFile(loc, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening file: %w", err)
	}

	var k *Kv
	if newfile {
		k = &Kv{
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
		k = &Kv{
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

func (k *Kv) Seek(key []byte) Iterator {
	return k.tree.SeekLE(key)
}

// used only for debugging
func (k *Kv) PrintTree() {
	btree.PrintTree(k.tree, k.tree.Get(k.tree.Root))
}

func (k *Kv) Open() error {
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

func (k *Kv) Close() {
	k.mmap.Close()
	k.fp.Close()
}

func (k *Kv) Get(key []byte) ([]byte, error) {
	return k.tree.GetVal(key)
}

func (k *Kv) Set(key []byte, val []byte) error {
	k.tree.Insert(key, val)
	return flushpages(k)
}

func (k *Kv) Del(key []byte) (bool, error) {
	deleted := k.tree.Delete(key)
	return deleted, flushpages(k)
}

func flushpages(k *Kv) error {
	err := writePages(k)
	if err != nil {
		return fmt.Errorf("writing pages to disk: %w", err)
	}
	return syncPages(k)
}

// writePages extends the file
// and extends the mmap and copies the temp
// page into the disk
func writePages(k *Kv) error {
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
func syncPages(k *Kv) error {
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

func masterLoad(db *Kv) error {
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

func masterStore(db *Kv) error {
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
