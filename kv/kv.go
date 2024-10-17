package kv

import (
	"bytes"
	"dbms/btree"
	"dbms/mmap"
	"encoding/binary"
	"fmt"
	"os"
)

const DB_SIG = "ThankToBuildYourOwnDbFromScratch"

type Kv struct {
	Path string // path to the db file

	tree btree.BTree // Btree
	fp   *os.File    // pointer to file
	mmap mmap.Mmap   // mmap of the file
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
		Fp:     fp,
		Total:  len(chunk),
		Chunks: [][]byte{chunk},
		File:   sz,
		Temp:   [][]byte{},
	}

	// initalize btree functions
	k.tree.Get = k.mmap.PageGet
	k.tree.Del = k.mmap.PageDel
	k.tree.New = k.mmap.PageNew

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
	npages := k.mmap.Flushed + uint64(len(k.mmap.Temp))
	err := k.mmap.ExtendFile(int(npages))
	if err != nil {
		return fmt.Errorf("extending file: %w", err)
	}

	err = k.mmap.ExtendMmap(int(npages))
	if err != nil {
		return fmt.Errorf("extending mmap: %w", err)
	}

	// copy the temp pages to the disk
	for i, page := range k.mmap.Temp {
		ptr := k.mmap.Flushed + uint64(i)
		copy(k.mmap.PageGet(ptr), page)
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
	k.mmap.Flushed += uint64(len(k.mmap.Temp))
	k.mmap.Temp = k.mmap.Temp[:0]

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
// |   signal  | ptr (root)| page used |
// | sig (32B) | root (8B) |    8B     |
func masterLoad(db *Kv) error {
	// file is not present
	// allocate a page for master on the first write
	if db.mmap.File == 0 {
		db.mmap.Flushed = 1
		return nil
	}

	// load master page
	data := db.mmap.Chunks[0]
	sig := data[:32]
	root := binary.LittleEndian.Uint64(data[32:])
	used := binary.LittleEndian.Uint64(data[40:])

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
	var data [48]byte

	copy(data[:32], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[32:], db.tree.Root)
	binary.LittleEndian.PutUint64(data[40:], db.mmap.Flushed)

	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("writing header to the file")
	}

	return nil
}
