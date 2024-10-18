package mmap

import (
	"dbms/btree"
	"dbms/freelist"
	"fmt"
	"os"
	"syscall"
)

type Mmap struct {
	FileSize  int               // size of the file
	ChunkSize int               // size of currently mapped chunks
	Chunks    [][]byte          // pages that are mapped to disk
	Fp        *os.File          // pointer to the file
	Flushed   uint64            // number of pages flushed to the disk
	NFree     int               // number of pages taken from the list
	NAppend   int               // number of pages to be appended
	Free      freelist.FreeList // free linst
	// allocated and deallocated pages keyed by the pointer
	// nil value dentes a deallocated page
	Updates map[uint64][]byte
}

// Close unmaps the mapped file
func (m *Mmap) Close() {
	for _, chunk := range m.Chunks {
		err := syscall.Munmap(chunk)
		if err != nil {
			panic("closing chunks")
		}
	}
}

// ExtendFile takes in the desired number of pages (npages) as input
// and extended the file to fit in npages, it extendsfile using
// fallocate and extends the file incrementally to void frequent file extention
func (m *Mmap) ExtendFile(npages int) error {
	// number is pages present is greater the required file size
	// no need to extend the file
	filePages := m.FileSize / btree.PAGE_SIZE
	if filePages > npages {
		return nil
	}

	// extend the file exponentially
	for filePages < npages {
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}

	newFileSize := filePages * btree.PAGE_SIZE
	err := syscall.Fallocate(int(m.Fp.Fd()), 0, 0, int64(newFileSize))
	if err != nil {
		return fmt.Errorf("extending file: %w", err)
	}
	m.FileSize = newFileSize
	return nil
}

// ExtendMmap extendes the mmap by creating a new mmap
func (m *Mmap) ExtendMmap(npages int) error {

	// there is already enough space
	if m.ChunkSize >= npages*btree.PAGE_SIZE {
		return nil
	}

	// create a new chunk
	newChunk, err := syscall.Mmap(m.FileSize, int64(m.ChunkSize), m.ChunkSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mapping new chunk: %w", err)
	}

	m.ChunkSize += m.ChunkSize
	m.Chunks = append(m.Chunks, newChunk)
	return nil
}

// PageGet takes in page index and parameter
// it traverses though the chunks to find the right page
func (m *Mmap) PageGet(pgIdx uint64) []byte {
	if page, ok := m.Updates[pgIdx]; ok {
		return page
	}
	return PageGetMapped(m, pgIdx)
}

func PageGetMapped(m *Mmap, pgIdx uint64) []byte {
	start := uint64(0)
	for _, chunk := range m.Chunks {
		npages := len(chunk) / btree.PAGE_SIZE
		end := start + uint64(npages)

		if pgIdx < end {
			offset := btree.PAGE_SIZE * (pgIdx - start)
			return chunk[offset : offset+btree.PAGE_SIZE]
		}
		start = end
	}
	panic("invalid page index")
}

// PageNew allocate a new page in memory
// and added it to the temp pages
func (m *Mmap) PageNew(node []byte) uint64 {

	if len(node) > btree.PAGE_SIZE {
		panic("node size is greater then the page size")
	}

	ptr := uint64(0)
	// if there are freepages left use the freepage
	if m.NFree < int(m.Free.Total()) {
		ptr = m.Free.Get(m.NFree)
		m.NFree++
	} else {
		ptr = m.Flushed + uint64(m.NAppend)
		m.NAppend++
	}

	// allocate a new page
	m.Updates[ptr] = node
	return ptr
}

func (m *Mmap) PageDel(ptr uint64) {
	m.Updates[ptr] = nil
}

func MmapInit(fileloc string) (int, []byte, error) {
	// open file
	fp, err := os.OpenFile(fileloc, os.O_RDWR, 0644)
	if err != nil {
		return 0, nil, fmt.Errorf("opening file: %w", err)
	}

	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("geeting file stats: %w", err)
	}
	size := int(fi.Size())
	if size%btree.PAGE_SIZE != 0 {
		return 0, nil, fmt.Errorf("file size is not the multiple of page size")
	}

	// map the file into the memory
	/*
		params of mmap syscall
		fd     = is file descriptor, it's a number given to the file
		prot   = desired protection for the mapped file
		offset = the contetns of a the memory mapped file is initalize with the
				 byte starting from the specified offset
		length = number of bytes to be initiazed from the starting offset
		flag   = defines if the updates made to the mmap is visiable to other
				 processes mapped to the same region
	*/
	chunk, err := syscall.Mmap(int(fp.Fd()), 0,
		size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, nil, fmt.Errorf("maping file into memory: %w", err)
	}

	return size, chunk, nil
}
