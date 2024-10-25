package mmap

import (
	"dbms/btree"
	"fmt"
	"os"
	"syscall"
)

type Mmap struct {
	File    int
	Total   int
	Chunks  [][]byte
	Fp      *os.File
	Flushed uint64
	Temp    [][]byte
}

func (m *Mmap) Close() {
	syscall.Fsync(int(m.Fp.Fd()))
	for _, chunk := range m.Chunks {
		err := syscall.Munmap(chunk)
		if err != nil {
			panic("closing chunks")
		}
	}
}

func (m *Mmap) ExtendFile(npages int) error {
	// number is pages present is greater the required file size
	// not need to extend the file
	filePages := m.File / btree.PAGE_SIZE
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
	m.File = newFileSize
	return nil
}

func (m *Mmap) ExtendMmap(npages int) error {

	// there is already enough space
	if m.Total >= npages*btree.PAGE_SIZE {
		return nil
	}

	// skip the master page
	if m.Total == 0 {
		m.Total = btree.PAGE_SIZE
	}
	// create a new chunk
	newChunk, err := syscall.Mmap(int(m.Fp.Fd()), int64(m.Total), m.Total,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mapping new chunk: %w", err)
	}

	m.Total += m.Total
	m.Chunks = append(m.Chunks, newChunk)
	return nil
}

func (m *Mmap) PageGet(pgIdx uint64) []byte {
	start := uint64(0)
	pgIdx -= 1
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

func (m *Mmap) PageNew(node []byte) uint64 {
	// TODO: reuse the deallocated page
	if len(node) > btree.PAGE_SIZE {
		panic("node size is greater then the page size")
	}

	// allocate a new page
	ptr := m.Flushed + uint64(len(m.Temp))
	m.Temp = append(m.Temp, node)
	return ptr
}

func (m *Mmap) PageDel(ptr uint64) {
	// TODO: deallocate the page
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
	chunk, err := syscall.Mmap(int(fp.Fd()), btree.PAGE_SIZE,
		size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		return 0, nil, fmt.Errorf("maping file into memory: %w", err)
	}

	return size, chunk, nil
}