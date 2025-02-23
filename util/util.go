package util

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"log"

	"github.com/google/uuid"
)

type Set struct {
	sha   hash.Hash
	store map[string]bool
}

func (s *Set) getHash(key []byte) []byte {
	s.sha.Reset()
	s.sha.Write(key)
	return s.sha.Sum(nil)
}

func (s *Set) Has(key []byte) bool {
	hashedkey := s.getHash(key)
	_, ok := s.store[string(hashedkey)]
	return ok
}

func (s *Set) Set(key []byte) bool {
	if s.Has(key) {
		return false
	}
	hashedkey := s.getHash(key)
	s.store[string(hashedkey)] = true
	return true
}

func (s *Set) Del(key []byte) bool {
	if !s.Has(key) {
		return false
	}
	hashedkey := s.getHash(key)
	delete(s.store, string(hashedkey))
	return true
}

func NewSet() *Set {
	s := &Set{
		sha:   sha256.New(),
		store: make(map[string]bool),
	}
	return s
}

func NewTempFileLoc() string {
	id, err := uuid.NewUUID()
	if err != nil {
		log.Fatalf("getting new uuid: %s", err)
	}

	return fmt.Sprintf("/tmp/%d", id.ID())
}

func IntSliceRemove(key uint64, ls []uint64) {
	for i := 0; i < len(ls); i++ {
		if ls[i] == key {
			ls = append(ls[:i], ls[i+1:]...)
		}
	}
}
