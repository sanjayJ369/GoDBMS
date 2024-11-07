package db

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stubTableDef = &TableDef{
	Name:   "stub",
	Cols:   []string{"id", "number", "string"},
	Types:  []uint32{TYPE_INT64, TYPE_INT64, TYPE_BYTES},
	Pkeys:  1,
	Prefix: 3,
}

func TestEncodeKey(t *testing.T) {
	prefix := uint32(32)
	vals := []Value{
		{
			Type: TYPE_INT64,
			I64:  324,
		}, {
			Type: TYPE_BYTES,
			Str:  []byte("hello"),
		},
	}

	got := encodeKey(prefix, vals)

	want := make([]byte, 0)
	want = append(want, byte(prefix))
	want = append(want, byte(vals[0].I64))
	want = append(want, []byte(vals[1].Str)...)

	assert.Equal(t, want, got, "endoing values")
}

func TestDecodeVal(t *testing.T) {

	getRecord := (&Record{}).AddI64("id", 1)

	stubRecord := getStubRecord()
	vals := stubRecord.Vals[1:]
	data, err := json.Marshal(vals)
	require.NoError(t, err, "marshing value")
	decodeVal(stubTableDef, getRecord, data)
	assert.Equal(t, *stubRecord, *getRecord)
}

type stubStore struct {
	store map[string][]byte
}

func (s *stubStore) Close() {}
func (s *stubStore) Del(key []byte) (bool, error) {
	delete(s.store, string(key))
	return true, nil
}
func (s *stubStore) Get(key []byte) ([]byte, error) {
	if val, ok := s.store[string(key)]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("key not found")
}
func (s *stubStore) Open() error {
	return nil
}
func (s *stubStore) Set(key, val []byte) error {
	s.store[string(key)] = val
	return nil
}

func NewStubStore() *stubStore {
	store := make(map[string][]byte)
	return &stubStore{
		store: store,
	}
}

func TestDBGet(t *testing.T) {

	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)

	rec := (&Record{}).AddI64("id", 1)
	stubRecord := getStubRecord()

	// insert value into db
	key := encodeKey(stubTableDef.Prefix, stubRecord.Vals[:stubTableDef.Pkeys])
	val, err := json.Marshal(stubRecord.Vals[stubTableDef.Pkeys:])
	require.NoError(t, err, "marshalling data")
	kvstore.store[string(key)] = val

	ok, err := dbGet(db, stubTableDef, rec)
	require.NoError(t, err, "getting record ")
	assert.Equal(t, ok, true)
	assert.Equal(t, *getStubRecord(), *rec)
}

func getStubRecord() *Record {
	stubRecord := &Record{}
	stubRecord.AddI64("id", 1)
	stubRecord.AddI64("number", 2)
	stubRecord.AddStr("string", []byte("this is a string"))
	return stubRecord
}
