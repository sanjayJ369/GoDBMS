package db

import (
	"encoding/binary"
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

func TestEnDecKey(t *testing.T) {
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

	t.Run("encodeKey encodes the key", func(t *testing.T) {
		got := encodeKey(prefix, vals)

		want := make([]byte, 4)
		binary.BigEndian.PutUint32(want, prefix)
		want = append(want, serializeInt(vals[0].I64)...)
		want = append(want, serializeBytes(vals[1].Str)...)
		want = append(want, byte(0x00))

		assert.Equal(t, want, got, "endoing values")
	})

	t.Run("decodeKey decodes the keys", func(t *testing.T) {
		encodedkeys := encodeKey(prefix, vals)

		tdef := &TableDef{
			Types: []uint32{TYPE_INT64, TYPE_BYTES},
			Pkeys: 2,
		}
		got := decodeKey(encodedkeys, *tdef)
		assert.Equal(t, vals, got, "comparing original values with decoded values")
	})
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

// stubStore replicates the implmentation of the acutal kv store
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

	// insert value into kv store
	insertRecord(t, kvstore, getStubRecord())

	ok, err := dbGet(db, stubTableDef, rec)
	require.NoError(t, err, "getting record")
	assert.Equal(t, ok, true)
	assert.Equal(t, *getStubRecord(), *rec)
}

func TestGet(t *testing.T) {
	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)

	// create table defination record to insert into db
	// the table defination will be used by the db.Get method
	// so inserting the table defination is nessary
	insertStubTableDefination(t, kvstore, *stubTableDef)

	// insert value into kv store
	insertRecord(t, kvstore, getStubRecord())

	// get stub record
	rec := &Record{}
	rec.AddI64("id", 1)
	ok, err := db.Get("stub", rec)
	require.NoError(t, err, "getting stub record")
	assert.True(t, ok, "getting stub record")
	// check got and want record
	assert.Equal(t, *getStubRecord(), *rec, "comparing stub record")

}

func TestDBSet(t *testing.T) {
	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)

	rec := getStubRecord()
	dbSet(db, stubTableDef, *rec)

	got := &Record{}
	got.AddI64("id", 1)

	ok, err := dbGet(db, stubTableDef, got)
	require.NoError(t, err, "getting value")
	require.Equal(t, true, ok, "getting value")

	assert.Equal(t, rec, got)
}

func TestUpdate(t *testing.T) {

	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)
	// insert table defination, Update function uses table defination
	// to get the previously inserted value
	insertStubTableDefination(t, kvstore, *stubTableDef)

	t.Run("updating a pre existing value", func(t *testing.T) {

		// create another record with updated values
		updatedStubRec := &Record{}
		updatedStubRec.AddI64("id", 1)
		updatedStubRec.AddI64("number", 5)
		updatedStubRec.AddStr("string", []byte("this is updated string"))

		// insert record before updating it
		insertRecord(t, kvstore, getStubRecord())

		// update record
		ok, err := db.Update("stub", *updatedStubRec)
		require.NoError(t, err, "updaing record")
		assert.True(t, ok, "updating record")

		// get the updated value
		gotRec := &Record{}
		gotRec.AddI64("id", 1)
		dbGet(db, stubTableDef, gotRec)

		// check if the value is updated
		assert.Equal(t, *updatedStubRec, *gotRec, "the record is updated")
	})

	t.Run("trying to update non existing value", func(t *testing.T) {
		// update a value that does not exist
		gotRec := &Record{}
		gotRec.AddI64("id", 2)

		ok, err := db.Update("stub", *gotRec)
		require.Error(t, err, "updating non exisistent value")
		assert.False(t, ok, "updating non exisistent value")
	})
}

func TestUpsert(t *testing.T) {

	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)
	// insert table defination, Update function uses table defination
	// to get the previously inserted value
	insertStubTableDefination(t, kvstore, *stubTableDef)

	t.Run("insertes a new value if it does not exist", func(t *testing.T) {
		rec := getStubRecord()
		ok, err := db.Upsert("stub", *rec)
		require.NoError(t, err, "upserting value")
		assert.True(t, ok, "upserting value")

		// check if the new value is inserted in the kv store
		key := encodeKey(stubTableDef.Prefix, rec.Vals[:stubTableDef.Pkeys])
		wantval := encodeVal(rec.Vals[stubTableDef.Pkeys:])
		gotval, ok := kvstore.store[string(key)]

		assert.True(t, ok, "getting inserted kv pair")
		assert.Equal(t, wantval, gotval)
	})

	t.Run("update previously inserted value", func(t *testing.T) {
		// create another record with updated values
		updatedStubRec := &Record{}
		updatedStubRec.AddI64("id", 1)
		updatedStubRec.AddI64("number", 5)
		updatedStubRec.AddStr("string", []byte("this is updated string"))

		// upsert the updated record
		ok, err := db.Upsert("stub", *updatedStubRec)
		require.NoError(t, err, "upserting pre-exisiting value")
		assert.True(t, ok, "upserting pre-exisiting value")

		// get the key value
		pkvals := updatedStubRec.Vals[:stubTableDef.Pkeys]
		colvals := updatedStubRec.Vals[stubTableDef.Pkeys:]
		key := encodeKey(stubTableDef.Prefix, pkvals)
		wantval := encodeVal(colvals)
		gotval, ok := kvstore.store[string(key)]

		// check if they are updated
		assert.True(t, ok, "getting inserted kv pair")
		assert.Equal(t, wantval, gotval)
	})
}

func TestDelete(t *testing.T) {

	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)
	insertStubTableDefination(t, kvstore, *stubTableDef)
	t.Run("delete non existent value", func(t *testing.T) {
		rec := getStubRecord()
		pksrec := getPKs(*rec, stubTableDef.Pkeys)

		ok, err := db.Delete("stub", *pksrec)
		require.Error(t, err, "deleting non existent value")
		assert.False(t, ok, "deleting non existent value")
	})

	t.Run("deleting a value", func(t *testing.T) {
		// insert a new record
		insertRecord(t, kvstore, getStubRecord())

		// delete the record
		pksrec := getPKs(*getStubRecord(), stubTableDef.Pkeys)
		ok, err := db.Delete("stub", *pksrec)
		require.NoError(t, err, "deleting a value")
		assert.True(t, ok, "deleting a value")

		// check kvstore if the value still exists
		key := encodeKey(stubTableDef.Prefix, pksrec.Vals)
		_, ok = kvstore.store[string(key)]
		assert.False(t, ok, "value still exists")
	})
}

func TestDataSerialization(t *testing.T) {
	t.Run("test serialization of bytes", func(t *testing.T) {
		data := make([]byte, 0)
		data = append(data, []byte("hello world")...)
		// add a nil byte (0x00)
		data = append(data, byte(0x00))
		data = append(data, []byte("world hello's back")...)

		encodedData := serializeBytes(data)
		decodedData := deserializeBytes(encodedData)

		assert.Equal(t, data, decodedData, "comparing original data with deserialized data")
	})

	t.Run("test serialization of int64", func(t *testing.T) {
		num := int64(-23423)
		encodedNum := serializeInt(num)
		decodedNum := deserializeInt(encodedNum)
		assert.Equal(t, num, decodedNum, "comparing original int64 with deserialized num")
	})
}

func TestTableNew(t *testing.T) {
	kvstore := NewStubStore()
	db := NewDB("/tmp/", kvstore)

	t.Run("inserting a new table defination", func(t *testing.T) {
		// insert new table defination
		err := db.TableNew(stubTableDef)
		require.NoError(t, err, "inserting new table")

		// check if the table defination is present in kvstore
		rec := &Record{}
		rec.AddStr("name", []byte(stubTableDef.Name))
		ok, err := db.Get("@table", rec)
		require.NoError(t, err, "getting table defination")
		assert.True(t, ok, "getting talble defination")
	})

	t.Run("trying to reinsert table defination", func(t *testing.T) {
		// inserting a table again should throw an error
		err := db.TableNew(stubTableDef)
		assert.Error(t, err, "reinserting table defination")
	})

	t.Run("prefix of the inserted tables is different", func(t *testing.T) {
		// new table defination
		modtdef := *stubTableDef
		modtdef.Name = "stub1"
		err := db.TableNew(&modtdef)
		require.NoError(t, err, "inserting new table defination")

		oldtdef, err := getTableDef(db, "stub")
		require.NoError(t, err, "getting old table defination")
		newtdef, err := getTableDef(db, "stub1")
		require.NoError(t, err, "getting new table defination")

		assert.NotEqual(t, oldtdef.Prefix, newtdef.Prefix, "table prefix's are equal")
		assert.Equal(t, newtdef.Prefix, oldtdef.Prefix+1, "different table prefixs")
	})
}

func getStubRecord() *Record {
	stubRecord := &Record{}
	stubRecord.AddI64("id", 1)
	stubRecord.AddI64("number", 2)
	stubRecord.AddStr("string", []byte("this is a string"))
	return stubRecord
}

func insertStubTableDefination(t testing.TB, kvstore *stubStore, stubTableDef TableDef) {
	t.Helper()
	// create table defination record to insert into store
	tdefRec := &Record{}
	tdefRec.AddStr("name", []byte(stubTableDef.Name))
	tdefByte, err := json.Marshal(stubTableDef)
	require.NoError(t, err, "marshalling table defination")
	tdefRec.AddStr("def", tdefByte)

	// insert table defination
	key := encodeKey(TDEF_TABLE.Prefix, tdefRec.Vals[:stubTableDef.Pkeys])
	val, err := json.Marshal(tdefRec.Vals[stubTableDef.Pkeys:])
	require.NoError(t, err, "marshalling table defination record")
	kvstore.store[string(key)] = val
}

func insertRecord(t testing.TB, kvstore *stubStore, stubRecord *Record) {
	t.Helper()

	key := encodeKey(stubTableDef.Prefix, stubRecord.Vals[:stubTableDef.Pkeys])
	val, err := json.Marshal(stubRecord.Vals[stubTableDef.Pkeys:])
	require.NoError(t, err, "marshalling data")
	kvstore.store[string(key)] = val
}
