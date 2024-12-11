package db

import (
	"dbms/kv"
	"dbms/util"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var stubTableDef = &TableDef{
	Name:     "stub",
	Cols:     []string{"id", "number", "string"},
	Types:    []uint32{TYPE_INT64, TYPE_INT64, TYPE_BYTES},
	Pkeys:    1,
	Prefixes: []uint32{3, 4},
	Indexes:  [][]string{{"id"}, {"number"}},
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

// stubTX replicates the implmentation of the acutal kv store
type stubTX struct {
	store map[string][]byte
}

func (s *stubTX) Del(key []byte) bool {
	delete(s.store, string(key))
	return true
}
func (s *stubTX) Get(key []byte) ([]byte, error) {
	if val, ok := s.store[string(key)]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("key not found")
}
func (s *stubTX) Set(key, val []byte) {
	s.store[string(key)] = val
}
func (s *stubTX) Seek(key []byte) kv.Iterator {
	return nil
}

func NewStubStore() *stubTX {
	store := make(map[string][]byte)
	return &stubTX{
		store: store,
	}
}

func TestDBGet(t *testing.T) {

	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}

	rec := (&Record{}).AddI64("id", 1)

	// insert value into kv store
	insertRecord(t, store, getStubRecord())

	ok, err := dbGet(tx, stubTableDef, rec)
	require.NoError(t, err, "getting record")
	assert.Equal(t, ok, true)
	assert.Equal(t, *getStubRecord(), *rec)
}

func TestGet(t *testing.T) {

	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}
	// create table defination record to insert into db
	// the table defination will be used by the db.Get method
	// so inserting the table defination is nessary
	insertStubTableDefination(t, store, *stubTableDef)

	// insert value into kv store
	insertRecord(t, store, getStubRecord())

	// get stub record
	rec := &Record{}
	rec.AddI64("id", 1)
	ok, err := tx.Get(stubTableDef.Name, rec)
	require.NoError(t, err, "getting stub record")
	assert.True(t, ok, "getting stub record")
	// check got and want record
	assert.Equal(t, *getStubRecord(), *rec, "comparing stub record")

}

func TestDBSet(t *testing.T) {
	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}

	rec := getStubRecord()
	dbSet(tx, stubTableDef, *rec)

	got := &Record{}
	got.AddI64("id", 1)

	ok, err := dbGet(tx, stubTableDef, got)
	require.NoError(t, err, "getting value")
	require.Equal(t, true, ok, "getting value")

	assert.Equal(t, rec, got)

	// check if seconday indexes are inserted
	// structure of seconday key
	// prefix + vals + pk

	// adding prefix to create a key
	// which should represent the seconday index
	vals := make([]Value, 0)
	vals = append(vals, getStubRecord().Vals[1]) // appending num
	vals = append(vals, getStubRecord().Vals[:stubTableDef.Pkeys]...)

	wantkey := encodeKey(stubTableDef.Prefixes[1], vals)

	assert.Contains(t, store, string(wantkey))
}

func TestUpdate(t *testing.T) {
	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}
	// insert table defination, Update function uses table defination
	// to get the previously inserted value
	insertStubTableDefination(t, store, *stubTableDef)

	t.Run("updating a pre existing value", func(t *testing.T) {

		// create another record with updated values
		updatedStubRec := &Record{}
		updatedStubRec.AddI64("id", 1)
		updatedStubRec.AddI64("number", 5)
		updatedStubRec.AddStr("string", []byte("this is updated string"))

		// insert record before updating it
		err := tx.Insert(stubTableDef.Name, *getStubRecord())
		require.NoError(t, err, "inserting record")

		// update record
		ok := tx.Update("stub", *updatedStubRec)
		assert.True(t, ok, "updating record")

		// get the updated value
		gotRec := &Record{}
		gotRec.AddI64("id", 1)
		dbGet(tx, stubTableDef, gotRec)

		// check if the value is updated
		assert.Equal(t, *updatedStubRec, *gotRec, "the record is updated")

		// check if the seconday index is update
		// updating seconday is just deleting old key
		// and inserting new key
		oldSecVal := make([]Value, 0)
		oldSecVal = append(oldSecVal, getStubRecord().Vals[1])                      // adding index col
		oldSecVal = append(oldSecVal, getStubRecord().Vals[:stubTableDef.Pkeys]...) // adding primay keys
		wantkey := encodeKey(stubTableDef.Prefixes[1], oldSecVal)

		_, ok = store[string(wantkey)]
		assert.False(t, ok, "old seconday index is still present")

		newSecVal := make([]Value, 0)
		newSecVal = append(newSecVal, updatedStubRec.Vals[1])
		newSecVal = append(newSecVal, updatedStubRec.Vals[:stubTableDef.Pkeys]...)
		wantkey = encodeKey(stubTableDef.Prefixes[1], newSecVal)

		_, ok = store[string(wantkey)]
		assert.True(t, ok, "new seconday index is not present")

	})

	t.Run("trying to update non existing value", func(t *testing.T) {
		// update a value that does not exist
		gotRec := &Record{}
		gotRec.AddI64("id", 2)

		ok := tx.Update("stub", *gotRec)
		assert.False(t, ok, "updating non exisistent value")
	})
}

func TestUpsert(t *testing.T) {

	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}
	// insert table defination, Update function uses table defination
	// to get the previously inserted value
	insertStubTableDefination(t, store, *stubTableDef)

	t.Run("insertes a new value if it does not exist", func(t *testing.T) {
		rec := getStubRecord()
		ok := tx.Upsert("stub", *rec)
		assert.True(t, ok, "upserting value")

		// check if the new value is inserted in the kv store
		key := encodeKey(stubTableDef.Prefixes[0], rec.Vals[:stubTableDef.Pkeys])
		wantval := encodeVal(rec.Vals[stubTableDef.Pkeys:])
		gotval, ok := store[string(key)]

		assert.True(t, ok, "getting inserted kv pair")
		assert.Equal(t, wantval, gotval)

		// check if seconday index keys are inserted
		vals := make([]Value, 0)
		vals = append(vals, getStubRecord().Vals[1]) // appending num
		vals = append(vals, getStubRecord().Vals[:stubTableDef.Pkeys]...)

		wantkey := encodeKey(stubTableDef.Prefixes[1], vals)
		_, ok = store[string(wantkey)]
		assert.True(t, ok, "getting inserted seconday index")
	})

	t.Run("update previously inserted value", func(t *testing.T) {
		// create another record with updated values
		updatedStubRec := &Record{}
		updatedStubRec.AddI64("id", 1)
		updatedStubRec.AddI64("number", 5)
		updatedStubRec.AddStr("string", []byte("this is updated string"))

		// upsert the updated record
		ok := tx.Upsert("stub", *updatedStubRec)
		assert.True(t, ok, "upserting pre-exisiting value")

		// get the key value
		pkvals := updatedStubRec.Vals[:stubTableDef.Pkeys]
		colvals := updatedStubRec.Vals[stubTableDef.Pkeys:]
		key := encodeKey(stubTableDef.Prefixes[0], pkvals)
		wantval := encodeVal(colvals)
		gotval, ok := store[string(key)]

		// check if they are updated
		assert.True(t, ok, "getting inserted kv pair")
		assert.Equal(t, wantval, gotval)

		// check if old seconday indexes keys are deleted
		// and if new seconday indexes are inserted
		oldSecVal := make([]Value, 0)
		oldSecVal = append(oldSecVal, getStubRecord().Vals[1])                      // adding index col
		oldSecVal = append(oldSecVal, getStubRecord().Vals[:stubTableDef.Pkeys]...) // adding primay keys
		wantkey := encodeKey(stubTableDef.Prefixes[1], oldSecVal)

		_, ok = store[string(wantkey)]
		assert.False(t, ok, "old seconday index is still present")

		newSecVal := make([]Value, 0)
		newSecVal = append(newSecVal, updatedStubRec.Vals[1])
		newSecVal = append(newSecVal, updatedStubRec.Vals[:stubTableDef.Pkeys]...)
		wantkey = encodeKey(stubTableDef.Prefixes[1], newSecVal)

		_, ok = store[string(wantkey)]
		assert.True(t, ok, "new seconday index is not present")
	})
}

func TestDelete(t *testing.T) {
	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}
	insertStubTableDefination(t, store, *stubTableDef)
	t.Run("delete non existent value", func(t *testing.T) {
		rec := getStubRecord()
		pksrec := getPKs(*rec, stubTableDef.Pkeys)

		ok := tx.Delete("stub", *pksrec)
		assert.False(t, ok, "deleting non existent value")
	})

	t.Run("deleting a value", func(t *testing.T) {
		// insert a new record
		tx.Insert(stubTableDef.Name, *getStubRecord())

		fmt.Println(store)
		// delete the record
		pksrec := getPKs(*getStubRecord(), stubTableDef.Pkeys)
		ok := tx.Delete("stub", *pksrec)
		assert.True(t, ok, "deleting a value")

		// check kvstore if the value still exists
		key := encodeKey(stubTableDef.Prefixes[0], pksrec.Vals)
		_, ok = store[string(key)]
		assert.False(t, ok, "value still exists")

		// check if the seconday index still exists
		vals := make([]Value, 0)
		vals = append(vals, getStubRecord().Vals[1]) // appending num
		vals = append(vals, getStubRecord().Vals[:stubTableDef.Pkeys]...)

		wantkey := encodeKey(stubTableDef.Prefixes[1], vals)
		_, ok = store[string(wantkey)]
		assert.False(t, ok, "seconday index still exists")

		fmt.Println(store)
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
	store := make(map[string][]byte)
	tx := &DBTX{
		kv: &stubTX{
			store: store,
		},
	}

	t.Run("inserting a new table defination", func(t *testing.T) {
		// insert new table defination
		err := tx.TableNew(stubTableDef)
		require.NoError(t, err, "inserting new table")

		// check if the table defination is present in kvstore
		rec := &Record{}
		rec.AddStr("name", []byte(stubTableDef.Name))
		ok, err := tx.Get("@table", rec)
		require.NoError(t, err, "getting table defination")
		assert.True(t, ok, "getting talble defination")
	})

	t.Run("trying to reinsert table defination", func(t *testing.T) {
		// inserting a table again should throw an error
		err := tx.TableNew(stubTableDef)
		assert.Error(t, err, "reinserting table defination")
	})

	t.Run("prefix of the inserted tables is different", func(t *testing.T) {
		// new table defination
		modtdef := *stubTableDef
		modtdef.Name = "stub1"
		err := tx.TableNew(&modtdef)
		fmt.Println(modtdef.Prefixes)
		require.NoError(t, err, "inserting new table defination")

		oldtdef, err := getTableDef(tx, "stub")
		require.NoError(t, err, "getting old table defination")
		newtdef, err := getTableDef(tx, "stub1")
		require.NoError(t, err, "getting new table defination")

		assert.NotEqual(t, oldtdef.Prefixes, newtdef.Prefixes, "table prefix's are equal")
		assert.Equal(t, newtdef.Prefixes[0], oldtdef.Prefixes[0]+2, "different table prefixs")
		assert.Equal(t, newtdef.Prefixes[1], newtdef.Prefixes[0]+1, "different table prefixs")
	})
}

func TestIterator(t *testing.T) {

	loc := util.NewTempFileLoc()
	store, err := kv.NewKv(loc)
	require.NoError(t, err, "creating kv store")
	db := NewDB(util.NewTempFileLoc(), store)
	tx := &DBTX{
		kv: kv.NewKVTX(),
	}
	db.Begin(tx)
	// insert tabel defination
	testTable := &TableDef{
		Name:    "test",
		Cols:    []string{"id", "number", "name"},
		Types:   []uint32{TYPE_INT64, TYPE_INT64, TYPE_BYTES},
		Indexes: [][]string{{"id"}, {"number"}},
		Pkeys:   1,
	}

	err = tx.TableNew(testTable)
	require.NoError(t, err, "insertion table defination")

	// insert stub key value pairs
	valSize := 200

	count := 1500
	for i := 0; i < count; i++ {
		rec := &Record{}
		rec.AddI64("id", int64(i))
		rec.AddI64("number", int64(count-i))
		val := make([]byte, valSize)
		copy(val, []byte(fmt.Sprintf("row: %d", i)))
		rec.AddStr("name", val)
		err := tx.Insert("test", *rec)
		require.NoError(t, err, "inserting new row")
	}
	err = db.Commit(tx)
	require.NoError(t, err, "commiting trnsaction")
	tx = &DBTX{
		kv: kv.NewKVTX(),
	}
	db.Begin(tx)
	startIdx := int64(500)
	endIdx := int64(1000)
	scanLen := endIdx - startIdx

	startRec := &Record{}
	startRec.AddI64("id", startIdx)
	startRec.AddI64("number", int64(count)-startIdx)

	endRec := &Record{}
	endRec.AddI64("id", endIdx)
	endRec.AddI64("number", int64(count)-endIdx)

	sc := tx.NewScanner(*testTable, *startRec, *endRec, 0)

	t.Run("Dref returns current Row", func(t *testing.T) {
		gotRec, err := sc.Deref()
		require.NoError(t, err, "getting row")
		wantRec := &Record{}
		wantRec.AddI64("id", 500)
		wantRec.AddI64("number", int64(count-500))
		val := make([]byte, valSize)
		copy(val, []byte(fmt.Sprintf("row: %d", 500)))
		wantRec.AddStr("name", val)

		assert.Equal(t, *wantRec, *gotRec)
	})

	t.Run("Next goes to next Row", func(t *testing.T) {
		for i := startIdx; i <= endIdx; i++ {
			gotRec, err := sc.Deref()
			require.NoError(t, err, "getting row")
			wantRec := &Record{}
			wantRec.AddI64("id", int64(i))
			wantRec.AddI64("number", int64(count)-i)
			val := make([]byte, valSize)
			copy(val, []byte(fmt.Sprintf("row: %d", i)))
			wantRec.AddStr("name", val)

			assert.Equal(t, *wantRec, *gotRec)
			sc.Next()
		}

		// iterating beyond range should return false
		assert.False(t, sc.Valid(), "iterating beyond range")

		// go beyoncd range and get the row
		// err is expected
		_, err := sc.Deref()
		require.Error(t, err, "dereferencing beyond range, expected error")

		// undoing the previous next
		// so that iterator gets within range
		sc.Prev()
	})

	t.Run("Prev goes to previous Row", func(t *testing.T) {
		for i := endIdx; i >= startIdx; i-- {
			gotRec, err := sc.Deref()
			require.NoError(t, err, "getting row")
			wantRec := &Record{}
			wantRec.AddI64("id", int64(i))
			wantRec.AddI64("number", int64(count)-i)
			val := make([]byte, valSize)
			copy(val, []byte(fmt.Sprintf("row: %d", i)))
			wantRec.AddStr("name", val)

			assert.Equal(t, *wantRec, *gotRec)
			sc.Prev()
		}

		// iterating beyond range should return false
		assert.False(t, sc.Valid(), "iterating beyond range")
	})

	t.Run("scanning of secondary indexes is performed in accordance with the sequence of the secondary index values.", func(t *testing.T) {
		// testing scanning of seconday indexex
		sc = tx.NewScanner(*testTable, *startRec, *endRec, 1)
		// here the seconday index is number
		// number field of startRec is > then the number field of endRec
		// so the scan should begin from endRec upto startRec

		num := endRec.Vals[1].I64 // number field
		id := endRec.Vals[0].I64  // id filed
		for i := 0; i < int(scanLen); i++ {
			gotRec, err := sc.Deref()
			sc.Next()
			require.NoError(t, err, "dereferencing row")

			wantRec := &Record{}
			wantRec.AddI64("id", id)
			wantRec.AddI64("number", num)
			val := make([]byte, valSize)
			copy(val, []byte(fmt.Sprintf("row: %d", id)))
			wantRec.AddStr("name", val)

			assert.Equal(t, wantRec, gotRec)
			num++
			id--
		}
	})
}

// returns a record having the following structure
//
//	.| "id"   |  "number" | "string"           |
//	.|   1    |     2     |  "this is a string"|
func getStubRecord() *Record {
	stubRecord := &Record{}
	stubRecord.AddI64("id", 1)
	stubRecord.AddI64("number", 2)
	stubRecord.AddStr("string", []byte("this is a string"))
	return stubRecord
}

func insertStubTableDefination(t testing.TB, store map[string][]byte, stubTableDef TableDef) {
	t.Helper()
	// create table defination record to insert into store
	tdefRec := &Record{}
	tdefRec.AddStr("name", []byte(stubTableDef.Name))
	tdefByte, err := json.Marshal(stubTableDef)
	require.NoError(t, err, "marshalling table defination")
	tdefRec.AddStr("def", tdefByte)

	// insert table defination
	key := encodeKey(TDEF_TABLE.Prefixes[0], tdefRec.Vals[:stubTableDef.Pkeys])
	val, err := json.Marshal(tdefRec.Vals[stubTableDef.Pkeys:])
	require.NoError(t, err, "marshalling table defination record")
	store[string(key)] = val
}

func insertRecord(t testing.TB, store map[string][]byte, stubRecord *Record) {
	t.Helper()

	key := encodeKey(stubTableDef.Prefixes[0], stubRecord.Vals[:stubTableDef.Pkeys])
	val, err := json.Marshal(stubRecord.Vals[stubTableDef.Pkeys:])
	require.NoError(t, err, "marshalling data")
	store[string(key)] = val
}
