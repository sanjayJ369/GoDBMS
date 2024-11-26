package db

import (
	"bytes"
	"dbms/kv"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	TYPE_BYTES = 1
	TYPE_INT64 = 2

	MODE_UPDATE  = 0 // update only
	MODE_INSERT  = 1 // insert only
	MODE_UPDSERT = 2 // update or insert
)

// update request
type UpdateReq struct {
	Key []byte
	Val []byte

	Added bool // reports if new KV-pair has been added
	Mode  int
}

// Value struct is used to store the data type of the column
// this type of struct is known as tagged union
// here we only store single type of data either int64 or []byte
// which type of data is been stored is represented by Struct Value
// Value is encode and decoded in the from of json
type Value struct {
	Type uint32 `json:"type"`
	I64  int64  `json:"int64"`
	Str  []byte `json:"str"`
}

// Records represent a single tuple
// Cols represent the name of the column
// Vals represents the value for it's corresponding column
type Record struct {
	Cols []string
	Vals []Value
}

// AddStr adds a byte attribute value to the record
func (r *Record) AddStr(col string, val []byte) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{
		Type: TYPE_BYTES,
		Str:  val,
	})
	return r
}

// AddI64 adds a integer attribute value to the record
func (r *Record) AddI64(col string, val int64) *Record {
	r.Cols = append(r.Cols, col)
	r.Vals = append(r.Vals, Value{
		Type: TYPE_INT64,
		I64:  val,
	})
	return r
}

// Get returns the value corresponding to the given attribute
func (r *Record) Get(col string) *Value {
	for i, c := range r.Cols {
		if c == col {
			return &r.Vals[i]
		}
	}
	return nil
}

// TableDef struct is used the table definations
// tables are defined in such a way that the first
// PKeys columns are the primary keys
type TableDef struct {
	Name     string     `json:"name"`     // name of the table
	Cols     []string   `json:"cols"`     // colums present in the table
	Types    []uint32   `json:"types"`    // types of each column
	Pkeys    int        `json:"pkeys"`    // number of primary keys
	Prefixes []uint32   `json:"prefixes"` // prefix corresponding to this table (inclding indices)
	Indexes  [][]string `json:"indexex"`  // indexes contain columns present in each index, 1st element is primary key
}

// TDEF_TABLE is an internal table which stores each table name
// and it's corresponding defination
var TDEF_TABLE = &TableDef{
	Name:     "@table",
	Cols:     []string{"name", "def"},
	Types:    []uint32{TYPE_BYTES, TYPE_BYTES},
	Pkeys:    1,
	Prefixes: []uint32{2},
	Indexes:  [][]string{{"name"}},
}

// TDEF_META is an internal table which stores the meta data
// requried for handling the database
var TDEF_META = &TableDef{
	Name:     "@meta",
	Cols:     []string{"key", "value"},
	Types:    []uint32{TYPE_BYTES, TYPE_BYTES},
	Pkeys:    1,
	Prefixes: []uint32{1},
	Indexes:  [][]string{{"key"}},
}

type KVStore interface {
	Close()
	Del(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	Open() error
	Set(key []byte, val []byte) error
	Seek(key []byte) kv.Iterator
}

type Scanner struct {
	iter kv.Iterator
	Key1 Record
	Key2 Record
	Tdef TableDef

	index int
	// keys in bytes for faster comparition
	key1 []byte
	key2 []byte
}

func (sc *Scanner) Next() {
	sc.iter.Next()
}

func (sc *Scanner) Prev() {
	sc.iter.Prev()
}

// reports if we are within the range
func (sc *Scanner) Valid() bool {
	if !sc.iter.Valid() {
		return false
	}

	// compare current key with range
	key, _ := sc.iter.Deref()
	cmp := bytes.Compare(key, sc.key1)
	if cmp < 0 {
		return false
	}

	cmp = bytes.Compare(key, sc.key2)
	if cmp > 0 {
		return false
	}

	return true
}

func (sc *Scanner) Deref() (*Record, error) {
	// check if out of range
	if !sc.Valid() {
		return nil, fmt.Errorf("out of range")
	}

	key, val := sc.iter.Deref()
	rec, err := kvToRecord(sc.Tdef, key, val)
	if err != nil {
		return nil, fmt.Errorf("getting row: %w", err)
	}
	return rec, nil
}

type DB struct {
	Path string
	kv   KVStore
}

func NewDB(path string, kv KVStore) *DB {
	return &DB{
		Path: path,
		kv:   kv,
	}
}

func (db *DB) NewScanner(tdef TableDef, key1, key2 Record, idx int) *Scanner {

	var key1Bytes []byte
	var key2Bytes []byte

	if idx == 0 {
		key1Bytes = encodeKey(tdef.Prefixes[0], getPKs(key1, tdef.Pkeys).Vals)
		key2Bytes = encodeKey(tdef.Prefixes[0], getPKs(key2, tdef.Pkeys).Vals)
	} else {
		key1vals := make([]Value, 0)
		key2vals := make([]Value, 0)
		for _, col := range tdef.Indexes[idx] {
			val1 := key1.Get(col)
			val2 := key2.Get(col)

			key1vals = append(key1vals, *val1)
			key2vals = append(key2vals, *val2)
		}

		key1Bytes = encodeKey(tdef.Prefixes[idx], key1vals)
		key2Bytes = encodeKey(tdef.Prefixes[idx], key2vals)
	}

	sc := &Scanner{
		iter:  db.kv.Seek(key1Bytes),
		Key1:  key1,
		Key2:  key2,
		Tdef:  tdef,
		index: idx,
		key1:  key1Bytes,
		key2:  key2Bytes,
	}
	return sc
}

func (db *DB) TableNew(tdef *TableDef) error {

	// create record to store the table defination
	rec := &Record{}
	rec.AddStr("name", []byte(tdef.Name))

	// check if the table already exists
	recpks := getPKs(*rec, tdef.Pkeys)
	ok, _ := dbGet(db, TDEF_TABLE, recpks)
	if ok {
		return fmt.Errorf("table already exists")
	}

	// before inserting the table get the table prefix
	prefixrec := &Record{}
	prefixrec.AddStr("key", []byte("prefix"))
	ok, _ = dbGet(db, TDEF_META, prefixrec)

	var curPrefix int64
	// there is no prefix stored in the meta
	if !ok {
		// prefix of new table starts from 3
		// as 1 as 2 are reserved for catalog tables
		curPrefix = 3
	} else {
		curPrefix = prefixrec.Get("value").I64
	}

	// assign prefixes to the indexes of the table
	Prefixes := make([]uint32, 0)
	for range tdef.Indexes {
		Prefixes = append(Prefixes, uint32(curPrefix))
		curPrefix++
	}

	tdef.Prefixes = make([]uint32, len(Prefixes))
	copy(tdef.Prefixes, Prefixes)

	// update the prefix to the new value
	prefixrec.AddI64("data", curPrefix)
	ok, err := dbSet(db, TDEF_META, *prefixrec)
	if err != nil {
		return fmt.Errorf("inserting prefix: %w", err)
	}
	if !ok {
		return fmt.Errorf("inserting prefix")
	}

	data, err := json.Marshal(tdef)
	if err != nil {
		return fmt.Errorf("marshalling table defination: %w", err)
	}
	rec.AddStr("def", data)

	// insert table defination on @table
	ok, err = dbSet(db, TDEF_TABLE, *rec)
	if err != nil {
		return fmt.Errorf("inserting table defination: %w", err)
	}
	if !ok {
		return fmt.Errorf("inserting table defination")
	}
	return nil
}

// Get gets the required rec, here the record(rec) should only contain
// the primary keys, the columns(attribute) which are not primary keys
// are added by the Get func into the rec itself
// the rec can be further used to get the column values
func (db *DB) Get(table string, rec *Record) (bool, error) {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false, err
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}
	return dbGet(db, tdef, rec)
}

// Insert only insertes a new record into the table
// if the value already exists error is thrown
// here the record should contain all the requied columns along with there values
// fist 'n' columns should always be the primary keys
func (db *DB) Insert(table string, rec Record) (bool, error) {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false, err
	}
	if tdef == nil {
		return false, fmt.Errorf("table not found: %s", table)
	}

	// create a new temp record to try getting the value
	// this is to check if it's already been inserted
	tempRec := &Record{}
	for i, val := range rec.Vals[:tdef.Pkeys] {
		switch val.Type {
		case TYPE_BYTES:
			tempRec.AddStr(rec.Cols[i], val.Str)
		case TYPE_INT64:
			tempRec.AddI64(rec.Cols[i], val.I64)
		}
	}

	ok, _ := dbGet(db, tdef, tempRec)
	// key does not exists insert new key value pair
	if !ok {
		return dbSet(db, tdef, rec)
	}

	return false, fmt.Errorf("key already exists")
}

// Update updates preexisting record in the given table
// to the new record provided
// if the record does not exists, error is thrown
func (db *DB) Update(table string, rec Record) (bool, error) {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false, fmt.Errorf("getting table defination: %s", err)
	}
	// try getting the value to check if it exists
	getRec := getPKs(rec, tdef.Pkeys)
	ok, _ := dbGet(db, tdef, getRec)
	if !ok {
		return false, fmt.Errorf("value does not exists")
	}

	// if the value exists update the primary index
	_, err = dbSet(db, tdef, rec)
	if err != nil {
		return false, fmt.Errorf("setting primary index: %w", err)
	}

	// delete old seconday indexes
	keys := getSecondaryIndexesKeys(tdef, *getRec)
	for _, key := range keys {
		_, err := db.kv.Del(key)
		if err != nil {
			return false, fmt.Errorf("deleting seconday index: %w", err)
		}
	}

	// insert new seconday indexex
	keys = getSecondaryIndexesKeys(tdef, rec)
	for _, key := range keys {
		err := db.kv.Set(key, []byte{})
		if err != nil {
			return false, fmt.Errorf("deleting seconday index: %w", err)
		}
	}

	return true, nil
}

// Upsert tries to update the value if it does not exist
// it inserts a new value, here the record is assumed to
// contain all the attributes(columns) of the table
func (db *DB) Upsert(table string, rec Record) (bool, error) {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false, fmt.Errorf("getting table defination: %s", err)
	}

	// if row exists update else insert
	getRec := getPKs(rec, tdef.Pkeys)
	ok, _ := dbGet(db, tdef, getRec)
	if ok {
		return db.Update(table, rec)
	} else {
		return db.Insert(table, rec)
	}
}

// Delete deletes the record entry from the table
// throws an error if the record does not exists
// here the record is assumed to contain all the primary keys
func (db *DB) Delete(table string, rec Record) (bool, error) {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false, fmt.Errorf("getting table defination: %s", err)
	}
	return dbDel(db, tdef, rec)
}

func dbDel(db *DB, tdef *TableDef, rec Record) (bool, error) {
	// get the record values
	values, err := getRecordVals(tdef, rec, tdef.Pkeys)
	if err != nil {
		return false, fmt.Errorf("getting record values: %w", err)
	}

	// encode the primary keys and table prefix to get the key
	key := encodeKey(tdef.Prefixes[0], values[:tdef.Pkeys])
	ok, err := dbGet(db, tdef, &rec)
	if !ok {
		return false, fmt.Errorf("value doest not exist: %w", err)
	}

	// the rec that is given may only contain the primary keys
	// so we get the entire row, which is used to get the keys of the
	// seconday indexes then delete the seconday keys
	// we don't have to call dbGet again as it's already been
	// called to check whether rec is present or not
	if len(tdef.Indexes) > 1 {
		keys := getSecondaryIndexesKeys(tdef, rec)
		for _, k := range keys {
			_, err := db.kv.Del(k)
			if err != nil {
				return false, fmt.Errorf("deleting seconday keys: %w", err)
			}
		}
	}

	return db.kv.Del(key)
}

func dbSet(db *DB, tdef *TableDef, rec Record) (bool, error) {
	// get the record values
	values, err := getRecordVals(tdef, rec, tdef.Pkeys)
	if err != nil {
		return false, fmt.Errorf("getting record values: %w", err)
	}

	// get the encoded key
	// set the primay index
	key := encodeKey(tdef.Prefixes[0], values[:tdef.Pkeys])
	val := encodeVal(values[tdef.Pkeys:])

	err = db.kv.Set(key, val)
	if err != nil {
		return false, fmt.Errorf("error setting key value pair: %w", err)
	}

	// setting the secondary indexes
	// seconday indexes does not hagetSecondaryIndexesKeysve any value
	if len(tdef.Indexes) > 1 {
		keys := getSecondaryIndexesKeys(tdef, rec)
		for _, k := range keys {
			err = db.kv.Set(k, []byte{})
			if err != nil {
				return false, fmt.Errorf("error setting key value pair: %w", err)
			}
		}
	}
	return true, nil
}

// getSecondaryIndexesKeys returns slice of keys when represent keys
// which contain prefix + cols of index + primary key of table
func getSecondaryIndexesKeys(tdef *TableDef, rec Record) [][]byte {

	keys := make([][]byte, 0)
	// loop though the indexes and create new key
	for i, idx := range tdef.Indexes[1:] {

		pks := make([]Value, 0)

		// populate the temprec with the cols of the index
		for _, col := range idx {
			val := rec.Get(col)
			// it is possible that that value is
			// not defined in case the value is byte
			// change the value to 0xff to preserve ordering
			if val.Type == TYPE_BYTES && len(val.Str) == 0 {
				val.Str = []byte{0xff}
			}

			pks = append(pks, *val)
		}

		// add primary keys to the temprec
		pks = append(pks, rec.Vals[:tdef.Pkeys]...)

		key := encodeKey(tdef.Prefixes[i+1], pks)
		keys = append(keys, key)
	}
	return keys
}

// dbGet gets the value and the value is stored in the rec itself
// the rec is assumend to contain all the primary keys
// example: consider tuple <id, name, course, semester>
// input rec: <id>
// output rec: <id, name, course, semester>
func dbGet(db *DB, tdef *TableDef, rec *Record) (bool, error) {
	// get the record values
	values, err := getRecordVals(tdef, *rec, tdef.Pkeys)
	if err != nil {
		return false, fmt.Errorf("getting record values: %w", err)
	}

	// encode the primary keys and table prefix to get the key
	key := encodeKey(tdef.Prefixes[0], values[:tdef.Pkeys])

	// get the value
	val, err := db.kv.Get(key)
	if err != nil {
		return false, fmt.Errorf("getting key(%s): %w", key, err)
	}

	// deocde the value into the record
	err = decodeVal(tdef, rec, val)
	if err != nil {
		return false, fmt.Errorf("decoding values: %w", err)
	}
	return true, nil
}

// getRecordVals returns values of the record
// and repots if the given record is valid
func getRecordVals(tdef *TableDef, rec Record, n int) ([]Value, error) {
	if len(rec.Cols) < n || n != tdef.Pkeys {
		return nil, fmt.Errorf("invalid number of columns in record")
	}
	return rec.Vals, nil
}

// encodeKey combines the prefix of the table
// and the primary keys of the record to get the
// record
func encodeKey(prefix uint32, pks []Value) []byte {
	delimiter := byte(0x00)
	key := make([]byte, 4)
	// add prefix
	binary.BigEndian.PutUint32(key, prefix)
	for _, val := range pks {
		switch val.Type {
		case TYPE_BYTES:
			key = append(key, serializeBytes(val.Str)...)
			// add delimiter
			key = append(key, delimiter)
		case TYPE_INT64:
			key = append(key, serializeInt(val.I64)...)
		default:
			panic("invalid value type")
		}
	}
	return key
}

// decodeKey decode the keys
func decodeKey(encodedBytes []byte, tdef TableDef) []Value {
	// ignore prefix
	// prefix is uint32 which is 4 bytes
	encodedBytes = encodedBytes[4:]

	// split the keys
	pks := make([][]byte, 0)
	bytePtr := 0
	for _, keyType := range tdef.Types[:tdef.Pkeys] {
		key := make([]byte, 0)
		switch keyType {
		case TYPE_BYTES:
			for encodedBytes[bytePtr] != byte(0x00) {
				key = append(key, encodedBytes[bytePtr])
				bytePtr++
			}
			// skip the delimiter 0x00
			bytePtr++
		case TYPE_INT64:
			key = append(key, encodedBytes[bytePtr:bytePtr+8]...)
			bytePtr += 8 // size of int64
		default:
			panic("invalid key type")
		}
		pks = append(pks, key)
	}

	// keys are still in the encoded form
	// decode keys
	vals := make([]Value, 0)
	for i, keyType := range tdef.Types[:tdef.Pkeys] {
		val := &Value{}
		switch keyType {
		case TYPE_INT64:
			val.Type = TYPE_INT64
			val.I64 = deserializeInt(pks[i])
		case TYPE_BYTES:
			val.Type = TYPE_BYTES
			val.Str = deserializeBytes(pks[i])
		}
		vals = append(vals, *val)
	}

	return vals
}

func deserializeBytes(encodedBytes []byte) []byte {
	decodedBytes := make([]byte, 0)
	escape := false
	for _, b := range encodedBytes {
		if b == 0x01 && !escape {
			escape = true
			continue
		}

		if escape {
			escape = false
			switch b {
			case 0x01:
				decodedBytes = append(decodedBytes, byte(0x00))
			case 0x02:
				decodedBytes = append(decodedBytes, byte(0x01))
			default:
				panic("invalid escape character")
			}
		} else {
			decodedBytes = append(decodedBytes, b)
		}
	}
	return decodedBytes
}

func serializeBytes(s []byte) []byte {
	encodedBytes := make([]byte, 0)
	for _, b := range s {
		switch byte(b) {
		case 0x00:
			encodedBytes = append(encodedBytes, []byte{0x01, 0x01}...)
		case 0x01:
			encodedBytes = append(encodedBytes, []byte{0x01, 0x02}...)
		default:
			encodedBytes = append(encodedBytes, byte(b))
		}
	}
	return encodedBytes
}

func deserializeInt(encodedNum []byte) int64 {
	num := binary.BigEndian.Uint64(encodedNum)
	// flip back the first bit
	num = num ^ (1 << 63)

	decodedNum := int64(num)
	return decodedNum
}

func serializeInt(num int64) []byte {
	// we should not allow -9223372036854775808 to as a key
	// because it is represented as 10000.0000 (1 followed by 63 zeros)
	// when we serialize it it turns out to be 0000..000 (64 0's)
	// which is our delimiter
	if num == -9223372036854775808 {
		panic("number is too small, serializing causes collusion with delimiter")
	}

	res := make([]byte, 8)
	// flip the 1st bit
	// if the number of positive, 1st bit will be  changed
	// to 1, which makes positive number greater then
	// negative number in bytes comparition
	// negative numbers are stored as 2's complements
	// the greater the magniture, greater the number
	binary.BigEndian.PutUint64(res, uint64(num)^(1<<63))
	return res
}

func encodeVal(vals []Value) []byte {
	data, err := json.Marshal(vals)
	if err != nil {
		panic("mashaling json data")
	}
	return data
}

// decodeVal decodes the values into the Record
func decodeVal(tdef *TableDef, rec *Record, val []byte) error {
	decodedVals := make([]Value, 0)
	err := json.Unmarshal(val, &decodedVals)
	if err != nil {
		return fmt.Errorf("unmashaing value into record: %w", err)
	}
	for i, val := range decodedVals {
		colName := tdef.Cols[i+tdef.Pkeys]
		rec.Cols = append(rec.Cols, colName)
		rec.Vals = append(rec.Vals, val)
	}
	return nil
}

func kvToRecord(tdef TableDef, key, val []byte) (*Record, error) {
	rec := &Record{}
	keys := decodeKey(key, tdef)
	for i, key := range keys {
		switch key.Type {
		case TYPE_BYTES:
			rec.AddStr(tdef.Cols[i], key.Str)
		case TYPE_INT64:
			rec.AddI64(tdef.Cols[i], key.I64)
		default:
			panic("invalid key type")
		}
	}

	err := decodeVal(&tdef, rec, val)
	if err != nil {
		return nil, fmt.Errorf("decoding values: %w", err)
	}
	return rec, nil
}

// getTableDef gets and returns the table defination
func getTableDef(db *DB, table string) (*TableDef, error) {
	if table == "@table" {
		return TDEF_TABLE, nil
	} else if table == "@meta" {
		return TDEF_META, nil
	}
	rec := (&Record{}).AddStr("name", []byte(table))
	_, err := dbGet(db, TDEF_TABLE, rec)
	if err != nil {
		return nil, fmt.Errorf("getting table defination: %w", err)
	}
	tdef := &TableDef{}
	err = json.Unmarshal(rec.Get("def").Str, tdef)
	if err != nil {
		return nil, fmt.Errorf("decoding table defination: %w", err)
	}
	return tdef, nil
}

// getPKs extracts the primary keys and returns
// record with only primary keys in it
func getPKs(rec Record, Pkeys int) *Record {
	newRec := &Record{}

	for i := 0; i < Pkeys; i++ {
		switch rec.Vals[i].Type {
		case TYPE_BYTES:
			newRec.AddStr(rec.Cols[i], rec.Vals[i].Str)
		case TYPE_INT64:
			newRec.AddI64(rec.Cols[i], rec.Vals[i].I64)
		}
	}

	return newRec
}
