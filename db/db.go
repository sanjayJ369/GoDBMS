package db

import (
	"bytes"
	"dbms/kv"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
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
	Begin(*kv.KVTX)
	Abort(*kv.KVTX)
	Commit(*kv.KVTX) error
	Close()
}

type Scanner struct {
	iter kv.Iterator
	Key1 Record
	Key2 Record
	Tdef TableDef

	// include keys or not
	key1Include bool
	key2Include bool

	index int
	db    *DBTX
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
	if cmp == 0 && !sc.key1Include {
		return false
	}

	if cmp < 0 {
		return false
	}

	cmp = bytes.Compare(key, sc.key2)
	if cmp == 0 && !sc.key2Include {
		return false
	}
	if cmp > 0 {
		return false
	}

	return true
}

func (sc *Scanner) kvToRecord(tdef *TableDef, key, val []byte, idx int) (*Record, error) {
	// if we are scanner seconday index
	// get the primary keys, then deref the row
	if idx != 0 {
		rec := getPKFromSecKey(tdef, key, idx)
		_, err := dbGet(sc.db, tdef, rec)
		if err != nil {
			return nil, fmt.Errorf("getting seconday index row: %w", err)
		}
		return rec, nil
	}

	rec := &Record{}
	keys := decodeKey(key, *tdef)
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

	err := decodeVal(tdef, rec, val)
	if err != nil {
		return nil, fmt.Errorf("decoding values: %w", err)
	}
	return rec, nil
}

func (sc *Scanner) Deref() (*Record, error) {
	// check if out of range
	if !sc.Valid() {
		return nil, fmt.Errorf("out of range")
	}

	key, val := sc.iter.Deref()
	rec, err := sc.kvToRecord(&sc.Tdef, key, val, sc.index)
	if err != nil {
		return nil, fmt.Errorf("getting row: %w", err)
	}
	return rec, nil
}

type DB struct {
	Path string
	kv   KVStore
}

type KVTX interface {
	Del(key []byte) bool
	Get(key []byte) ([]byte, error)
	Seek(key []byte) kv.Iterator
	Set(key []byte, val []byte)
}

type TX interface {
	Delete(table string, rec Record) bool
	Get(table string, rec *Record) (bool, error)
	Insert(table string, rec Record) error
	NewScanner(tdef TableDef, key1 Record, key2 Record, idx int) *Scanner
	TableNew(tdef *TableDef) error
	Update(table string, rec Record) bool
	Upsert(table string, rec Record) bool
}

type DBTX struct {
	kv KVTX
	db *DB
}

func (db *DB) NewTX() *DBTX {
	return &DBTX{
		kv: kv.NewKVTX(),
	}
}
func (db *DB) Begin(tx *DBTX) {
	tx.db = db
	kvTX, ok := tx.kv.(*kv.KVTX)
	if !ok {
		panic("Invalid type for tx.kv; expected *kv.KVTX")
	}
	db.kv.Begin(kvTX)
}

func (db *DB) Commit(tx *DBTX) error {
	kvTX, ok := tx.kv.(*kv.KVTX)
	if !ok {
		panic("Invalid type for tx.kv; expected *kv.KVTX")
	}
	return db.kv.Commit(kvTX)
}

func (db *DB) Abort(tx *DBTX) {
	kvTX, ok := tx.kv.(*kv.KVTX)
	if !ok {
		panic("Invalid type for tx.kv; expected *kv.KVTX")
	}
	db.kv.Abort(kvTX)
}

func NewDB(path string, kv KVStore) *DB {
	return &DB{
		Path: path,
		kv:   kv,
	}
}

// NewScanner returns a scanner which is can used to iterate
// though the valus from key1 to key2, in ascending order
// if key2 is less then key1, iterator starts from key2 and
// goes upto key1
func (db *DBTX) NewScanner(tdef TableDef, key1, key2 Record, key1inc, key2inc bool, idx int) *Scanner {

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

		// key1vals = append(key1vals, key1.Vals[:tdef.Pkeys]...)
		// key2vals = append(key2vals, key2.Vals[:tdef.Pkeys]...)

		key1Bytes = encodeKey(tdef.Prefixes[idx], key1vals)
		key2Bytes = encodeKey(tdef.Prefixes[idx], key2vals)
	}

	// as the data in the store is stored in ascending order
	// we are making sure that key1 is less then key2
	// this is done so that key2 marks the left end
	cmp := bytes.Compare(key1Bytes, key2Bytes)
	if cmp > 0 {
		key1Bytes, key2Bytes = key2Bytes, key1Bytes
		key1, key2 = key2, key1
		key1inc, key2inc = key2inc, key1inc
	}

	if idx != 0 {
		if key1inc {
			key1Bytes = append(key1Bytes, byte(0x00))
		} else {
			key1Bytes = append(key1Bytes, byte(0xff))
		}

		if key2inc {
			key2Bytes = append(key2Bytes, byte(0xff))
		} else {
			key2Bytes = append(key2Bytes, byte(0x00))
		}
	}

	sc := &Scanner{
		iter:        db.kv.Seek(key1Bytes),
		Key1:        key1,
		Key2:        key2,
		Tdef:        tdef,
		index:       idx,
		key1:        key1Bytes,
		key2:        key2Bytes,
		key1Include: key1inc,
		key2Include: key2inc,
		db:          db,
	}

	key, _ := sc.iter.Deref()
	// rec, _ := sc.kvToRecord(&sc.Tdef, key, val, sc.index)
	// fmt.Println(rec)

	// when derefercing a non-primary key
	// we arrivate at a node previous to the actual node
	// so we move a step ahead
	cmp = bytes.Compare(key, sc.key1)
	if cmp < 0 && idx != 0 {
		sc.Next()
	}

	key, _ = sc.iter.Deref()
	cmp = bytes.Compare(key, sc.key1)
	for cmp <= 0 && !sc.key1Include {
		sc.Next()
		key, _ = sc.iter.Deref()
		cmp = bytes.Compare(key, sc.key1)
	}

	return sc
}

func (db *DBTX) TableNew(tdef *TableDef) error {

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
	dbSet(db, TDEF_META, *prefixrec)

	data, err := json.Marshal(tdef)
	if err != nil {
		return fmt.Errorf("marshalling table defination: %w", err)
	}
	rec.AddStr("def", data)

	// insert table defination on @table
	dbSet(db, TDEF_TABLE, *rec)
	return nil
}

// Get gets the required rec, here the record(rec) should only contain
// the primary keys, the columns(attribute) which are not primary keys
// are added by the Get func into the rec itself
// the rec can be further used to get the column values
func (db *DBTX) Get(table string, rec *Record) (bool, error) {
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
func (db *DBTX) Insert(table string, rec Record) error {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return err
	}
	if tdef == nil {
		return fmt.Errorf("table not found: %s", table)
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

	return fmt.Errorf("key already exists")
}

// Update updates preexisting record in the given table
// to the new record provided
// if the record does not exists, error is thrown
func (db *DBTX) Update(table string, rec Record) bool {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false
	}
	// try getting the value to check if it exists
	getRec := getPKs(rec, tdef.Pkeys)
	ok, _ := dbGet(db, tdef, getRec)
	if !ok {
		return false
	}

	// if the value already exists delete the value
	ok = dbDel(db, tdef, *getRec)
	if !ok {
		return false
	}

	// if the value exists update the primary index
	err = dbSet(db, tdef, rec)
	if err != nil {
		fmt.Printf("updating the record: %s", err)
		return false
	}

	return true
}

// Upsert tries to update the value if it does not exist
// it inserts a new value, here the record is assumed to
// contain all the attributes(columns) of the table
func (db *DBTX) Upsert(table string, rec Record) bool {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false
	}

	// if row exists update else insert
	getRec := getPKs(rec, tdef.Pkeys)
	ok, _ := dbGet(db, tdef, getRec)
	if ok {
		return db.Update(table, rec)
	} else {
		err := db.Insert(table, rec)
		if err != nil {
			log.Printf("inserting record: %s", err)
			return false
		}
	}
	return true
}

// Delete deletes the record entry from the table
// throws an error if the record does not exists
// here the record is assumed to contain all the primary keys
func (db *DBTX) Delete(table string, rec Record) bool {
	tdef, err := getTableDef(db, table)
	if err != nil {
		return false
	}
	return dbDel(db, tdef, rec)
}

func dbDel(db *DBTX, tdef *TableDef, rec Record) bool {
	// get the record values
	values, err := getRecordVals(tdef, rec, tdef.Pkeys)
	if err != nil {
		return false
	}

	// encode the primary keys and table prefix to get the key
	key := encodeKey(tdef.Prefixes[0], values[:tdef.Pkeys])
	ok, _ := dbGet(db, tdef, &rec)
	if !ok {
		return false
	}

	// the rec that is given may only contain the primary keys
	// so we get the entire row, which is used to get the keys of the
	// seconday indexes then delete the seconday keys
	// we don't have to call dbGet again as it's already been
	// called to check whether rec is present or not
	if len(tdef.Indexes) > 1 {
		keys := getSecondaryIndexesKeys(tdef, rec)
		for _, k := range keys {
			ok := db.kv.Del(k)
			if !ok {
				return false
			}
		}
	}
	return db.kv.Del(key)
}

func dbSet(db *DBTX, tdef *TableDef, rec Record) error {
	// get the record values
	values, err := getRecordVals(tdef, rec, tdef.Pkeys)
	if err != nil {
		return fmt.Errorf("getting record values: %w", err)
	}

	// get the encoded key
	// set the primay index
	key := encodeKey(tdef.Prefixes[0], values[:tdef.Pkeys])
	val := encodeVal(values[tdef.Pkeys:])
	db.kv.Set(key, val)

	// setting the secondary indexes
	// seconday indexes does not haave getSecondaryIndexesKeys any value
	if len(tdef.Indexes) > 1 {
		keys := getSecondaryIndexesKeys(tdef, rec)
		for _, k := range keys {
			db.kv.Set(k, []byte{})
		}
	}
	return nil
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
func dbGet(db *DBTX, tdef *TableDef, rec *Record) (bool, error) {
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

// getPKFromSecKey extracts the primary keys the seconday index key
func getPKFromSecKey(tdef *TableDef, key []byte, idx int) *Record {
	// ignore the prefix
	key = key[4:]

	pks := make([][]byte, 0)
	bytePtr := 0
	valueTypes := make([]uint32, 0)
	for _, col := range tdef.Indexes[idx] {
		for i, c := range tdef.Cols {
			if col == c {
				valueTypes = append(valueTypes, tdef.Types[i])
				break
			}
		}
	}

	// skip seconday indexes
	for _, keyType := range valueTypes {
		switch keyType {
		case TYPE_BYTES:
			for key[bytePtr] != byte(0x00) {
				bytePtr++
			}
			// skip the delimiter 0x00
			bytePtr++
		case TYPE_INT64:
			bytePtr += 8 // size of int64
		default:
			panic("invalid key type")
		}
	}

	// extract primary keys
	for _, keyType := range tdef.Types[:tdef.Pkeys] {
		pk := make([]byte, 0)
		switch keyType {
		case TYPE_BYTES:
			for key[bytePtr] != byte(0x00) {
				pk = append(pk, key[bytePtr])
				bytePtr++
			}
			// skip the delimiter 0x00
			bytePtr++
		case TYPE_INT64:
			pk = append(pk, key[bytePtr:bytePtr+8]...)
			bytePtr += 8 // size of int64
		default:
			panic("invalid key type")
		}
		pks = append(pks, pk)
	}

	// keys are still in the encoded form
	// decode keys
	rec := &Record{}
	for i, keyType := range tdef.Types[:tdef.Pkeys] {
		switch keyType {
		case TYPE_INT64:
			val := deserializeInt(pks[i])
			rec.AddI64(tdef.Cols[i], val)
		case TYPE_BYTES:
			val := deserializeBytes(pks[i])
			rec.AddStr(tdef.Cols[i], val)
		}
	}
	return rec
}

// getTableDef gets and returns the table defination
func getTableDef(db *DBTX, table string) (*TableDef, error) {
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
