package db

import (
	"encoding/json"
	"fmt"
)

const (
	TYPE_BYTES = 1
	TYPE_INT64 = 2
)

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
	Name   string   `json:"name"`   // name of the table
	Cols   []string `json:"cols"`   // colums present in the table
	Types  []uint32 `json:"types"`  // types of each column
	Pkeys  int      `json:"pkeys"`  // number of primary keys
	Prefix uint32   `json:"prefix"` // prefix corresponding to this table
}

// TDEF_TABLE is an internal table which stores each table name
// and it's corresponding defination
var TDEF_TABLE = &TableDef{
	Name:   "@table",
	Cols:   []string{"name", "def"},
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Pkeys:  1,
	Prefix: 2,
}

// TDEF_META is an internal table which stores the meta data
// requried for handling the database
var TDEF_META = &TableDef{
	Name:   "@meta",
	Cols:   []string{"key", "value"},
	Types:  []uint32{TYPE_BYTES, TYPE_BYTES},
	Pkeys:  1,
	Prefix: 1,
}

type KVStore interface {
	Close()
	Del(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	Open() error
	Set(key []byte, val []byte) error
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

// TODO: interfaces for handling single record in a table
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

// func (db *DB) Insert(table string, rec Record) (bool, error)
// func (db *DB) Update(table string, rec Record) (bool, error)
// func (db *DB) Upsert(table string, rec Record) (bool, error)
// func (db *DB) Delete(table string, rec Record) (bool, error)

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
	key := encodeKey(tdef.Prefix, values[:tdef.Pkeys])

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
	if len(rec.Cols) < n {
		return nil, fmt.Errorf("invalid number of columns in record")
	}
	return rec.Vals, nil
}

// encodeKey combines the prefix of the table
// and the primary keys of the record to get the
// record
func encodeKey(prefix uint32, pks []Value) []byte {
	key := []byte{}
	key = append(key, byte(prefix))
	for _, val := range pks {
		switch val.Type {
		case TYPE_BYTES:
			key = append(key, val.Str...)
		case TYPE_INT64:
			key = append(key, byte(val.I64))
		default:
			panic("invalid value type")
		}
	}
	return key
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

// getTableDef gets and returns the table defination
func getTableDef(db *DB, table string) (*TableDef, error) {
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
