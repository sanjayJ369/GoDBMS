package db

import (
	"dbms/kv"
	"log"
)

// OpenDB open db, creats one if it does not exist
func OpenDB(loc string) (*DB, func()) {
	kvstore, err := kv.NewKv(loc)
	if err != nil {
		log.Fatalf("creating kvstore: %s", err.Error())
	}
	database := NewDB(loc, kvstore)
	return database, kvstore.Close
}

// ExecuteQuery parser the query and executes the query
func ExecuteQuery(query string, db *DB) (interface{}, error) {
	// start a transaction
	tx := db.NewTX()
	p := &Parser{
		input: []byte(query),
	}
	stmt := pStmt(p)
	db.Begin(tx)
	res, err := qlStmt(p, stmt, tx)
	db.Commit(tx)
	return res, err
}
