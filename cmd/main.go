package main

import (
	"dbms/db"
	"dbms/kv"
	"fmt"
	"log"
	"strings"
)

func main() {
	loc := "./temp.db"
	kvstore, err := kv.NewKv(loc)
	if err != nil {
		log.Fatalf("creating kvstore: %s", err.Error())
	}
	defer kvstore.Close()
	database := db.NewDB(loc, kvstore)

	// new table defination
	tdef := &db.TableDef{
		Name:  "demo",
		Cols:  []string{"id", "data"},
		Types: []uint32{db.TYPE_INT64, db.TYPE_BYTES},
		Pkeys: 1,
	}

	err = database.TableNew(tdef)
	if err != nil {
		log.Fatalf("creating new table: %s", err.Error())
	}

	// create records
	records := make([]db.Record, 0)
	for i := 0; i < 100; i++ {
		rec := &db.Record{}
		rec.AddI64("id", int64(i))
		val := make([]byte, 200)
		// creating a value of size 200 bytes
		// this is done to make sure multiple nodes are created
		copy(val, []byte(fmt.Sprintf("some temp data: %d", i)))
		rec.AddStr("data", val)
		records = append(records, *rec)
	}

	// insert records
	for i, rec := range records {
		fmt.Println("inserting row:", i)
		database.Insert("demo", rec)
	}

	// scan though 100 values
	startRec := &db.Record{}
	startRec.AddI64("id", 50)

	endRec := &db.Record{}
	endRec.AddI64("id", 60)

	// scanner to scan rows from start record to end record
	scanner := database.NewScanner(*tdef, *startRec, *endRec, 0)

	fmt.Println("scanning values")
	// scanning rows
	for scanner.Valid() {
		rec, err := scanner.Deref()
		if err != nil {
			log.Fatalf("derefercing row: %s", err.Error())
		}
		data := string(rec.Get("data").Str)
		data = strings.TrimRight(data, string([]byte{0x00}))
		fmt.Println(data)
		scanner.Next()
	}
}
