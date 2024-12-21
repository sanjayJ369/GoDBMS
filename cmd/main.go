package main

import (
	"dbms/db"
	"dbms/kv"
	"dbms/util"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
)

var tdef = &db.TableDef{
	Name:    "demo",
	Cols:    []string{"id", "number", "data"},
	Types:   []uint32{db.TYPE_INT64, db.TYPE_INT64, db.TYPE_BYTES},
	Pkeys:   1,
	Indexes: [][]string{{"id"}, {"number"}},
}

func main() {

	var wg sync.WaitGroup
	// concurrent transactions demo
	loc := util.NewTempFileLoc()
	kvstore, err := kv.NewKv(loc)
	if err != nil {
		log.Fatalf("creating kvstore: %s", err.Error())
	}
	defer kvstore.Close()

	database := db.NewDB(loc, kvstore)
	records := insertRecords(database)

	// reading  trsaction
	wg.Add(1)
	go func() {
		readtx := database.NewTX()
		database.Begin(readtx)
		// read values
		for _, r := range records {
			rec := &db.Record{}
			rec.AddI64("id", r.Get("id").I64)
			_, err := readtx.Get(tdef.Name, rec)
			if err != nil {
				fmt.Printf("getting record: %s", err)
			}
			fmt.Println("reading transaction: ", "id: ", rec.Get("id").I64, " number: ", rec.Get("number").I64)
		}
		database.Commit(readtx)
		wg.Done()
	}()

	// write trsaction
	wg.Add(1)
	go func() {
		writetx := database.NewTX()
		database.Begin(writetx)
		// read values
		for _, r := range records {
			rec := &db.Record{}
			rec.AddI64("id", r.Get("id").I64)
			writetx.Delete(tdef.Name, r)
			fmt.Println("writed transaction: ", "id ", r.Get("id").I64, " deleted")
		}
		database.Commit(writetx)
		wg.Done()
	}()

	wg.Wait()
}

// inserted records format
// | id (int64) | number (int64) | data (string) |
func insertRecords(database *db.DB) []db.Record {
	tx := database.NewTX()

	// begin transaction
	database.Begin(tx)
	err := tx.TableNew(tdef)
	if err != nil {
		log.Fatalf("creating new table: %s", err.Error())
	}

	// create records
	records := make([]db.Record, 0)
	for i := 0; i < 100; i++ {
		rec := &db.Record{}
		rec.AddI64("id", int64(i))
		rec.AddI64("number", int64(rand.IntN(1000)))
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
		tx.Insert("demo", rec)
	}
	database.Commit(tx)
	return records
}

func iteratorDemo() {
	loc := util.NewTempFileLoc()
	kvstore, err := kv.NewKv(loc)
	if err != nil {
		log.Fatalf("creating kvstore: %s", err.Error())
	}
	defer kvstore.Close()

	database := db.NewDB(loc, kvstore)
	tx := database.NewTX()

	records := insertRecords(database)

	// scan though 100 values
	startRec := &db.Record{}
	startRec.AddI64("id", records[50].Vals[0].I64)     // id
	startRec.AddI64("number", records[50].Vals[1].I64) // number

	endRec := &db.Record{}
	endRec.AddI64("id", records[60].Vals[0].I64)     // id
	endRec.AddI64("number", records[60].Vals[1].I64) // number

	// scanner to scan rows from start record to end record
	scanner := tx.NewScanner(*tdef, *startRec, *endRec, 0)

	fmt.Println("\n\nscanning though primary index:")
	// scanning rows
	for scanner.Valid() {
		rec, err := scanner.Deref()
		if err != nil {
			log.Fatalf("derefercing row: %s", err.Error())
		}
		fmt.Println("id: ", rec.Get("id").I64, " : ", "number: ", rec.Get("number").I64)
		scanner.Next()
	}

	scanner = tx.NewScanner(*tdef, *startRec, *endRec, 1)

	fmt.Println("\n\nscanning though seconday index:")
	// scanning rows
	for scanner.Valid() {
		rec, err := scanner.Deref()
		if err != nil {
			log.Fatalf("derefercing row: %s", err.Error())
		}
		fmt.Println("id: ", rec.Get("id").I64, " : ", "number: ", rec.Get("number").I64)
		scanner.Next()
	}
	// commit transaction
	database.Commit(tx)
}
