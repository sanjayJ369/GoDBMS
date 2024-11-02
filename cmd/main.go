package main

import (
	"dbms/kv"
	"fmt"
	"log"
)

func main() {
	loc := "./temp.db"
	db, err := kv.NewKv(loc)
	if err != nil {
		log.Fatalf("opening db: %s", err.Error())
	}
	defer db.Close()

	key := []byte("hello")
	val := []byte("world")

	key1 := []byte("it")
	val1 := []byte("works, finally")

	db.Set(key, val)
	db.Set(key1, val1)

	got, err := db.Get(key)
	if err != nil {
		log.Fatalf("getting key(%s): %s", string(key), err.Error())
	}
	fmt.Println("key:", string(key), " val:", string(got))

	got, err = db.Get(key1)
	if err != nil {
		log.Fatalf("getting key(%s): %s", string(key), err.Error())
	}
	fmt.Println("key:", string(key1), " val:", string(got))
}
