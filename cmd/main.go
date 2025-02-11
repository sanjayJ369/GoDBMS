package main

import (
	"bufio"
	"dbms/db"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	var database *db.DB
	var close func()

	for {
		input := getQueryInput()
		inputArr := strings.Split(input, " ")
		switch inputArr[0] {
		case "open":
			name := strings.Trim(inputArr[1], ";")
			fmt.Println("opening db: ", name)
			database, close = db.OpenDB(name)
		case "exit;":
			fmt.Println("exiting")
			close()
			os.Exit(1)
		default:
			execQuery(input, database)
		}
	}
}

func execQuery(query string, database *db.DB) error {
	res, err := db.ExecuteQuery(query, database)
	if err != nil {
		return err
	}

	switch val := res.(type) {
	case []db.Record:
		printRecords(val)
	case int:
		fmt.Printf("\ndeleted:%d record(s)\n", val)
	}
	return nil
}

func printRecords(recs []db.Record) {

	if len(recs) <= 0 {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(recs[0].Cols)

	for _, r := range recs {
		table.Append(r.ToString())
	}

	table.Render()
}

func getQueryInput() string {
	res := ""
	for {
		fmt.Print(">> ")
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
		}
		res += " " + line
		if len(line) > 2 && line[len(line)-2] == ';' {
			break
		}
	}
	return strings.Trim(res, " \n")
}
