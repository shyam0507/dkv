package main

import (
	"fmt"
	"os"

	"github.com/shyam0507/dkv/internal"
)

var memTable internal.IMemTable
var wal internal.IWal

func init() {
	fmt.Println("Initializing the key value store server...")
	memTable = internal.NewMemTable()
	wal = internal.NewWal("wal.txt")

	// Check if the wal has any data
	// If yes, load it into the memtable
	// If no, do nothing
	fmt.Println("Loading the wal into the memtable...")
	data, err := wal.Get()
	if err != nil {
		fmt.Println(err)
	}
	err = memTable.Rebuild(*data)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Initialization Complete...")

}

func main() {
	fmt.Println("Starting the key value store server...")

	fmt.Println(os.Args[0])
	fmt.Println(os.Args[1])

	if os.Args[1] == "put" {
		putKey(os.Args[2], os.Args[3])
		getKey(os.Args[2])
	} else if os.Args[1] == "get" {
		getKey(os.Args[2])
	}

}

func putKey(key string, value string) {
	wal.Put(key, value)
	memTable.Add(key, value)
}

func getKey(key string) {
	fmt.Println(memTable.Get(key))
}
