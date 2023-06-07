package internal

import (
	"fmt"
	"os"
)

type IWal interface {
	Put(string, string) error
	Delete(string, string) error
	Get() (*[]byte, error)
}

type wal struct {
	filename string
}

func NewWal(filename string) IWal {
	return &wal{
		filename: filename,
	}
}

func (w *wal) Put(key string, value string) error {
	appendStr := []byte(key + " " + value + "\n")
	f, err := os.OpenFile(w.filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f.Close()
	n, err := f.Write(appendStr)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("wrote %d bytes\n", n)
	return nil
}

func (w *wal) Delete(key string, value string) error {
	appendStr := []byte(key + " " + value + " 1 \n")
	f, err := os.OpenFile(w.filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f.Close()
	n, err := f.Write(appendStr)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("wrote %d bytes\n", n)
	return nil
}

func (w *wal) Get() (*[]byte, error) {
	f, err := os.ReadFile(w.filename)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return &f, nil
}
