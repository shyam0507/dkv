package internal

import (
	"fmt"
	"strings"
)

type IMemTable interface {
	Add(key, value string) error
	Get(key string) (string, error)
	Flush() error
	Delete(key, value string) error
	Rebuild(data []byte) error
}

type ValueStore struct {
	value     string
	isDeleted byte
}

type memTable struct {
	store map[string]ValueStore
}

func NewMemTable() IMemTable {
	return &memTable{store: make(map[string]ValueStore)}
}

func (m *memTable) Add(key, value string) error {
	m.store[key] = ValueStore{value: value}
	return nil
}

func (m *memTable) Get(key string) (string, error) {
	val, exists := m.store[key]
	if !exists {
		return "", fmt.Errorf("key %s does not exist", key)
	}
	return val.value, nil
}

func (m *memTable) Flush() error {
	return nil
}

func (m *memTable) Delete(key, value string) error {
	m.store[key] = ValueStore{value: value, isDeleted: 1}
	return nil
}

func (m *memTable) Rebuild(data []byte) error {
	walData := strings.Split(string(data), "\n")
	for _, v := range walData {
		if v == "" {
			continue
		}
		kv := strings.Split(v, " ")
		if len(kv) == 3 && kv[2] != "1" {
			return fmt.Errorf("invalid data in wal")
		} else if len(kv) != 2 {
			return fmt.Errorf("invalid data in wal")
		}

		// No delete marker
		if len(kv) == 2 {
			m.store[kv[0]] = ValueStore{value: kv[1]}
		} else {
			m.store[kv[0]] = ValueStore{value: kv[1], isDeleted: 1}
		}
	}
	return nil
}
