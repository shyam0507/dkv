package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/hashicorp/raft"
)

type SetPayload struct {
	Key   string
	Value string
}

type GetPayload struct {
	Key string
}

type FSM struct {
	kv *sync.Map
}

func (fsm *FSM) Get(key string) (string, error) {
	value, _ := fsm.kv.Load(key)
	return value.(string), nil
}

// raft interface
func (fsm *FSM) Apply(log *raft.Log) any {

	switch log.Type {
	case raft.LogCommand:
		var sp SetPayload
		err := json.Unmarshal(log.Data, &sp)

		if err != nil {
			fmt.Println("Error unmarshaling log data:", err)
			return err
		}
		fsm.kv.Store(sp.Key, sp.Value)

	default:
		slog.Error("Unexpected log type", "type", log.Type)
		return fmt.Errorf("unexpected log type: %v", log.Type)
	}

	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release()                          {}

func (n *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (n *FSM) Restore(rc io.ReadCloser) error {
	// deleting first isn't really necessary since there's no exposed DELETE operation anyway.
	// so any changes over time will just get naturally overwritten

	decoder := json.NewDecoder(rc)

	for decoder.More() {
		var sp SetPayload
		err := decoder.Decode(&sp)
		if err != nil {
			return fmt.Errorf("could not decode payload: %s", err)
		}

		n.kv.Store(sp.Key, sp.Value)
	}

	return rc.Close()
}
