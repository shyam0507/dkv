package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shyam0507/dkv/internal"
)

var ra *raft.Raft
var fsm *internal.FSM

func main() {
	fmt.Println("Starting the key value store server...")

	config, masterAddr := internal.GetConfig()

	fmt.Println("Master address:", masterAddr)

	ra, fsm = internal.NewNode(config.Id, config.RaftPort, masterAddr)

	http.HandleFunc("/join", func(w http.ResponseWriter, r *http.Request) {

		joinAddr := r.URL.Query().Get("addr")
		nodeId := r.URL.Query().Get("id")

		slog.Info("Joining node at ", "Addr", joinAddr)
		err := ra.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(joinAddr), 0, 0).Error()
		if err != nil {
			slog.Info("Failed to add follower", "error", err)
		}

		slog.Info("Node joined", "NodeID", nodeId, "Address", joinAddr)
	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {

		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		p := internal.SetPayload{
			Key:   key,
			Value: value,
		}

		data, err := json.Marshal(p)
		if err != nil {
			slog.Error("Failed to marshal set payload", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		ra.ApplyLog(raft.Log{Type: raft.LogCommand, Data: data}, time.Second*5)

		slog.Info("Key set", "key", key, "value", value)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {

		key := r.URL.Query().Get("key")

		value, _ := fsm.Get(key)

		// fmt.Fprintf(w, "Key %s set to %s", key, value)
		w.Write([]byte(value))
	})

	http.ListenAndServe(":"+config.HttpPort, nil)

	fmt.Println("http server started at 8080")

}
