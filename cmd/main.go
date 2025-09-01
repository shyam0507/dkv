package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/shyam0507/dkv/internal"
)

var ra *raft.Raft
var fsm *internal.FSM

type config struct {
	id       string
	httpPort string
	raftPort string
}

func getConfig() config {
	cfg := config{}
	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			continue
		}

		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			continue
		}

		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	return cfg
}

func main() {
	fmt.Println("Starting the key value store server...")

	config := getConfig()

	ra, fsm = internal.NewNode(config.id, config.raftPort)

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {

		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		ra.ApplyLog(raft.Log{Type: raft.LogCommand, Data: []byte(key + " " + value)}, time.Second*5)

		fmt.Fprintf(w, "Key %s set to %s", key, value)
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {

		key := r.URL.Query().Get("key")

		value, _ := fsm.Get(key)

		fmt.Fprintf(w, "Key %s set to %s", key, value)
		w.Write([]byte(value))
	})

	http.ListenAndServe(":"+config.httpPort, nil)

	fmt.Println("http server started at 8080")

}
