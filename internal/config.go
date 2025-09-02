package internal

import (
	"flag"
	"log/slog"
)

type NodeConfig struct {
	Id       string
	HttpPort string
	RaftPort string
}

func GetConfig() (NodeConfig, string) {
	cfg := NodeConfig{}
	var joinCluster string

	flag.StringVar(&cfg.Id, "id", "", "node id")
	flag.StringVar(&cfg.HttpPort, "http-port", "", "http port")
	flag.StringVar(&cfg.RaftPort, "raft-port", "", "raft port")

	flag.StringVar(&joinCluster, "join", "", "join existing cluster")

	flag.Parse()

	if cfg.Id == "" || cfg.HttpPort == "" || cfg.RaftPort == "" {
		flag.Usage()
		panic("Missing required flags")
	}

	slog.Info("Node configuration", "id", cfg.Id, "http-port", cfg.HttpPort, "raft-port", cfg.RaftPort)

	return cfg, joinCluster
}
