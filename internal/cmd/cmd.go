package cmd

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mauri870/kvd/internal/kvstore"
	"github.com/mauri870/kvd/internal/raftstore"
	"github.com/mauri870/kvd/internal/respserver"
)

func Run(ctx context.Context, logLvl slog.Level, rawArgs []string) error {
	slog.SetLogLoggerLevel(logLvl)
	slog.Info("Log level set", "level", logLvl)

	// flag parsing
	var addr string
	var raftAddr string
	var inmem bool
	flag.StringVar(&addr, "addr", "localhost:6379", "RESP server address")
	flag.StringVar(&raftAddr, "raft-addr", "localhost:19000", "Raft server address")
	flag.BoolVar(&inmem, "inmem", false, "use in-memory storage for Raft")
	flag.CommandLine.Parse(rawArgs)

	// get the remaining arguments
	args := flag.Args()

	// directory to store raft data, logs, and snapshots
	if len(args[0]) < 1 {
		return fmt.Errorf("missing raft store directory")
	}
	raftDir := args[0]
	err := os.Mkdir(raftDir, 0o700)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("failed to create raft store directory: %w", err)
	}

	// create the kv store
	kv, err := kvstore.New(raftDir + "/kv.db")
	if err != nil {
		return fmt.Errorf("failed to create kv store: %w", err)
	}

	slog.Info("Initializing Raft store", "dir", raftDir, "addr", raftAddr, "inmem", inmem)
	store := raftstore.New(kv, raftDir, raftAddr, inmem, os.Stderr)
	if err := store.Open(true, raftDir); err != nil {
		return fmt.Errorf("failed to open raft store: %w", err)
	}

	server, err := respserver.New(store)
	if err != nil {
		return fmt.Errorf("failed to create RESP server: %w", err)
	}

	slog.Info("Starting RESP server", "addr", addr)
	server.Run(ctx, addr, 5*time.Second)

	return nil
}
