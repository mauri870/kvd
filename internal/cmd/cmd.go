package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/mauri870/kvd/internal/httpserver"
	"github.com/mauri870/kvd/internal/kvstore"
	"github.com/mauri870/kvd/internal/raftstore"
	"github.com/mauri870/kvd/internal/respserver"
	"go.uber.org/zap"
)

func Run(ctx context.Context, logger *zap.Logger, rawArgs []string) error {
	// flag parsing
	var addr string
	var httpAddr string
	var raftAddr string
	var inmem bool
	flag.StringVar(&addr, "addr", "localhost:6379", "RESP server address")
	flag.StringVar(&httpAddr, "http-addr", "localhost:8080", "HTTP server address")
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

	logger.Info("Initializing Raft store", zap.String("dir", raftDir), zap.String("addr", raftAddr), zap.Bool("inmem", inmem))
	store := raftstore.New(kv, raftDir, raftAddr, inmem, os.Stderr, logger)
	if err := store.Open(true, raftDir); err != nil {
		return fmt.Errorf("failed to open raft store: %w", err)
	}

	server, err := respserver.New(store, logger)
	if err != nil {
		return fmt.Errorf("failed to create RESP server: %w", err)
	}

	logger.Info("Starting RESP server", zap.String("addr", addr))
	go server.Run(ctx, addr, 5*time.Second)

	httpServer, err := httpserver.New(store, logger)
	if err != nil {
		return fmt.Errorf("failed to create HTTP server: %w", err)
	}

	logger.Info("Starting HTTP server", zap.String("addr", httpAddr))
	httpServer.Run(ctx, httpAddr)

	return nil
}
