package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
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
	var cpuprofile, memprofile string
	var inmem bool
	flag.StringVar(&addr, "addr", "localhost:6379", "RESP server address")
	flag.StringVar(&httpAddr, "http-addr", "localhost:8080", "HTTP server address")
	flag.StringVar(&raftAddr, "raft-addr", "localhost:19000", "Raft server address")
	flag.BoolVar(&inmem, "inmem", false, "use in-memory storage for Raft")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to file")
	flag.StringVar(&memprofile, "memprofile", "", "write heap memory profile to file")
	flag.CommandLine.Parse(rawArgs)

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			return fmt.Errorf("failed to create cpu profile file: %w", err)
		}
		defer f.Close()

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		logger.Info("CPU profiling enabled", zap.String("file", cpuprofile))
	}

	defer func() {
		if memprofile == "" {
			return
		}

		f, err := os.Create(memprofile)
		if err != nil {
			logger.Error("Failed to create memory profile file", zap.Error(err))
			return
		}
		defer f.Close()
		pprof.WriteHeapProfile(f)
		logger.Info("Memory profiling enabled", zap.String("file", memprofile))
	}()

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
