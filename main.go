package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"github.com/mauri870/kvd/internal/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	if err := cmd.Run(ctx, os.Args[1:]); err != nil {
		slog.Error("failed to run command", "error", err)
		os.Exit(1)
	}
}
