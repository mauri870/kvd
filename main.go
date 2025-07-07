package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/mauri870/kvd/internal/cmd"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	loggerCfg := zap.NewProductionConfig()
	loggerCfg.Level.SetLevel(zap.InfoLevel)

	logger, err := loggerCfg.Build()
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}
	logger.Info("Log level set", zap.String("level", loggerCfg.Level.String()))

	if err := cmd.Run(ctx, logger, os.Args[1:]); err != nil {
		return fmt.Errorf("failed to run: %w", err)
	}

	return nil
}
