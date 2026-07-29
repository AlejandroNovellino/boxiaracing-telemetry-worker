package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/config"
	kafkaclient "github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/kafka"
	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/transform"
	"github.com/AlejandroNovellino/boxiaracing-telemetry-worker/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	client, err := kafkaclient.New(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer client.Close()

	runner := worker.New(
		client,
		client,
		transform.JSONPassthrough{},
		worker.Options{
			OutputTopic: cfg.Kafka.OutputTopic,
			DLQTopic:    cfg.Kafka.DLQTopic,
			Logger:      logger,
		},
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"telemetry worker starting",
		"input_topic", cfg.Kafka.InputTopic,
		"output_topic", cfg.Kafka.OutputTopic,
		"dlq_topic", cfg.Kafka.DLQTopic,
		"consumer_group", cfg.Kafka.ConsumerGroup,
	)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}

	logger.Info("telemetry worker stopped")
	return nil
}
