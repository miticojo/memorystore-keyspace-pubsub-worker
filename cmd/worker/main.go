package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/miticojo/memorystore-keyspace-pubsub-worker/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLogLevel(os.Getenv("LOG_LEVEL")),
	}))

	cfg := worker.Config{
		Addr:                os.Getenv("MEMORYSTORE_ADDR"),
		Mode:                worker.Mode(strings.ToLower(strings.TrimSpace(os.Getenv("MEMORYSTORE_MODE")))),
		Username:            os.Getenv("MEMORYSTORE_USERNAME"),
		Password:            os.Getenv("MEMORYSTORE_PASSWORD"),
		TLSEnabled:          parseBool(os.Getenv("MEMORYSTORE_TLS_ENABLED")),
		KeyspacePattern:     os.Getenv("KEYSPACE_PATTERN"),
		AutoConfigureEvents: os.Getenv("AUTO_CONFIGURE_KEYSPACE_EVENTS"),
		PubSubProjectID:     os.Getenv("PUBSUB_PROJECT_ID"),
		PubSubTopicID:       os.Getenv("PUBSUB_TOPIC_ID"),
		PublishTimeout:      parseDuration(os.Getenv("PUBLISH_TIMEOUT"), 10*time.Second),
	}

	if err := cfg.Validate(); err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pub, err := worker.NewPubSubPublisher(ctx, cfg.PubSubProjectID, cfg.PubSubTopicID, cfg.PublishTimeout)
	if err != nil {
		logger.Error("failed to initialize Pub/Sub publisher", "error", err)
		os.Exit(1)
	}
	defer func() {
		if closeErr := pub.Close(); closeErr != nil {
			logger.Warn("error closing Pub/Sub publisher", "error", closeErr)
		}
	}()

	logger.Info("starting memorystore keyspace worker",
		"addr", cfg.Addr,
		"mode", cfg.Mode,
		"pattern", cfg.KeyspacePattern,
		"pubsub_project", cfg.PubSubProjectID,
		"pubsub_topic", cfg.PubSubTopicID,
	)

	if err := worker.Run(ctx, cfg, pub, logger); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("worker stopped gracefully")
}

func parseBool(v string) bool {
	b, _ := strconv.ParseBool(strings.TrimSpace(v))
	return b
}

func parseDuration(v string, fallback time.Duration) time.Duration {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return d
}

func parseLogLevel(v string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
