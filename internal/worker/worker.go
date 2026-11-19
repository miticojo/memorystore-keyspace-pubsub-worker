package worker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/redis/go-redis/v9"
)

// Mode selects the Memorystore topology.
type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeCluster    Mode = "cluster"
)

// Config holds the runtime configuration for the keyspace-to-Pub/Sub worker.
type Config struct {
	Addr                     string
	Mode                     Mode
	Username                 string
	Password                 string
	TLSEnabled               bool
	KeyspacePattern          string
	AutoConfigureEvents      string
	PubSubProjectID          string
	PubSubTopicID            string
	PublishTimeout           time.Duration
	ReconnectInitialBackoff  time.Duration
	ReconnectMaxBackoff      time.Duration
}

// Validate checks required configuration fields and applies defaults.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return errors.New("MEMORYSTORE_ADDR is required")
	}
	if c.Mode == "" {
		c.Mode = ModeStandalone
	}
	if c.Mode != ModeStandalone && c.Mode != ModeCluster {
		return fmt.Errorf("invalid MEMORYSTORE_MODE %q: must be 'standalone' or 'cluster'", c.Mode)
	}
	if strings.TrimSpace(c.KeyspacePattern) == "" {
		c.KeyspacePattern = "__keyevent@*__:*"
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	if c.ReconnectInitialBackoff <= 0 {
		c.ReconnectInitialBackoff = 1 * time.Second
	}
	if c.ReconnectMaxBackoff <= 0 {
		c.ReconnectMaxBackoff = 30 * time.Second
	}
	return nil
}

// KeyspaceEvent is the normalized event payload published to Cloud Pub/Sub.
type KeyspaceEvent struct {
	Channel    string    `json:"channel"`
	EventType  string    `json:"event_type"`
	Key        string    `json:"key"`
	DB         int       `json:"db"`
	ShardAddr  string    `json:"shard_addr"`
	ReceivedAt time.Time `json:"received_at"`
}

// Publisher abstracts Cloud Pub/Sub publishing for unit testing.
type Publisher interface {
	Publish(ctx context.Context, event KeyspaceEvent) (string, error)
	Close() error
}

// PubSubPublisher publishes KeyspaceEvent JSON messages to Google Cloud Pub/Sub.
type PubSubPublisher struct {
	client    *pubsub.Client
	publisher *pubsub.Publisher
	timeout   time.Duration
}

// NewPubSubPublisher creates a PubSubPublisher for the given project and topic.
func NewPubSubPublisher(ctx context.Context, projectID, topicID string, timeout time.Duration) (*PubSubPublisher, error) {
	if strings.TrimSpace(projectID) == "" || strings.TrimSpace(topicID) == "" {
		return nil, errors.New("PUBSUB_PROJECT_ID and PUBSUB_TOPIC_ID are required")
	}
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create pubsub client: %w", err)
	}
	return &PubSubPublisher{
		client:    client,
		publisher: client.Publisher(topicID),
		timeout:   timeout,
	}, nil
}

// Publish serializes the event to JSON and sends it to Cloud Pub/Sub with routing attributes.
func (p *PubSubPublisher) Publish(ctx context.Context, event KeyspaceEvent) (string, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("marshal event: %w", err)
	}
	pubCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	res := p.publisher.Publish(pubCtx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"source":     "memorystore",
			"event_type": event.EventType,
			"db":         strconv.Itoa(event.DB),
		},
	})
	return res.Get(pubCtx)
}

// Close flushes pending messages and closes the underlying Pub/Sub client.
func (p *PubSubPublisher) Close() error {
	p.publisher.Stop()
	return p.client.Close()
}

// ParseKeyspaceMessage parses __keyevent@<db>__:<event> or __keyspace@<db>__:<key> messages.
func ParseKeyspaceMessage(channel, payload, shardAddr string, now time.Time) KeyspaceEvent {
	ev := KeyspaceEvent{
		Channel:    channel,
		ShardAddr:  shardAddr,
		ReceivedAt: now.UTC(),
		DB:         -1,
	}

	prefix, suffix, hasSep := strings.Cut(channel, ":")
	if !hasSep {
		ev.EventType = payload
		ev.Key = channel
		return ev
	}

	if atIdx := strings.Index(prefix, "@"); atIdx != -1 {
		dbStr := strings.TrimSuffix(prefix[atIdx+1:], "__")
		if db, err := strconv.Atoi(dbStr); err == nil {
			ev.DB = db
		}
	}

	switch {
	case strings.HasPrefix(prefix, "__keyevent@"):
		ev.EventType = suffix
		ev.Key = payload
	case strings.HasPrefix(prefix, "__keyspace@"):
		ev.Key = suffix
		ev.EventType = payload
	default:
		ev.EventType = payload
		ev.Key = suffix
	}
	return ev
}

// Run connects to Memorystore (standalone or cluster), subscribes to keyspace notifications,
// and forwards events to pub until ctx is cancelled.
func Run(ctx context.Context, cfg Config, pub Publisher, logger *slog.Logger) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}

	var tlsCfg *tls.Config
	if cfg.TLSEnabled {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	if cfg.Mode == ModeCluster {
		return runCluster(ctx, cfg, tlsCfg, pub, logger)
	}
	return runStandalone(ctx, cfg, tlsCfg, pub, logger)
}

func runStandalone(ctx context.Context, cfg Config, tlsCfg *tls.Config, pub Publisher, logger *slog.Logger) error {
	client := redis.NewClient(&redis.Options{
		Addr:      cfg.Addr,
		Username:  cfg.Username,
		Password:  cfg.Password,
		TLSConfig: tlsCfg,
	})
	defer func() { _ = client.Close() }()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping memorystore standalone %s: %w", cfg.Addr, err)
	}

	if cfg.AutoConfigureEvents != "" {
		if err := client.ConfigSet(ctx, "notify-keyspace-events", cfg.AutoConfigureEvents).Err(); err != nil {
			return fmt.Errorf("set notify-keyspace-events=%q: %w", cfg.AutoConfigureEvents, err)
		}
		logger.Info("configured notify-keyspace-events", "addr", cfg.Addr, "value", cfg.AutoConfigureEvents)
	}

	return subscribeLoop(ctx, client, cfg.Addr, cfg, pub, logger)
}

func runCluster(ctx context.Context, cfg Config, tlsCfg *tls.Config, pub Publisher, logger *slog.Logger) error {
	cluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:     []string{cfg.Addr},
		Username:  cfg.Username,
		Password:  cfg.Password,
		TLSConfig: tlsCfg,
	})
	defer func() { _ = cluster.Close() }()

	if err := cluster.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping memorystore cluster %s: %w", cfg.Addr, err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 16)

	// Keyspace notifications in Cluster Mode are node-local and not broadcast across shards,
	// so we must subscribe to every primary shard individually.
	err := cluster.ForEachMaster(ctx, func(ctx context.Context, shard *redis.Client) error {
		shardAddr := shard.Options().Addr
		if cfg.AutoConfigureEvents != "" {
			if err := shard.ConfigSet(ctx, "notify-keyspace-events", cfg.AutoConfigureEvents).Err(); err != nil {
				return fmt.Errorf("set notify-keyspace-events on shard %s: %w", shardAddr, err)
			}
			logger.Info("configured notify-keyspace-events on shard", "shard", shardAddr, "value", cfg.AutoConfigureEvents)
		}

		wg.Add(1)
		go func(c *redis.Client, addr string) {
			defer wg.Done()
			if subErr := subscribeLoop(ctx, c, addr, cfg, pub, logger); subErr != nil && ctx.Err() == nil {
				select {
				case errCh <- fmt.Errorf("shard %s: %w", addr, subErr):
				default:
				}
			}
		}(shard, shardAddr)
		return nil
	})
	if err != nil {
		return fmt.Errorf("discover cluster primary shards: %w", err)
	}

	wg.Wait()
	close(errCh)
	if subErr, ok := <-errCh; ok {
		return subErr
	}
	return nil
}

func subscribeLoop(ctx context.Context, client *redis.Client, shardAddr string, cfg Config, pub Publisher, logger *slog.Logger) error {
	backoff := cfg.ReconnectInitialBackoff

	for {
		if ctx.Err() != nil {
			return nil
		}

		err := consumeOnce(ctx, client, shardAddr, cfg.KeyspacePattern, pub, logger)
		if ctx.Err() != nil {
			return nil
		}

		logger.Warn("keyspace subscription interrupted, reconnecting",
			"shard", shardAddr,
			"error", err,
			"backoff", backoff.String(),
		)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}

		backoff *= 2
		if backoff > cfg.ReconnectMaxBackoff {
			backoff = cfg.ReconnectMaxBackoff
		}
	}
}

func consumeOnce(ctx context.Context, client *redis.Client, shardAddr, pattern string, pub Publisher, logger *slog.Logger) error {
	pubsubConn := client.PSubscribe(ctx, pattern)
	defer func() { _ = pubsubConn.Close() }()

	if _, err := pubsubConn.Receive(ctx); err != nil {
		return fmt.Errorf("psubscribe %q: %w", pattern, err)
	}

	logger.Info("subscribed to memorystore keyspace notifications",
		"shard", shardAddr,
		"pattern", pattern,
	)

	ch := pubsubConn.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return errors.New("pubsub channel closed")
			}
			event := ParseKeyspaceMessage(msg.Channel, msg.Payload, shardAddr, time.Now())
			msgID, err := pub.Publish(ctx, event)
			if err != nil {
				logger.Error("failed to publish keyspace event to pubsub",
					"shard", shardAddr,
					"channel", event.Channel,
					"key", event.Key,
					"event_type", event.EventType,
					"error", err,
				)
				continue
			}
			logger.Debug("published keyspace event",
				"msg_id", msgID,
				"shard", shardAddr,
				"event_type", event.EventType,
				"key", event.Key,
				"db", event.DB,
			)
		}
	}
}
