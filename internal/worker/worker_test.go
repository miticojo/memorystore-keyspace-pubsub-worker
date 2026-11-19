package worker

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type mockPublisher struct {
	mu     sync.Mutex
	events []KeyspaceEvent
	notify chan KeyspaceEvent
}

func newMockPublisher() *mockPublisher {
	return &mockPublisher{
		notify: make(chan KeyspaceEvent, 16),
	}
}

func (m *mockPublisher) Publish(_ context.Context, event KeyspaceEvent) (string, error) {
	m.mu.Lock()
	m.events = append(m.events, event)
	m.mu.Unlock()
	select {
	case m.notify <- event:
	default:
	}
	return "mock-msg-id", nil
}

func (m *mockPublisher) Close() error { return nil }

func TestParseKeyspaceMessage(t *testing.T) {
	now := time.Date(2026, 11, 19, 15, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		channel   string
		payload   string
		wantEvent string
		wantKey   string
		wantDB    int
	}{
		{
			name:      "keyevent expired",
			channel:   "__keyevent@0__:expired",
			payload:   "session:user-42",
			wantEvent: "expired",
			wantKey:   "session:user-42",
			wantDB:    0,
		},
		{
			name:      "keyspace del on db 2",
			channel:   "__keyspace@2__:cart:99",
			payload:   "del",
			wantEvent: "del",
			wantKey:   "cart:99",
			wantDB:    2,
		},
		{
			name:      "fallback channel without colon",
			channel:   "custom-channel",
			payload:   "ping",
			wantEvent: "ping",
			wantKey:   "custom-channel",
			wantDB:    -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseKeyspaceMessage(tc.channel, tc.payload, "10.0.0.5:6379", now)
			if got.EventType != tc.wantEvent {
				t.Errorf("EventType = %q, want %q", got.EventType, tc.wantEvent)
			}
			if got.Key != tc.wantKey {
				t.Errorf("Key = %q, want %q", got.Key, tc.wantKey)
			}
			if got.DB != tc.wantDB {
				t.Errorf("DB = %d, want %d", got.DB, tc.wantDB)
			}
			if got.ShardAddr != "10.0.0.5:6379" {
				t.Errorf("ShardAddr = %q, want 10.0.0.5:6379", got.ShardAddr)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	var empty Config
	if err := empty.Validate(); err == nil {
		t.Fatal("expected error when Addr is empty")
	}

	badMode := Config{Addr: "localhost:6379", Mode: "invalid"}
	if err := badMode.Validate(); err == nil {
		t.Fatal("expected error for invalid mode")
	}

	valid := Config{Addr: "localhost:6379"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if valid.Mode != ModeStandalone || valid.KeyspacePattern != "__keyevent@*__:*" {
		t.Fatalf("defaults not applied: %+v", valid)
	}
}

func TestRunStandaloneReceivesKeyspaceEvents(t *testing.T) {
	mr := miniredis.RunT(t)
	admin := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = admin.Close() }()

	pub := newMockPublisher()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{
		Addr:            mr.Addr(),
		Mode:            ModeStandalone,
		KeyspacePattern: "__keyevent@*__:*",
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cfg, pub, slog.Default())
	}()

	// Wait until PSubscribe is registered on miniredis.
	deadlineSub := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadlineSub) {
		n, err := admin.Publish(ctx, "__keyevent@0__:expired", "session:abc").Result()
		if err == nil && n > 0 {
			break
		}
		select {
		case runErr := <-errCh:
			t.Fatalf("Run exited prematurely: %v", runErr)
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}

	deadline := time.After(3 * time.Second)
	var sawExpired bool
	for !sawExpired {
		select {
		case runErr := <-errCh:
			t.Fatalf("Run exited prematurely: %v", runErr)
		case ev := <-pub.notify:
			if ev.Key == "session:abc" && ev.EventType == "expired" && ev.DB == 0 {
				sawExpired = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for expired event; received=%v", pub.events)
		}
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned unexpected error on shutdown: %v", err)
	}
}
