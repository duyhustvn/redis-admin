package sentinel

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// SentinelEvent is a single Pub/Sub message from a Sentinel node.
type SentinelEvent struct {
	Channel    string    `json:"channel"`
	Payload    string    `json:"payload"`
	NodeAddr   string    `json:"node_addr,omitempty"`
	IsFlapping bool      `json:"is_flapping,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
}

// EventBus distributes SentinelEvents to all active SSE subscribers.
type EventBus struct {
	mu   sync.RWMutex
	subs map[chan SentinelEvent]struct{}
}

// NewEventBus creates an empty EventBus.
func NewEventBus() *EventBus {
	return &EventBus{subs: make(map[chan SentinelEvent]struct{})}
}

// Subscribe registers a new subscriber and returns its channel.
// The channel is buffered to prevent slow consumers from blocking the publisher.
func (b *EventBus) Subscribe() chan SentinelEvent {
	ch := make(chan SentinelEvent, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes the subscriber and closes its channel.
func (b *EventBus) Unsubscribe(ch chan SentinelEvent) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
	close(ch)
}

// publish sends an event to all subscribers non-blocking; slow consumers are dropped.
func (b *EventBus) publish(evt SentinelEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- evt:
		default:
		}
	}
}

// flapWindow holds recent sdown timestamps for a single node address.
type flapWindow struct {
	times []time.Time
}

// PubSubListener subscribes to all Sentinel channels on each configured sentinel
// address and forwards events to the EventBus. It also detects node flapping.
type PubSubListener struct {
	cfg    *config.Config
	bus    *EventBus
	logger *zap.Logger
}

// NewPubSubListener creates a PubSubListener.
func NewPubSubListener(cfg *config.Config, bus *EventBus, logger *zap.Logger) *PubSubListener {
	return &PubSubListener{cfg: cfg, bus: bus, logger: logger}
}

// Run starts a goroutine per sentinel address and blocks until ctx is cancelled.
// Re-connects automatically on disconnect.
func (l *PubSubListener) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, addr := range l.cfg.SentinelAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			l.listenSentinel(ctx, addr)
		}(addr)
	}
	wg.Wait()
}

// listenSentinel connects to one sentinel and reads events until ctx is done,
// reconnecting with backoff on each disconnect.
func (l *PubSubListener) listenSentinel(ctx context.Context, addr string) {
	flapTracker := make(map[string]*flapWindow)
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := l.subscribe(ctx, addr, flapTracker); err != nil {
			l.logger.Warn("sentinel pubsub disconnected",
				zap.String("sentinel", addr),
				zap.Error(err),
				zap.Duration("reconnect_in", backoff),
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}
}

// subscribe opens a PSubscribe("*") to one sentinel and reads until the context
// is cancelled or an error occurs.
func (l *PubSubListener) subscribe(ctx context.Context, addr string, flapTracker map[string]*flapWindow) error {
	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    l.cfg.SentinelPassword,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 0, // block indefinitely for pub/sub
	})
	defer client.Close()

	pubsub := client.PSubscribe(ctx, "*")
	defer pubsub.Close()

	l.logger.Info("subscribed to sentinel pubsub", zap.String("sentinel", addr))

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-pubsub.Channel():
			if !ok {
				return nil
			}
			evt := l.parseEvent(msg.Channel, msg.Payload)
			evt = l.detectFlapping(evt, flapTracker)
			l.bus.publish(evt)
			l.logEvent(evt)
		}
	}
}

// parseEvent extracts the node address from the sentinel payload if present.
func (l *PubSubListener) parseEvent(channel, payload string) SentinelEvent {
	evt := SentinelEvent{
		Channel:   channel,
		Payload:   payload,
		Timestamp: time.Now().UTC(),
	}
	// Payload format: "<role> <name> <ip> <port> @ <master-name> <master-ip> <master-port>"
	parts := strings.Fields(payload)
	if len(parts) >= 4 {
		evt.NodeAddr = parts[2] + ":" + parts[3]
	}
	return evt
}

// detectFlapping sets IsFlapping when a node has +sdown > 3 times in 60 seconds.
func (l *PubSubListener) detectFlapping(evt SentinelEvent, tracker map[string]*flapWindow) SentinelEvent {
	if evt.NodeAddr == "" || evt.Channel != "+sdown" {
		return evt
	}

	now := time.Now()
	window := 60 * time.Second
	threshold := 3

	fw, ok := tracker[evt.NodeAddr]
	if !ok {
		fw = &flapWindow{}
		tracker[evt.NodeAddr] = fw
	}

	// Prune timestamps outside the window.
	cutoff := now.Add(-window)
	filtered := fw.times[:0]
	for _, t := range fw.times {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	fw.times = append(filtered, now)

	if len(fw.times) >= threshold {
		evt.IsFlapping = true
		l.logger.Warn("node flapping detected",
			zap.String("node", evt.NodeAddr),
			zap.Int("sdown_count", len(fw.times)),
			zap.Duration("window", window),
		)
	}
	return evt
}

func (l *PubSubListener) logEvent(evt SentinelEvent) {
	l.logger.Info("sentinel event",
		zap.String("channel", evt.Channel),
		zap.String("payload", evt.Payload),
		zap.String("node", evt.NodeAddr),
		zap.Bool("flapping", evt.IsFlapping),
	)
}
