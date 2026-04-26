package operations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// webhookPayload is the JSON body sent to the webhook URL on failover events.
type webhookPayload struct {
	Event     string    `json:"event"`
	OldMaster string    `json:"old_master,omitempty"`
	NewMaster string    `json:"new_master,omitempty"`
	Message   string    `json:"message,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// notifier fires a POST to a configured webhook URL.
type notifier struct {
	url    string
	client *http.Client
	logger *zap.Logger
}

func newNotifier(url string, logger *zap.Logger) *notifier {
	return &notifier{
		url:    url,
		client: &http.Client{Timeout: 5 * time.Second},
		logger: logger,
	}
}

// send posts payload to the webhook URL. Errors are logged but not propagated
// so that notification failures never block the caller.
func (n *notifier) send(ctx context.Context, payload webhookPayload) {
	if n.url == "" {
		return
	}
	b, err := json.Marshal(payload)
	if err != nil {
		n.logger.Warn("webhook marshal failed", zap.Error(err))
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(b))
	if err != nil {
		n.logger.Warn("webhook request build failed", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		n.logger.Warn("webhook delivery failed", zap.String("url", n.url), zap.Error(err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		n.logger.Warn("webhook non-2xx response",
			zap.String("url", n.url),
			zap.Int("status", resp.StatusCode),
		)
		return
	}
	n.logger.Info("webhook delivered",
		zap.String("event", payload.Event),
		zap.Int("status", resp.StatusCode),
	)
}

// notifyFailover sends a failover event notification asynchronously.
func (n *notifier) notifyFailover(oldMaster, newMaster string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	go func() {
		defer cancel()
		n.send(ctx, webhookPayload{
			Event:     "failover",
			OldMaster: oldMaster,
			NewMaster: newMaster,
			Message:   fmt.Sprintf("Redis master failed over from %s to %s", oldMaster, newMaster),
			Timestamp: time.Now().UTC(),
		})
	}()
}
