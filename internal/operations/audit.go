// Package operations provides config management, audit logging, failover orchestration,
// and webhook notifications for the Redis Sentinel cluster.
package operations

import (
	"sync"
	"time"
)

const auditRingMax = 500

// AuditEntry records a single Redis config change.
type AuditEntry struct {
	Timestamp time.Time `json:"timestamp"`
	NodeAddr  string    `json:"node_addr"`
	Key       string    `json:"key"`
	OldValue  string    `json:"old_value"`
	NewValue  string    `json:"new_value"`
	RemoteIP  string    `json:"remote_ip,omitempty"`
}

// auditLog is a bounded ring buffer of AuditEntry values.
type auditLog struct {
	mu      sync.RWMutex
	entries []AuditEntry
}

func newAuditLog() *auditLog { return &auditLog{} }

func (a *auditLog) record(e AuditEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.entries = append(a.entries, e)
	if len(a.entries) > auditRingMax {
		a.entries = a.entries[len(a.entries)-auditRingMax:]
	}
}

func (a *auditLog) list() []AuditEntry {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]AuditEntry, len(a.entries))
	copy(out, a.entries)
	return out
}
