package operations

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// NodeConfigValue is a single node's value for one config key.
type NodeConfigValue struct {
	NodeAddr string `json:"node_addr"`
	Value    string `json:"value"`
}

// ConfigDiff shows the per-node values for one Redis config key.
// IsDrift is true when at least two nodes disagree on the value.
type ConfigDiff struct {
	Key     string            `json:"key"`
	Values  []NodeConfigValue `json:"values"`
	IsDrift bool              `json:"is_drift"`
}

// GetConfigDiff runs CONFIG GET * on every node and returns keys whose values
// differ across nodes.
func (s *OperationsService) GetConfigDiff(ctx context.Context) ([]ConfigDiff, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get config diff: %w", err)
	}

	type nodeRole struct {
		addr string
	}
	nodes := []nodeRole{{addrs.Master}}
	for _, r := range addrs.Replicas {
		nodes = append(nodes, nodeRole{r})
	}

	// configMap[key][nodeAddr] = value
	configMap := make(map[string]map[string]string)
	var errs []error

	for _, n := range nodes {
		cfg, err := s.fetchAllConfig(ctx, n.addr)
		if err != nil {
			s.logger.Warn("config fetch failed", zap.String("node", n.addr), zap.Error(err))
			errs = append(errs, fmt.Errorf("node %s: %w", n.addr, err))
			continue
		}
		for k, v := range cfg {
			if configMap[k] == nil {
				configMap[k] = make(map[string]string)
			}
			configMap[k][n.addr] = v
		}
	}

	// Build sorted diff list — only include keys present on >1 node.
	keys := make([]string, 0, len(configMap))
	for k := range configMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var diffs []ConfigDiff
	for _, key := range keys {
		nodeVals := configMap[key]
		if len(nodeVals) < 2 {
			continue
		}
		diff := ConfigDiff{Key: key}
		firstVal := ""
		for addr, val := range nodeVals {
			diff.Values = append(diff.Values, NodeConfigValue{NodeAddr: addr, Value: val})
			if firstVal == "" {
				firstVal = val
			} else if val != firstVal {
				diff.IsDrift = true
			}
		}
		// Only include drifted keys to keep response concise.
		if diff.IsDrift {
			diffs = append(diffs, diff)
		}
	}
	return diffs, errors.Join(errs...)
}

func (s *OperationsService) fetchAllConfig(ctx context.Context, addr string) (map[string]string, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	vals, err := client.ConfigGet(tctx, "*").Result()
	if err != nil {
		return nil, fmt.Errorf("CONFIG GET * on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}
	return vals, nil
}

// SetConfig applies a CONFIG SET on a specific node and records the change in the audit log.
func (s *OperationsService) SetConfig(ctx context.Context, nodeAddr, key, value, remoteIP string) error {
	// Read current value first for the audit record.
	oldVal, err := s.fetchConfigKey(ctx, nodeAddr, key)
	if err != nil {
		s.logger.Warn("could not read old config value before SET",
			zap.String("node", nodeAddr),
			zap.String("key", key),
			zap.Error(err),
		)
		oldVal = "<unknown>"
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(nodeAddr, s.cfg.RedisPassword)
	defer client.Close()

	if err := client.ConfigSet(tctx, key, value).Err(); err != nil {
		return fmt.Errorf("CONFIG SET %s=%s on %s: %w", key, value, nodeAddr, err)
	}

	s.audit.record(AuditEntry{
		Timestamp: time.Now().UTC(),
		NodeAddr:  nodeAddr,
		Key:       key,
		OldValue:  oldVal,
		NewValue:  value,
		RemoteIP:  remoteIP,
	})

	s.logger.Info("config changed",
		zap.String("node", nodeAddr),
		zap.String("key", key),
		zap.String("old", oldVal),
		zap.String("new", value),
		zap.String("remote_ip", remoteIP),
	)
	return nil
}

// GetAuditLog returns a snapshot of the audit ring buffer.
func (s *OperationsService) GetAuditLog() []AuditEntry {
	return s.audit.list()
}

func (s *OperationsService) fetchConfigKey(ctx context.Context, addr, key string) (string, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	vals, err := client.ConfigGet(tctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("CONFIG GET %s on %s: %w", key, addr, err)
	}
	if v, ok := vals[key]; ok {
		return v, nil
	}
	return "", nil
}
