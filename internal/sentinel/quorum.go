package sentinel

import (
	"context"
	"strings"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"go.uber.org/zap"
)

// QuorumChecker validates sentinel quorum by running SENTINEL CKQUORUM against
// every configured sentinel address.
type QuorumChecker struct {
	cfg    *config.Config
	logger *zap.Logger
}

// NewQuorumChecker creates a QuorumChecker.
func NewQuorumChecker(cfg *config.Config, logger *zap.Logger) *QuorumChecker {
	return &QuorumChecker{cfg: cfg, logger: logger}
}

// Check returns true if at least one sentinel reports quorum as satisfied.
// It attempts each sentinel address in order and stops on the first OK response.
func (q *QuorumChecker) Check(ctx context.Context) bool {
	for _, addr := range q.cfg.SentinelAddrs {
		sc := NewSentinelManagementClient(addr, q.cfg.SentinelPassword)
		result, err := sc.CkQuorum(ctx, q.cfg.MasterName).Result()
		sc.Close()
		if err != nil {
			q.logger.Warn("ckquorum failed",
				zap.String("sentinel", addr),
				zap.Error(err),
			)
			continue
		}
		if strings.HasPrefix(result, "OK") {
			return true
		}
		q.logger.Warn("ckquorum not OK",
			zap.String("sentinel", addr),
			zap.String("result", result),
		)
	}
	return false
}
