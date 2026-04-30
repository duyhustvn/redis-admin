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

// Check xác nhận quorum Sentinel có đang được thỏa mãn không.
//
// Redis command: SENTINEL CKQUORUM <masterName>
//
// Lưu ý: lệnh này gửi đến Sentinel node (port 26379), KHÔNG phải Redis node (port 6379).
// Client dùng là SentinelManagementClient, khác với DirectClient dùng cho Redis node.
//
// Output từ Sentinel:
//
//	OK 3 usable Sentinels. Quorum and failover authorization can be reached
//	  → quorum đủ, failover có thể thực hiện
//
//	NOQUORUM 1 usable Sentinels. Not enough for quorum or failover
//	  → không đủ sentinel reachable để bầu master mới
//
// Xử lý:
//   - Lặp qua danh sách sentinel theo thứ tự cấu hình.
//   - Dừng sớm tại sentinel đầu tiên trả về "OK" — không cần hỏi tất cả.
//   - Sentinel không reachable hoặc trả về NOQUORUM → log warn, thử sentinel tiếp theo.
//   - Hết danh sách mà không có OK → return false.
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
