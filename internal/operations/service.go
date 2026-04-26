package operations

import (
	"context"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// Service exposes cluster operation endpoints.
type Service interface {
	GetConfigDiff(ctx context.Context) ([]ConfigDiff, error)
	GetAuditLog() []AuditEntry
	SetConfig(ctx context.Context, nodeAddr, key, value, remoteIP string) error
	Failover(ctx context.Context, dryRun bool) (*FailoverResult, error)
}

// OperationsService implements Service.
type OperationsService struct {
	cfg         *config.Config
	sentinelSvc sentinel.Service
	logger      *zap.Logger
	audit       *auditLog
	notify      *notifier
}

// New creates an OperationsService.
func New(cfg *config.Config, svc sentinel.Service, logger *zap.Logger) *OperationsService {
	return &OperationsService{
		cfg:         cfg,
		sentinelSvc: svc,
		logger:      logger,
		audit:       newAuditLog(),
		notify:      newNotifier(cfg.WebhookURL, logger),
	}
}
