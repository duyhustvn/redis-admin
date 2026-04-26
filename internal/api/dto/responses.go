package dto

import "time"

// HealthResponse is the response body for GET /healthz and GET /readyz.
//
//	@Description	Service health status
type HealthResponse struct {
	Status    string    `json:"status"     example:"ok"`
	Timestamp time.Time `json:"timestamp"`
}

// FailoverResponse is the response body for POST /api/v1/ops/failover.
//
//	@Description	Failover operation result
type FailoverResponse struct {
	NewMaster string   `json:"new_master"  example:"10.0.0.3:6379"`
	ElapsedMs int64    `json:"elapsed_ms"  example:"342"`
	DryRun    bool     `json:"dry_run"     example:"false"`
	PreChecks []string `json:"pre_checks"`
}
