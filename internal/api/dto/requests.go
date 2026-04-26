// Package dto contains request and response structs referenced in swaggo annotations.
// All structs in this package are picked up by swag init --parseInternal.
package dto

// ConfirmRequest is embedded by mutation endpoints that require explicit confirmation.
type ConfirmRequest struct {
	Confirm bool `json:"confirm" example:"true"`
}

// FailoverRequest is the request body for POST /api/v1/ops/failover.
//
//	@Description	Failover trigger options
type FailoverRequest struct {
	DryRun  bool `json:"dry_run"  example:"false"`
	Confirm bool `json:"confirm"  example:"true"`
}

// SeedRequest is the request body for POST /api/v1/ops/chaos/seed.
//
//	@Description	Dummy data seed parameters
type SeedRequest struct {
	KeyCount  int    `json:"key_count"  example:"1000"`
	KeyPrefix string `json:"key_prefix" example:"test:"`
	ValueSize int    `json:"value_size" example:"256"`
	KeyType   string `json:"key_type"   example:"string"` // string|hash|list|set|zset
	TTLSec    int64  `json:"ttl_sec"    example:"3600"`   // 0 = no expiry
	Confirm   bool   `json:"confirm"    example:"true"`
}

// FlushRequest is the request body for POST /api/v1/ops/chaos/flush.
//
//	@Description	Pattern-scoped cluster flush parameters
type FlushRequest struct {
	Pattern string `json:"pattern" example:"test:*"`
	Confirm bool   `json:"confirm" example:"true"`
}

// ChaosFailoverRequest is the request body for POST /api/v1/ops/chaos/failover.
//
//	@Description	Chaos failover trigger parameters
type ChaosFailoverRequest struct {
	Mode         string `json:"mode"          example:"sentinel"` // "sentinel"|"pod"
	PodNamespace string `json:"pod_namespace" example:"redis"`
	PodName      string `json:"pod_name"      example:"redis-master-0"`
	Confirm      bool   `json:"confirm"       example:"true"`
}

// ConfigSetRequest is the request body for POST /api/v1/config/set.
//
//	@Description	Redis CONFIG SET parameters
type ConfigSetRequest struct {
	NodeAddr string `json:"node_addr" example:"10.0.0.2:6379"`
	Key      string `json:"key"       example:"maxmemory-policy"`
	Value    string `json:"value"     example:"allkeys-lfu"`
	Confirm  bool   `json:"confirm"   example:"true"`
}
