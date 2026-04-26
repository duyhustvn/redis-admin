package sentinel

import (
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/redis/go-redis/v9"
)

// NewMasterClient returns a failover client that always routes writes to the
// current Sentinel master.
func NewMasterClient(cfg *config.Config) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       cfg.MasterName,
		SentinelAddrs:    cfg.SentinelAddrs,
		SentinelPassword: cfg.SentinelPassword,
		Password:         cfg.RedisPassword,
		DB:               0,
		ReadTimeout:      2 * time.Second,
		WriteTimeout:     2 * time.Second,
		PoolSize:         10,
		DialTimeout:      3 * time.Second,
	})
}

// NewReplicaClient returns a failover client that routes reads to replicas only.
func NewReplicaClient(cfg *config.Config) *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       cfg.MasterName,
		SentinelAddrs:    cfg.SentinelAddrs,
		SentinelPassword: cfg.SentinelPassword,
		Password:         cfg.RedisPassword,
		ReplicaOnly:      true,
		ReadTimeout:      2 * time.Second,
		WriteTimeout:     2 * time.Second,
		PoolSize:         5,
		DialTimeout:      3 * time.Second,
	})
}

// NewDirectClient opens a plain connection to a specific Redis or Sentinel address.
// Use this for node-level commands (INFO, CONFIG GET, SLOWLOG) and Sentinel management.
func NewDirectClient(addr, password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		DialTimeout:  3 * time.Second,
		PoolSize:     3,
	})
}

// NewSentinelManagementClient returns a SentinelClient for issuing Sentinel
// management commands (SENTINEL replicas, SENTINEL ckquorum, etc.) against a
// specific sentinel address.
func NewSentinelManagementClient(addr, password string) *redis.SentinelClient {
	return redis.NewSentinelClient(&redis.Options{
		Addr:        addr,
		Password:    password,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 2 * time.Second,
	})
}
