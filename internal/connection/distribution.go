package connection

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"go.uber.org/zap"
)

// ReplicaDistribution shows the read/write command split for one replica.
// Overloaded is set when the replica handles >80% of total cluster read traffic —
// a sign of load imbalance across replicas.
type ReplicaDistribution struct {
	NodeAddr   string  `json:"node_addr"`
	ReadCount  int64   `json:"read_count"`
	WriteCount int64   `json:"write_count"`
	TotalCount int64   `json:"total_count"`
	ReadPct    float64 `json:"read_pct"`
	WritePct   float64 `json:"write_pct"`
	Overloaded bool    `json:"overloaded"` // true when this replica carries >80% of total cluster reads
}

// GetDistribution fetches INFO commandstats from every replica, classifies each
// command as read or write, and flags replicas that carry >80% of total cluster reads.
func (s *ConnectionService) GetDistribution(ctx context.Context) ([]ReplicaDistribution, error) {
	addrs, err := s.sentinelSvc.GetNodeAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("get distribution: %w", err)
	}

	var results []ReplicaDistribution
	var errs []error

	// Collect stats from every node (master included for context) but we only
	// surface replicas in the final list.
	for _, addr := range addrs.Replicas {
		dist, err := s.fetchCommandDist(ctx, addr)
		if err != nil {
			s.logger.Warn("commandstats fetch failed",
				zap.String("replica", addr),
				zap.Error(err),
			)
			errs = append(errs, fmt.Errorf("replica %s: %w", addr, err))
			continue
		}
		results = append(results, dist)
	}

	// Flag overloaded: replica with >80% of total cluster reads.
	var totalReads int64
	for _, r := range results {
		totalReads += r.ReadCount
	}
	if totalReads > 0 {
		for i := range results {
			if float64(results[i].ReadCount)/float64(totalReads) > 0.80 {
				results[i].Overloaded = true
			}
		}
	}

	return results, errors.Join(errs...)
}

func (s *ConnectionService) fetchCommandDist(ctx context.Context, addr string) (ReplicaDistribution, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	client := sentinel.NewDirectClient(addr, s.cfg.RedisPassword)
	defer client.Close()

	raw, err := client.Info(ctx, "commandstats").Result()
	if err != nil {
		return ReplicaDistribution{}, fmt.Errorf("INFO commandstats on %s: %w", addr, sentinel.ErrNodeUnreachable)
	}

	var reads, writes int64
	for _, line := range strings.Split(raw, "\r\n") {
		if !strings.HasPrefix(line, "cmdstat_") {
			continue
		}
		// cmdstat_get:calls=100,usec=500,usec_per_call=5.00,...
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx < 0 {
			continue
		}
		cmd := strings.TrimPrefix(line[:colonIdx], "cmdstat_")
		calls := extractCalls(line[colonIdx+1:])
		if calls == 0 {
			continue
		}
		if isReadCommand(cmd) {
			reads += calls
		} else {
			writes += calls
		}
	}

	total := reads + writes
	dist := ReplicaDistribution{
		NodeAddr:   addr,
		ReadCount:  reads,
		WriteCount: writes,
		TotalCount: total,
	}
	if total > 0 {
		dist.ReadPct = float64(reads) / float64(total) * 100
		dist.WritePct = float64(writes) / float64(total) * 100
	}
	return dist, nil
}

// extractCalls parses "calls=N,..." and returns N.
func extractCalls(stats string) int64 {
	for _, kv := range strings.Split(stats, ",") {
		if strings.HasPrefix(kv, "calls=") {
			n, _ := strconv.ParseInt(strings.TrimPrefix(kv, "calls="), 10, 64)
			return n
		}
	}
	return 0
}

// readCommands is the set of Redis commands classified as read-only.
var readCommands = map[string]bool{
	// Strings
	"get": true, "mget": true, "getrange": true, "substr": true, "strlen": true,
	"getbit": true, "bitcount": true, "bitpos": true, "pfcount": true,
	// Hashes
	"hget": true, "hmget": true, "hgetall": true, "hkeys": true, "hvals": true,
	"hlen": true, "hexists": true, "hscan": true, "hrandfield": true, "hstrlen": true,
	// Lists
	"lrange": true, "llen": true, "lindex": true, "lpos": true,
	// Sets
	"scard": true, "srandmember": true, "sismember": true, "smismember": true,
	"smembers": true, "sscan": true, "sintercard": true,
	// Sorted sets
	"zrange": true, "zrevrange": true, "zrangebyscore": true, "zrevrangebyscore": true,
	"zrangebylex": true, "zrevrangebylex": true, "zcard": true, "zcount": true,
	"zlexcount": true, "zrank": true, "zrevrank": true, "zscore": true, "zmscore": true,
	"zscan": true, "zrandmember": true,
	// Key metadata
	"exists": true, "type": true, "ttl": true, "pttl": true,
	"expiretime": true, "pexpiretime": true, "dump": true, "object": true,
	// Geo
	"geodist": true, "geopos": true, "geohash": true, "geosearch": true,
	"georadius": true, "georadiusbymember": true,
	// Streams
	"xlen": true, "xrange": true, "xrevrange": true, "xread": true,
	"xinfo": true, "xpending": true,
	// Scan / server
	"scan": true, "keys": true, "randomkey": true, "dbsize": true,
	"info": true, "sort": true, "lolwut": true, "time": true, "command": true,
}

func isReadCommand(cmd string) bool {
	return readCommands[strings.ToLower(cmd)]
}
