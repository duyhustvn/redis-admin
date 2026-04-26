//go:build integration

// Integration tests for redis-sentinel-admin.
//
// Prerequisites:
//
//	docker compose -f integration/docker-compose.yml up -d
//	go run ./cmd/rsa-server --config config.yaml   # in a separate terminal
//
// Run:
//
//	go test -v -tags integration ./integration/
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

const baseURL = "http://localhost:8080"

// apiResponse mirrors api.APIResponse for test decoding.
type apiResponse struct {
	Data  json.RawMessage `json:"data"`
	Error *apiError       `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// get performs a GET request and returns the decoded response.
func get(t *testing.T, path string) apiResponse {
	t.Helper()
	resp, err := http.Get(baseURL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d body=%s", path, resp.StatusCode, body)
	}
	var ar apiResponse
	if err := json.Unmarshal(body, &ar); err != nil {
		t.Fatalf("GET %s: unmarshal: %v (body=%s)", path, err, body)
	}
	return ar
}

// post performs a POST request with a JSON body and returns the decoded response.
func post(t *testing.T, path string, payload interface{}) (apiResponse, int) {
	t.Helper()
	b, _ := json.Marshal(payload)
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var ar apiResponse
	_ = json.Unmarshal(body, &ar)
	return ar, resp.StatusCode
}

// ── Health ────────────────────────────────────────────────────────────────────

func TestHealthz(t *testing.T) {
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: want 200, got %d", resp.StatusCode)
	}
}

func TestReadyz(t *testing.T) {
	resp, err := http.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz: want 200, got %d", resp.StatusCode)
	}
}

// ── Topology ──────────────────────────────────────────────────────────────────

func TestTopology(t *testing.T) {
	ar := get(t, "/api/v1/topology")
	if ar.Error != nil {
		t.Fatalf("topology error: %s", ar.Error.Message)
	}
	var snap struct {
		Master struct {
			Addr      string `json:"addr"`
			IsHealthy bool   `json:"is_healthy"`
		} `json:"master"`
		QuorumOK bool `json:"quorum_ok"`
	}
	if err := json.Unmarshal(ar.Data, &snap); err != nil {
		t.Fatalf("decode topology: %v", err)
	}
	if snap.Master.Addr == "" {
		t.Fatal("topology: master addr is empty")
	}
	if !snap.Master.IsHealthy {
		t.Errorf("topology: master not healthy")
	}
}

// ── Replication ───────────────────────────────────────────────────────────────

func TestReplicationLag(t *testing.T) {
	ar := get(t, "/api/v1/replication/lag")
	if ar.Error != nil {
		t.Fatalf("replication/lag error: %s", ar.Error.Message)
	}
	var lags []struct {
		NodeAddr  string `json:"node_addr"`
		LagBytes  int64  `json:"lag_bytes"`
		IsCaughtUp bool  `json:"is_caught_up"`
	}
	if err := json.Unmarshal(ar.Data, &lags); err != nil {
		t.Fatalf("decode lag: %v", err)
	}
	if len(lags) == 0 {
		t.Fatal("replication/lag: expected at least one replica")
	}
	for _, l := range lags {
		t.Logf("replica %s lag=%d caught_up=%v", l.NodeAddr, l.LagBytes, l.IsCaughtUp)
	}
}

func TestResyncStats(t *testing.T) {
	ar := get(t, "/api/v1/replication/resync-stats")
	if ar.Error != nil {
		t.Fatalf("resync-stats error: %s", ar.Error.Message)
	}
	var reports []struct {
		NodeAddr string `json:"node_addr"`
		Role     string `json:"role"`
	}
	if err := json.Unmarshal(ar.Data, &reports); err != nil {
		t.Fatalf("decode resync-stats: %v", err)
	}
	if len(reports) == 0 {
		t.Fatal("resync-stats: expected at least one node")
	}
}

// ── Diagnostics ───────────────────────────────────────────────────────────────

func TestSlowlog(t *testing.T) {
	ar := get(t, "/api/v1/diagnostics/slowlog?limit=10")
	if ar.Error != nil {
		t.Fatalf("slowlog error: %s", ar.Error.Message)
	}
	// Slowlog may legitimately be empty on a fresh cluster — just verify shape.
	var entries []json.RawMessage
	_ = json.Unmarshal(ar.Data, &entries)
	t.Logf("slowlog: %d entries", len(entries))
}

func TestPipelineStats(t *testing.T) {
	ar := get(t, "/api/v1/diagnostics/pipeline")
	if ar.Error != nil {
		t.Fatalf("pipeline error: %s", ar.Error.Message)
	}
}

func TestMemoryReport(t *testing.T) {
	ar := get(t, "/api/v1/diagnostics/memory")
	if ar.Error != nil {
		t.Fatalf("memory error: %s", ar.Error.Message)
	}
	var reports []struct {
		NodeAddr  string  `json:"node_addr"`
		FragRatio float64 `json:"frag_ratio"`
	}
	if err := json.Unmarshal(ar.Data, &reports); err != nil {
		t.Fatalf("decode memory: %v", err)
	}
	if len(reports) == 0 {
		t.Fatal("memory: expected at least one node")
	}
}

// ── Connections ───────────────────────────────────────────────────────────────

func TestConnections(t *testing.T) {
	ar := get(t, "/api/v1/connections")
	if ar.Error != nil {
		t.Fatalf("connections error: %s", ar.Error.Message)
	}
}

func TestDistribution(t *testing.T) {
	ar := get(t, "/api/v1/connections/distribution")
	if ar.Error != nil {
		t.Fatalf("distribution error: %s", ar.Error.Message)
	}
}

// ── Config ────────────────────────────────────────────────────────────────────

func TestConfigDiff(t *testing.T) {
	ar := get(t, "/api/v1/config/diff")
	if ar.Error != nil {
		t.Fatalf("config/diff error: %s", ar.Error.Message)
	}
	// On a freshly started cluster, all nodes should agree — drift list may be empty.
	var diffs []json.RawMessage
	_ = json.Unmarshal(ar.Data, &diffs)
	t.Logf("config/diff: %d drifted keys", len(diffs))
}

func TestConfigAuditEmpty(t *testing.T) {
	ar := get(t, "/api/v1/config/audit")
	if ar.Error != nil {
		t.Fatalf("config/audit error: %s", ar.Error.Message)
	}
}

func TestSetConfigRequiresConfirm(t *testing.T) {
	ar, status := post(t, "/api/v1/config/set", map[string]interface{}{
		"node_addr": "127.0.0.1:6379",
		"key":       "slowlog-max-len",
		"value":     "128",
		// confirm deliberately omitted
	})
	if status == http.StatusOK {
		t.Fatal("config/set without confirm should not return 200")
	}
	if ar.Error == nil || ar.Error.Code != "CONFIRM_REQUIRED" {
		t.Fatalf("expected CONFIRM_REQUIRED, got: %+v", ar.Error)
	}
}

// ── Chaos — seed + flush round-trip ──────────────────────────────────────────

func TestSeedAndFlush(t *testing.T) {
	const prefix = "inttest:"
	const keyCount = 50

	// Seed
	seedResp, status := post(t, "/api/v1/ops/chaos/seed", map[string]interface{}{
		"key_count":  keyCount,
		"key_prefix": prefix,
		"value_size": 32,
		"key_type":   "string",
		"ttl_sec":    600,
		"confirm":    true,
	})
	if status != http.StatusOK {
		t.Fatalf("chaos/seed: status %d error=%v", status, seedResp.Error)
	}
	var seedResult struct {
		KeysCreated int64 `json:"keys_created"`
	}
	if err := json.Unmarshal(seedResp.Data, &seedResult); err != nil {
		t.Fatalf("decode seed result: %v", err)
	}
	if seedResult.KeysCreated != keyCount {
		t.Errorf("seed: want %d keys, got %d", keyCount, seedResult.KeysCreated)
	}
	t.Logf("seeded %d keys under %s", seedResult.KeysCreated, prefix)

	// Brief pause to let replication catch up.
	time.Sleep(200 * time.Millisecond)

	// Flush
	flushResp, status := post(t, "/api/v1/ops/chaos/flush", map[string]interface{}{
		"pattern": fmt.Sprintf("%s*", prefix),
		"confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("chaos/flush: status %d error=%v", status, flushResp.Error)
	}
	var flushResult struct {
		KeysDeleted int64 `json:"keys_deleted"`
	}
	if err := json.Unmarshal(flushResp.Data, &flushResult); err != nil {
		t.Fatalf("decode flush result: %v", err)
	}
	if flushResult.KeysDeleted < keyCount {
		t.Errorf("flush: want >= %d keys deleted, got %d", keyCount, flushResult.KeysDeleted)
	}
	t.Logf("flushed %d keys", flushResult.KeysDeleted)
}

func TestFlushRequiresConfirm(t *testing.T) {
	ar, status := post(t, "/api/v1/ops/chaos/flush", map[string]interface{}{
		"pattern": "test:*",
		// confirm deliberately omitted
	})
	if status == http.StatusOK {
		t.Fatal("chaos/flush without confirm should not return 200")
	}
	if ar.Error == nil || ar.Error.Code != "CONFIRM_REQUIRED" {
		t.Fatalf("expected CONFIRM_REQUIRED, got: %+v", ar.Error)
	}
}

// ── Failover dry-run ──────────────────────────────────────────────────────────

func TestFailoverDryRun(t *testing.T) {
	ar, status := post(t, "/api/v1/ops/failover", map[string]interface{}{
		"dry_run": true,
		"confirm": false,
	})
	if status != http.StatusOK {
		t.Fatalf("failover dry-run: status %d error=%v", status, ar.Error)
	}
	var result struct {
		DryRun          bool     `json:"dry_run"`
		PreChecks       []string `json:"pre_checks"`
		SelectedReplica string   `json:"selected_replica"`
		OldMaster       string   `json:"old_master"`
	}
	if err := json.Unmarshal(ar.Data, &result); err != nil {
		t.Fatalf("decode failover result: %v", err)
	}
	if !result.DryRun {
		t.Error("failover dry-run: dry_run field should be true")
	}
	if result.OldMaster == "" {
		t.Error("failover dry-run: old_master should not be empty")
	}
	if len(result.PreChecks) == 0 {
		t.Error("failover dry-run: pre_checks should not be empty")
	}
	t.Logf("failover dry-run: master=%s candidate=%s checks=%v",
		result.OldMaster, result.SelectedReplica, result.PreChecks)
}

func TestFailoverRequiresConfirm(t *testing.T) {
	ar, status := post(t, "/api/v1/ops/failover", map[string]interface{}{
		"dry_run": false,
		"confirm": false,
	})
	if status == http.StatusOK {
		t.Fatal("failover without confirm should not return 200")
	}
	if ar.Error == nil || ar.Error.Code != "CONFIRM_REQUIRED" {
		t.Fatalf("expected CONFIRM_REQUIRED, got: %+v", ar.Error)
	}
}
