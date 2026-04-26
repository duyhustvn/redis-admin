package chaos

import (
	"context"
	"fmt"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"go.uber.org/zap"
)

// ChaosFailoverResult describes the outcome of a chaos failover trigger.
type ChaosFailoverResult struct {
	Mode      string `json:"mode"`              // "sentinel" | "pod"
	Target    string `json:"target,omitempty"`  // sentinel addr or pod name
	ElapsedMs int64  `json:"elapsed_ms"`
}

// TriggerChaosFailover fires a failover without any pre-checks.
//
// mode "sentinel" — sends SENTINEL FAILOVER directly to the first reachable sentinel.
// mode "pod"      — deletes the named Pod via the K8s API to force Redis failover.
func (s *ChaosService) TriggerChaosFailover(ctx context.Context, mode, podNamespace, podName string) (*ChaosFailoverResult, error) {
	start := time.Now()

	switch mode {
	case "pod":
		return s.podKillFailover(ctx, podNamespace, podName, start)
	default: // "sentinel"
		return s.sentinelFailover(ctx, start)
	}
}

func (s *ChaosService) sentinelFailover(ctx context.Context, start time.Time) (*ChaosFailoverResult, error) {
	var target string
	for _, addr := range s.cfg.SentinelAddrs {
		sc := sentinel.NewDirectClient(addr, s.cfg.SentinelPassword)
		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := sc.Ping(tctx).Err()
		cancel()
		if err != nil {
			sc.Close()
			continue
		}
		tctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
		err = sc.Do(tctx2, "SENTINEL", "failover", s.cfg.MasterName).Err()
		cancel2()
		sc.Close()
		if err != nil {
			return nil, fmt.Errorf("SENTINEL FAILOVER on %s: %w", addr, err)
		}
		target = addr
		break
	}
	if target == "" {
		return nil, fmt.Errorf("no reachable sentinel found: %w", sentinel.ErrNodeUnreachable)
	}

	s.logger.Info("chaos failover triggered via sentinel", zap.String("sentinel", target))
	return &ChaosFailoverResult{
		Mode:      "sentinel",
		Target:    target,
		ElapsedMs: time.Since(start).Milliseconds(),
	}, nil
}

func (s *ChaosService) podKillFailover(ctx context.Context, namespace, podName string, start time.Time) (*ChaosFailoverResult, error) {
	if s.k8sClient == nil {
		return nil, fmt.Errorf("pod-delete mode requires a Kubernetes client: K8s is unavailable")
	}
	if podName == "" {
		return nil, fmt.Errorf("pod_name is required for mode=pod")
	}
	if namespace == "" {
		namespace = s.cfg.K8sNamespace
	}

	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err := s.k8sClient.CoreV1().Pods(namespace).Delete(tctx, podName, metav1.DeleteOptions{})
	if err != nil {
		return nil, fmt.Errorf("delete pod %s/%s: %w", namespace, podName, err)
	}

	s.logger.Info("chaos failover triggered via pod delete",
		zap.String("namespace", namespace),
		zap.String("pod", podName),
	)
	return &ChaosFailoverResult{
		Mode:      "pod",
		Target:    fmt.Sprintf("%s/%s", namespace, podName),
		ElapsedMs: time.Since(start).Milliseconds(),
	}, nil
}
