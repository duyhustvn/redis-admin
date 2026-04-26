package k8s

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
	"go.uber.org/zap"
)

// ThrottleChecker detects CPU-throttled pods by comparing actual CPU usage
// (from metrics-server) against the pod's CPU limit (from the informer cache).
// A nil ThrottleChecker is safe to use — all methods are no-ops.
type ThrottleChecker struct {
	podCache      *PodCache
	metricsClient metricsclient.Interface
	logger        *zap.Logger
}

// NewThrottleChecker creates a ThrottleChecker backed by the metrics-server API.
// Returns nil when the metrics client cannot be initialised (no metrics-server,
// no k8s config) so callers can safely skip throttle checks.
func NewThrottleChecker(cache *PodCache, logger *zap.Logger) *ThrottleChecker {
	cfg, err := loadK8sConfig()
	if err != nil {
		logger.Debug("throttle checker disabled: no k8s config", zap.Error(err))
		return nil
	}
	mc, err := metricsclient.NewForConfig(cfg)
	if err != nil {
		logger.Debug("throttle checker disabled: metrics client init failed", zap.Error(err))
		return nil
	}
	return &ThrottleChecker{podCache: cache, metricsClient: mc, logger: logger}
}

// IsThrottled returns true when podName in namespace is consuming >80% of its CPU
// limit according to the metrics-server. Returns false on any lookup failure so
// the caller sees a conservative (non-alarming) result when metrics are unavailable.
func (t *ThrottleChecker) IsThrottled(ctx context.Context, namespace, podName string) bool {
	if t == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	podMetrics, err := t.metricsClient.MetricsV1beta1().PodMetricses(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.logger.Debug("get pod metrics failed", zap.String("pod", podName), zap.Error(err))
		return false
	}

	pod := t.podCache.LookupByName(namespace, podName)
	if pod == nil {
		return false
	}

	for i, containerMetrics := range podMetrics.Containers {
		if i >= len(pod.Spec.Containers) {
			break
		}
		limit := pod.Spec.Containers[i].Resources.Limits.Cpu()
		if limit.IsZero() {
			continue
		}
		actual := containerMetrics.Usage.Cpu()
		if actual.MilliValue() > limit.MilliValue()*80/100 {
			return true
		}
	}
	return false
}
