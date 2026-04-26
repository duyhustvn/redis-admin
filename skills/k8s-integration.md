# Skill: K8s Integration Patterns

Read this file before working on `internal/k8s/` or any feature that requires mapping Redis client IPs to Kubernetes Pods.

---

## Core Principle

**Never poll the K8s API in hot paths.** Always use an Informer to build a local in-memory cache. The cache is updated by watch events, not polling. This avoids hammering the K8s API server.

## Pod Informer Setup

```go
package k8s

import (
    "context"
    "sync"

    corev1 "k8s.io/api/core/v1"
    "k8s.io/client-go/informers"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/tools/cache"
    "k8s.io/client-go/tools/clientcmd"
)

type PodCache struct {
    mu      sync.RWMutex
    ipToPod map[string]*corev1.Pod // key: pod IP
}

func NewPodCache() *PodCache {
    return &PodCache{ipToPod: make(map[string]*corev1.Pod)}
}

func (c *PodCache) Lookup(ip string) (*corev1.Pod, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    pod, ok := c.ipToPod[ip]
    return pod, ok
}

func (c *PodCache) set(pod *corev1.Pod) {
    c.mu.Lock()
    defer c.mu.Unlock()
    if pod.Status.PodIP != "" {
        c.ipToPod[pod.Status.PodIP] = pod
    }
}

func (c *PodCache) delete(pod *corev1.Pod) {
    c.mu.Lock()
    defer c.mu.Unlock()
    delete(c.ipToPod, pod.Status.PodIP)
}
```

## Starting the Informer

```go
func StartPodInformer(ctx context.Context, namespace string, cache *PodCache) error {
    config, err := loadK8sConfig()
    if err != nil {
        return fmt.Errorf("load k8s config: %w", err)
    }

    client, err := kubernetes.NewForConfig(config)
    if err != nil {
        return fmt.Errorf("create k8s client: %w", err)
    }

    factory := informers.NewSharedInformerFactoryWithOptions(
        client,
        0, // resync period — 0 = no periodic resync, rely on watch events
        informers.WithNamespace(namespace),
    )

    podInformer := factory.Core().V1().Pods().Informer()
    podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
        AddFunc: func(obj interface{}) {
            pod, ok := obj.(*corev1.Pod)
            if ok {
                cache.set(pod)
            }
        },
        UpdateFunc: func(_, newObj interface{}) {
            pod, ok := newObj.(*corev1.Pod)
            if ok {
                cache.set(pod)
            }
        },
        DeleteFunc: func(obj interface{}) {
            pod, ok := obj.(*corev1.Pod)
            if !ok {
                // Handle tombstone
                tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
                if !ok {
                    return
                }
                pod, ok = tombstone.Obj.(*corev1.Pod)
                if !ok {
                    return
                }
            }
            cache.delete(pod)
        },
    })

    factory.Start(ctx.Done())

    // Wait for initial cache sync before returning
    if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
        return fmt.Errorf("timed out waiting for pod cache sync")
    }

    return nil
}
```

## K8s Config Loading (In-cluster + Local fallback)

```go
func loadK8sConfig() (*rest.Config, error) {
    // Try in-cluster first (running inside K8s pod)
    cfg, err := rest.InClusterConfig()
    if err == nil {
        return cfg, nil
    }

    // Fall back to kubeconfig (local development)
    kubeconfig := os.Getenv("KUBECONFIG")
    if kubeconfig == "" {
        kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
    }
    return clientcmd.BuildConfigFromFlags("", kubeconfig)
}
```

## IP → Pod Mapping (Used by connection/mapper.go)

```go
// Extract deployment name from pod owner references
func GetDeploymentName(pod *corev1.Pod) string {
    for _, ref := range pod.OwnerReferences {
        if ref.Kind == "ReplicaSet" {
            // ReplicaSet name is typically "deployment-name-<hash>"
            // Strip the last hash segment
            parts := strings.Split(ref.Name, "-")
            if len(parts) > 1 {
                return strings.Join(parts[:len(parts)-1], "-")
            }
        }
    }
    return ""
}

// Full lookup — returns enriched ClientInfo
type PodInfo struct {
    PodName    string
    Namespace  string
    Deployment string
    NodeName   string
    Labels     map[string]string
}

func (c *PodCache) LookupEnriched(ip string) (*PodInfo, bool) {
    pod, ok := c.Lookup(ip)
    if !ok {
        return nil, false
    }
    return &PodInfo{
        PodName:    pod.Name,
        Namespace:  pod.Namespace,
        Deployment: GetDeploymentName(pod),
        NodeName:   pod.Spec.NodeName,
        Labels:     pod.Labels,
    }, true
}
```

## CPU Throttle Detection

```go
// Requires metrics-server installed in cluster
// Uses metrics.k8s.io API

import (
    metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
    metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

func CheckCPUThrottle(ctx context.Context, namespace, podName string) (throttled bool, err error) {
    // Get actual CPU usage
    metrics, err := metricsClient.MetricsV1beta1().PodMetricses(namespace).Get(ctx, podName, metav1.GetOptions{})
    if err != nil {
        return false, fmt.Errorf("get pod metrics: %w", err)
    }

    // Get CPU limit from pod spec
    pod, ok := podCache.Lookup(podIP)
    if !ok {
        return false, fmt.Errorf("pod not in cache")
    }

    for i, container := range pod.Spec.Containers {
        limit := container.Resources.Limits.Cpu()
        actual := metrics.Containers[i].Usage.Cpu()

        // If actual usage > 80% of limit, likely being throttled
        if actual.MilliValue() > limit.MilliValue()*80/100 {
            return true, nil
        }
    }
    return false, nil
}
```

## RBAC Requirements

Add to your deployment's ServiceAccount:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: redis-sentinel-admin
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["metrics.k8s.io"]
  resources: ["pods"]
  verbs: ["get", "list"]
```

## Testing with Fake Client

```go
import (
    "k8s.io/client-go/kubernetes/fake"
)

func TestPodMapper(t *testing.T) {
    fakeClient := fake.NewSimpleClientset(&corev1.Pod{
        ObjectMeta: metav1.ObjectMeta{
            Name:      "backend-worker-abc123",
            Namespace: "default",
        },
        Status: corev1.PodStatus{
            PodIP: "10.244.1.5",
        },
    })
    // Use fakeClient in place of real client
}
```

## Common Pitfalls

| Problem | Solution |
|---|---|
| Cache not ready on startup | Always call `WaitForCacheSync` before serving requests |
| DeleteFunc receives tombstone object | Always handle `DeletedFinalStateUnknown` in DeleteFunc |
| IP not in cache after pod starts | Wait ~1s for watch event to propagate; cache miss is OK, just return unknown |
| Multiple pods same IP (after restart) | UpdateFunc handles this — new pod replaces old in cache |
| Running locally without K8s | Gracefully disable K8s features when `InClusterConfig` and kubeconfig both fail |
