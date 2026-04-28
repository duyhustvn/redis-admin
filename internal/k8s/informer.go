// Package k8s provides a Pod informer cache for IP-to-Pod lookups.
package k8s

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// PodInfo holds enriched metadata about a Pod resolved from its IP.
type PodInfo struct {
	PodName    string
	Namespace  string
	Deployment string
	NodeName   string
	Labels     map[string]string
}

// PodCache maintains a thread-safe map from Pod IP to Pod object.
type PodCache struct {
	mu      sync.RWMutex
	ipToPod map[string]*corev1.Pod
}

// NewPodCache creates an empty PodCache.
func NewPodCache() *PodCache {
	return &PodCache{ipToPod: make(map[string]*corev1.Pod)}
}

// Lookup returns the Pod for ip, or (nil, false) on cache miss.
func (c *PodCache) Lookup(ip string) (*corev1.Pod, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pod, ok := c.ipToPod[ip]
	return pod, ok
}

// LookupEnriched returns enriched PodInfo for ip, or (nil, false) on cache miss.
func (c *PodCache) LookupEnriched(ip string) (*PodInfo, bool) {
	pod, ok := c.Lookup(ip)
	if !ok {
		return nil, false
	}
	return &PodInfo{
		PodName:    pod.Name,
		Namespace:  pod.Namespace,
		Deployment: deploymentName(pod),
		NodeName:   pod.Spec.NodeName,
		Labels:     pod.Labels,
	}, true
}

func (c *PodCache) set(pod *corev1.Pod) {
	if pod.Status.PodIP == "" {
		return
	}
	c.mu.Lock()
	c.ipToPod[pod.Status.PodIP] = pod
	c.mu.Unlock()
}

func (c *PodCache) delete(pod *corev1.Pod) {
	c.mu.Lock()
	delete(c.ipToPod, pod.Status.PodIP)
	c.mu.Unlock()
}

// BuildK8sClient returns an in-cluster client when running inside a Pod, or a
// kubeconfig-based client for local development.
func BuildK8sClient() (kubernetes.Interface, error) {
	cfg, err := loadK8sConfig()
	if err != nil {
		return nil, fmt.Errorf("load k8s config: %w", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create k8s client: %w", err)
	}
	return client, nil
}

// StartPodInformer wires up a shared informer for the given namespace that
// keeps cache up to date via watch events. It blocks until the initial list
// has been synced then returns. The informer continues running until ctx is done.
func StartPodInformer(ctx context.Context, client kubernetes.Interface, namespace string, cache *PodCache) error {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // resync period — rely on watch events, no forced periodic resync
		informers.WithNamespace(namespace),
	)

	podInformer := factory.Core().V1().Pods().Informer()
	_, err := podInformer.AddEventHandler(kcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*corev1.Pod); ok {
				cache.set(pod)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if pod, ok := newObj.(*corev1.Pod); ok {
				cache.set(pod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				// Handle tombstone objects emitted during resync.
				tombstone, ok := obj.(kcache.DeletedFinalStateUnknown)
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
	if err != nil {
		return fmt.Errorf("add event handler: %w", err)
	}

	factory.Start(ctx.Done())

	if !kcache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for pod cache sync")
	}
	return nil
}

func loadK8sConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

// deploymentName infers the Deployment name from a Pod's ReplicaSet owner reference.
func deploymentName(pod *corev1.Pod) string {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "ReplicaSet" {
			parts := strings.Split(ref.Name, "-")
			if len(parts) > 1 {
				return strings.Join(parts[:len(parts)-1], "-")
			}
		}
	}
	return ""
}
