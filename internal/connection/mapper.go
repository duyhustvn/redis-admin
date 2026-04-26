// Package connection provides per-node client connection monitoring and
// read/write distribution analysis.
package connection

import (
	"strings"

	"github.com/duydinhle/redis-sentinel-admin/internal/k8s"
)

// enrichWithPodInfo looks up the source IP of info in the pod cache and fills
// PodName, Namespace, and Deployment when a match is found.
func enrichWithPodInfo(info *ClientInfo, cache *k8s.PodCache) {
	if cache == nil {
		return
	}
	ip := sourceIP(info.SourceAddr)
	podInfo, ok := cache.LookupEnriched(ip)
	if !ok {
		return
	}
	info.PodName = podInfo.PodName
	info.Namespace = podInfo.Namespace
	info.Deployment = podInfo.Deployment
}

// sourceIP extracts the IP portion from an "ip:port" string.
func sourceIP(addr string) string {
	if idx := strings.LastIndexByte(addr, ':'); idx >= 0 {
		return addr[:idx]
	}
	return addr
}
