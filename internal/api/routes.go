package api

import (
	echoSwagger "github.com/swaggo/echo-swagger"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// Deps holds pre-built handler functions injected at startup.
// Using echo.HandlerFunc here instead of service interfaces avoids the import
// cycle: api/handlers → api (response helpers) and api (routes) → api/handlers.
type Deps struct {
	// Phase 1
	Healthz     echo.HandlerFunc
	Readyz      echo.HandlerFunc
	GetTopology echo.HandlerFunc
	EventStream echo.HandlerFunc

	// Phase 2
	GetConnections  echo.HandlerFunc
	GetDistribution echo.HandlerFunc
	GetSlowlog      echo.HandlerFunc
	GetPipeline     echo.HandlerFunc

	// Phase 3
	GetMemory    echo.HandlerFunc
	StreamBigkeys echo.HandlerFunc
	GetHotkeys   echo.HandlerFunc
	GetTTLReport echo.HandlerFunc

	Logger *zap.Logger
}

// RegisterRoutes mounts all routes on e.
// Health probes are at root level (used by K8s); API routes are under /api/v1.
func RegisterRoutes(e *echo.Echo, deps Deps) {
	e.GET("/healthz", deps.Healthz)
	e.GET("/readyz", deps.Readyz)
	e.GET("/swagger/*", echoSwagger.WrapHandler)

	v1 := e.Group("/api/v1")

	// Phase 1
	v1.GET("/topology", deps.GetTopology)
	v1.GET("/events/stream", deps.EventStream)

	// Phase 2
	if deps.GetConnections != nil {
		v1.GET("/connections", deps.GetConnections)
	}
	if deps.GetDistribution != nil {
		v1.GET("/connections/distribution", deps.GetDistribution)
	}
	if deps.GetSlowlog != nil {
		v1.GET("/diagnostics/slowlog", deps.GetSlowlog)
	}
	if deps.GetPipeline != nil {
		v1.GET("/diagnostics/pipeline", deps.GetPipeline)
	}

	// Phase 3
	if deps.GetMemory != nil {
		v1.GET("/diagnostics/memory", deps.GetMemory)
	}
	if deps.StreamBigkeys != nil {
		v1.GET("/keys/bigkeys", deps.StreamBigkeys)
	}
	if deps.GetHotkeys != nil {
		v1.GET("/keys/hotkeys", deps.GetHotkeys)
	}
	if deps.GetTTLReport != nil {
		v1.GET("/keys/ttl-report", deps.GetTTLReport)
	}
}
