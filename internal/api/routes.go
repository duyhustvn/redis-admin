package api

import (
	echoSwagger "github.com/swaggo/echo-swagger"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// Deps holds pre-built handler functions injected at startup.
// Keeping handlers out of this package avoids an import cycle:
// api/handlers → api (response helpers) and api (routes) → api/handlers.
type Deps struct {
	Healthz     echo.HandlerFunc
	Readyz      echo.HandlerFunc
	GetTopology echo.HandlerFunc
	EventStream echo.HandlerFunc
	Logger      *zap.Logger
}

// RegisterRoutes mounts all Phase 1 routes on e.
// Health probes are at the root level (used by K8s); API routes are under /api/v1.
func RegisterRoutes(e *echo.Echo, deps Deps) {
	e.GET("/healthz", deps.Healthz)
	e.GET("/readyz", deps.Readyz)
	e.GET("/swagger/*", echoSwagger.WrapHandler)

	v1 := e.Group("/api/v1")
	v1.GET("/topology", deps.GetTopology)
	v1.GET("/events/stream", deps.EventStream)
}
