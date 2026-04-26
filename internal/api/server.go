package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/duydinhle/redis-sentinel-admin/internal/config"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.uber.org/zap"
)

// Server wraps an Echo instance with lifecycle methods.
type Server struct {
	echo   *echo.Echo
	cfg    *config.Config
	logger *zap.Logger
}

// New creates and configures the Echo server with middleware and a custom error handler.
func New(cfg *config.Config, logger *zap.Logger) *Server {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodOptions},
	}))
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogMethod:   true,
		LogURI:      true,
		LogStatus:   true,
		LogLatency:  true,
		LogRemoteIP: true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			logger.Info("request",
				zap.String("method", v.Method),
				zap.String("uri", v.URI),
				zap.Int("status", v.Status),
				zap.Duration("latency", v.Latency),
				zap.String("remote_ip", v.RemoteIP),
			)
			return nil
		},
	}))

	e.HTTPErrorHandler = func(err error, c echo.Context) {
		var he *echo.HTTPError
		if errors.As(err, &he) {
			_ = Err(c, he.Code, "HTTP_ERROR", fmt.Sprintf("%v", he.Message))
			return
		}
		_ = Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
	}

	return &Server{echo: e, cfg: cfg, logger: logger}
}

// Echo returns the underlying Echo instance for route registration.
func (s *Server) Echo() *echo.Echo {
	return s.echo
}

// Start begins listening on the configured HTTP port. It blocks until the
// server is stopped; a shutdown-caused error is suppressed.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.cfg.HTTPPort)
	s.logger.Info("HTTP server starting", zap.String("addr", addr))
	if err := s.echo.Start(addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server: %w", err)
	}
	return nil
}

// Shutdown gracefully drains in-flight requests within the deadline of ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("HTTP server shutting down")
	return s.echo.Shutdown(ctx)
}
