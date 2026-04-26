package handlers

import (
	"net/http"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/api/dto"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/labstack/echo/v4"
)

// Healthz godoc
//
//	@Summary		Liveness probe
//	@Description	Returns 200 OK when the process is alive. Used by Kubernetes liveness probe.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=dto.HealthResponse}
//	@Router			/healthz [get]
func Healthz() echo.HandlerFunc {
	return func(c echo.Context) error {
		return api.OK(c, dto.HealthResponse{
			Status:    "ok",
			Timestamp: time.Now().UTC(),
		})
	}
}

// Readyz godoc
//
//	@Summary		Readiness probe
//	@Description	Returns 200 OK when the service can reach the Redis master via Sentinel. Used by Kubernetes readiness probe.
//	@Tags			health
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=dto.HealthResponse}
//	@Failure		503	{object}	api.APIResponse{error=api.APIError}	"Master unreachable"
//	@Failure		504	{object}	api.APIResponse{error=api.APIError}	"Redis timeout"
//	@Router			/readyz [get]
func Readyz(svc sentinel.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := svc.IsReady(c.Request().Context()); err != nil {
			return api.HandleErr(c, err)
		}
		return c.JSON(http.StatusOK, api.APIResponse{
			Data: dto.HealthResponse{
				Status:    "ok",
				Timestamp: time.Now().UTC(),
			},
			Error: nil,
		})
	}
}
