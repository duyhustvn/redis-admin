package handlers

import (
	"context"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/labstack/echo/v4"
)

// GetTopology godoc
//
//	@Summary		Get cluster topology
//	@Description	Returns the current Master, Replica, and Sentinel node states including per-node health, connected clients, uptime, and quorum status.
//	@Tags			topology
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=sentinel.TopologySnapshot}
//	@Failure		502	{object}	api.APIResponse{error=api.APIError}	"No sentinel reachable"
//	@Failure		503	{object}	api.APIResponse{error=api.APIError}	"No master or quorum not met"
//	@Failure		504	{object}	api.APIResponse{error=api.APIError}	"Redis timeout"
//	@Router			/topology [get]
func GetTopology(svc sentinel.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
		defer cancel()

		snapshot, err := svc.GetTopology(ctx)
		if err != nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, snapshot)
	}
}
