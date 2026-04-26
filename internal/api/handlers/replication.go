package handlers

import (
	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/replication"
	"github.com/labstack/echo/v4"
)

// GetReplicationLag godoc
//
//	@Summary		Replication lag per replica
//	@Description	Returns the replication offset lag in bytes for every replica, plus a ring-buffer trend of recent samples and a promotion score (0–100, higher = better failover candidate).
//	@Tags			replication
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]replication.ReplicaLag}
//	@Failure		502	{object}	api.APIResponse{data=nil}	"Node unreachable"
//	@Failure		503	{object}	api.APIResponse{data=nil}	"No master"
//	@Router			/replication/lag [get]
func GetReplicationLag(svc replication.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		lags, err := svc.GetReplicationLag(c.Request().Context())
		if err != nil && lags == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, lags)
	}
}

// GetResyncStats godoc
//
//	@Summary		Replication backlog and resync stats
//	@Description	Returns full-resync counters and backlog utilisation per node. Raises an alert when the backlog exceeds 90% capacity and includes a sizing advisory.
//	@Tags			replication
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]replication.ResyncReport}
//	@Failure		502	{object}	api.APIResponse{data=nil}	"Node unreachable"
//	@Failure		503	{object}	api.APIResponse{data=nil}	"No master"
//	@Router			/replication/resync-stats [get]
func GetResyncStats(svc replication.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		reports, err := svc.GetResyncStats(c.Request().Context())
		if err != nil && reports == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, reports)
	}
}
