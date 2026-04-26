package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/diagnostics"
	"github.com/labstack/echo/v4"
)

// GetSlowlog godoc
//
//	@Summary		Get slow commands across cluster
//	@Description	Pulls SLOWLOG GET from all nodes (master + replicas), aggregates entries, and returns them sorted by execution time descending. Use the limit parameter to control how many entries are returned.
//	@Tags			diagnostics
//	@Produce		json
//	@Param			limit	query		int	false	"Max entries to return (default 50, max 200)"	default(50)
//	@Success		200		{object}	api.APIResponse{data=[]diagnostics.SlowlogEntry}
//	@Failure		400		{object}	api.APIResponse{error=api.APIError}	"Invalid limit"
//	@Failure		502		{object}	api.APIResponse{error=api.APIError}	"No sentinel reachable"
//	@Failure		504		{object}	api.APIResponse{error=api.APIError}	"Redis timeout"
//	@Router			/diagnostics/slowlog [get]
func GetSlowlog(svc diagnostics.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		limit := 50
		if v := c.QueryParam("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 || n > 200 {
				return api.Err(c, http.StatusBadRequest, "INVALID_PARAM",
					"limit must be an integer between 1 and 200")
			}
			limit = n
		}

		ctx, cancel := context.WithTimeout(c.Request().Context(), 15*time.Second)
		defer cancel()

		entries, err := svc.GetSlowlog(ctx, limit)
		if err != nil && entries == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, entries)
	}
}

// GetPipelineStats godoc
//
//	@Summary		Get pipeline and transaction stats per node
//	@Description	Collects EXECABORT error counts, rejected_calls, maximum input buffer size, and oversized pipeline client counts (qbuf > 1 MiB) from every node.
//	@Tags			diagnostics
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]diagnostics.PipelineReport}
//	@Failure		502	{object}	api.APIResponse{error=api.APIError}	"No sentinel reachable"
//	@Failure		504	{object}	api.APIResponse{error=api.APIError}	"Redis timeout"
//	@Router			/diagnostics/pipeline [get]
func GetPipelineStats(svc diagnostics.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 15*time.Second)
		defer cancel()

		reports, err := svc.GetPipelineStats(ctx)
		if err != nil && reports == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, reports)
	}
}
