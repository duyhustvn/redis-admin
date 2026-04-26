package handlers

import (
	"net/http"
	"strconv"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/api/dto"
	"github.com/duydinhle/redis-sentinel-admin/internal/diagnostics"
	"github.com/duydinhle/redis-sentinel-admin/internal/operations"
	"github.com/labstack/echo/v4"
)

// TriggerFailover godoc
//
//	@Summary		Graceful failover
//	@Description	Performs a lag-check, selects the best replica, then triggers SENTINEL FAILOVER. Use dry_run:true to preview without executing. Requires confirm:true when dry_run is false.
//	@Tags			operations
//	@Accept			json
//	@Produce		json
//	@Param			request	body		dto.FailoverRequest				true	"Failover options"
//	@Success		200		{object}	api.APIResponse{data=operations.FailoverResult}
//	@Failure		400		{object}	api.APIResponse{data=nil}	"confirm not set"
//	@Failure		503		{object}	api.APIResponse{data=nil}	"No eligible replica or quorum not met"
//	@Router			/ops/failover [post]
func TriggerFailover(svc operations.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req dto.FailoverRequest
		if err := c.Bind(&req); err != nil {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		}
		if !req.DryRun && !req.Confirm {
			return api.Err(c, http.StatusBadRequest, "CONFIRM_REQUIRED",
				"set confirm:true to execute failover, or dry_run:true to preview")
		}

		result, err := svc.Failover(c.Request().Context(), req.DryRun)
		if err != nil {
			if result != nil {
				// Return partial result with the error message so caller sees pre-check details.
				return c.JSON(http.StatusUnprocessableEntity, api.APIResponse{
					Data:  result,
					Error: &api.APIError{Code: "FAILOVER_ERROR", Message: err.Error()},
				})
			}
			return api.HandleErr(c, err)
		}
		return api.OK(c, result)
	}
}

// GetStaleLocks godoc
//
//	@Summary		Stale distributed lock detector
//	@Description	Scans keys matching a pattern and returns those with no TTL or with a TTL exceeding the stale threshold. Useful for detecting leaked distributed locks.
//	@Tags			diagnostics
//	@Produce		json
//	@Param			pattern			query	string	false	"Key pattern to scan (default lock:*)"	default(lock:*)
//	@Param			stale_threshold	query	int		false	"TTL in seconds above which a lock is considered stale; 0 = flag only keys with no TTL (default 300)"	default(300)
//	@Success		200	{object}	api.APIResponse{data=[]diagnostics.LockReport}
//	@Failure		502	{object}	api.APIResponse{data=nil}
//	@Router			/diagnostics/locks [get]
func GetStaleLocks(svc diagnostics.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		pattern := c.QueryParam("pattern")
		if pattern == "" {
			pattern = "lock:*"
		}

		staleThreshold := int64(300)
		if v := c.QueryParam("stale_threshold"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
				staleThreshold = n
			}
		}

		locks, err := svc.GetStaleLocks(c.Request().Context(), pattern, staleThreshold)
		if err != nil && locks == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, locks)
	}
}
