package handlers

import (
	"net/http"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/api/dto"
	"github.com/duydinhle/redis-sentinel-admin/internal/operations"
	"github.com/labstack/echo/v4"
)

// GetConfigDiff godoc
//
//	@Summary		Cross-node config diff
//	@Description	Runs CONFIG GET * on every node and returns keys whose values differ across nodes. Only drifted keys are included in the response.
//	@Tags			config
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]operations.ConfigDiff}
//	@Failure		502	{object}	api.APIResponse{data=nil}	"Node unreachable"
//	@Router			/config/diff [get]
func GetConfigDiff(svc operations.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		diffs, err := svc.GetConfigDiff(c.Request().Context())
		if err != nil && diffs == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, diffs)
	}
}

// GetConfigAudit godoc
//
//	@Summary		Config change audit log
//	@Description	Returns the in-memory ring buffer of the last 500 CONFIG SET operations, including old value, new value, node address, and remote IP.
//	@Tags			config
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]operations.AuditEntry}
//	@Router			/config/audit [get]
func GetConfigAudit(svc operations.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		return api.OK(c, svc.GetAuditLog())
	}
}

// SetConfig godoc
//
//	@Summary		Set a Redis config value
//	@Description	Applies CONFIG SET on a specific node and records the change in the audit log. Requires confirm:true in the request body.
//	@Tags			config
//	@Accept			json
//	@Produce		json
//	@Param			request	body		dto.ConfigSetRequest	true	"CONFIG SET parameters"
//	@Success		200		{object}	api.APIResponse{data=nil}
//	@Failure		400		{object}	api.APIResponse{data=nil}	"confirm not set or invalid request"
//	@Failure		502		{object}	api.APIResponse{data=nil}	"Node unreachable"
//	@Router			/config/set [post]
func SetConfig(svc operations.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req dto.ConfigSetRequest
		if err := c.Bind(&req); err != nil {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		}
		if !req.Confirm {
			return api.Err(c, http.StatusBadRequest, "CONFIRM_REQUIRED",
				"set confirm:true to apply this config change")
		}
		if req.NodeAddr == "" || req.Key == "" {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST",
				"node_addr and key are required")
		}

		remoteIP := c.RealIP()
		if err := svc.SetConfig(c.Request().Context(), req.NodeAddr, req.Key, req.Value, remoteIP); err != nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, nil)
	}
}
