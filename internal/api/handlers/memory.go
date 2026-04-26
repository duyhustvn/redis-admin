package handlers

import (
	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/diagnostics"
	"github.com/labstack/echo/v4"
)

// GetMemory godoc
//
//	@Summary		Memory health report
//	@Description	Returns memory fragmentation, eviction rate, and persistence health for every cluster node.
//	@Tags			diagnostics
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]diagnostics.MemoryReport}
//	@Failure		500	{object}	api.APIResponse{data=nil}
//	@Router			/diagnostics/memory [get]
func GetMemory(svc diagnostics.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		reports, err := svc.GetMemory(c.Request().Context())
		if err != nil && reports == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, reports)
	}
}
