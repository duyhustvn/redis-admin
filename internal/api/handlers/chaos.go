package handlers

import (
	"net/http"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/api/dto"
	"github.com/duydinhle/redis-sentinel-admin/internal/chaos"
	"github.com/labstack/echo/v4"
)

// SeedData godoc
//
//	@Summary		Seed dummy data
//	@Description	Generates configurable dummy keys on the master node. Supports string, hash, list, set, and zset types. Requires confirm:true.
//	@Tags			chaos
//	@Accept			json
//	@Produce		json
//	@Param			request	body		dto.SeedRequest				true	"Seed parameters"
//	@Success		200		{object}	api.APIResponse{data=chaos.SeedResult}
//	@Failure		400		{object}	api.APIResponse{data=nil}	"confirm not set or invalid request"
//	@Failure		502		{object}	api.APIResponse{data=nil}	"Master unreachable"
//	@Router			/ops/chaos/seed [post]
func SeedData(svc chaos.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req dto.SeedRequest
		if err := c.Bind(&req); err != nil {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		}
		if !req.Confirm {
			return api.Err(c, http.StatusBadRequest, "CONFIRM_REQUIRED",
				"set confirm:true to seed data")
		}
		if req.KeyCount <= 0 {
			req.KeyCount = 100
		}
		if req.KeyCount > 100_000 {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST",
				"key_count must not exceed 100 000")
		}

		result, err := svc.Seed(
			c.Request().Context(),
			req.KeyPrefix,
			req.KeyCount,
			req.ValueSize,
			req.KeyType,
			req.TTLSec,
		)
		if err != nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, result)
	}
}

// FlushData godoc
//
//	@Summary		Pattern-scoped cluster flush
//	@Description	Deletes all keys matching the given pattern via SCAN+DEL on the master. Never calls FLUSHALL. Requires confirm:true.
//	@Tags			chaos
//	@Accept			json
//	@Produce		json
//	@Param			request	body		dto.FlushRequest			true	"Flush parameters"
//	@Success		200		{object}	api.APIResponse{data=chaos.FlushResult}
//	@Failure		400		{object}	api.APIResponse{data=nil}	"confirm not set"
//	@Failure		502		{object}	api.APIResponse{data=nil}	"Master unreachable"
//	@Router			/ops/chaos/flush [post]
func FlushData(svc chaos.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req dto.FlushRequest
		if err := c.Bind(&req); err != nil {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		}
		if !req.Confirm {
			return api.Err(c, http.StatusBadRequest, "CONFIRM_REQUIRED",
				"set confirm:true to flush keys")
		}

		result, err := svc.Flush(c.Request().Context(), req.Pattern)
		if err != nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, result)
	}
}

// ChaosFailover godoc
//
//	@Summary		Chaos failover trigger
//	@Description	Triggers a failover without pre-checks. mode=sentinel sends SENTINEL FAILOVER directly; mode=pod deletes the named Redis pod via K8s to force failover. Requires confirm:true.
//	@Tags			chaos
//	@Accept			json
//	@Produce		json
//	@Param			request	body		dto.ChaosFailoverRequest		true	"Chaos failover parameters"
//	@Success		200		{object}	api.APIResponse{data=chaos.ChaosFailoverResult}
//	@Failure		400		{object}	api.APIResponse{data=nil}	"confirm not set or invalid request"
//	@Failure		503		{object}	api.APIResponse{data=nil}	"No reachable sentinel or K8s unavailable"
//	@Router			/ops/chaos/failover [post]
func ChaosFailover(svc chaos.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req dto.ChaosFailoverRequest
		if err := c.Bind(&req); err != nil {
			return api.Err(c, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		}
		if !req.Confirm {
			return api.Err(c, http.StatusBadRequest, "CONFIRM_REQUIRED",
				"set confirm:true to trigger chaos failover")
		}
		if req.Mode == "" {
			req.Mode = "sentinel"
		}

		result, err := svc.TriggerChaosFailover(
			c.Request().Context(),
			req.Mode,
			req.PodNamespace,
			req.PodName,
		)
		if err != nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, result)
	}
}
