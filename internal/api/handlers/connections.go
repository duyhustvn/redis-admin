package handlers

import (
	"context"
	"time"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/connection"
	"github.com/labstack/echo/v4"
)

// GetConnections godoc
//
//	@Summary		List client connections per node
//	@Description	Runs CLIENT LIST on every Redis node, groups connections by source IP, and enriches each entry with Kubernetes Pod metadata (name, namespace, deployment) where available. Also reports which connected pods are CPU-throttled.
//	@Tags			connections
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]connection.NodeConnections}
//	@Failure		502	{object}	api.APIResponse{error=api.APIError}	"No sentinel reachable"
//	@Failure		503	{object}	api.APIResponse{error=api.APIError}	"No master found"
//	@Failure		504	{object}	api.APIResponse{error=api.APIError}	"Redis timeout"
//	@Router			/connections [get]
func GetConnections(svc connection.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 10*time.Second)
		defer cancel()

		result, err := svc.GetConnections(ctx)
		if err != nil && result == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, result)
	}
}

// GetDistribution godoc
//
//	@Summary		Read/write distribution per replica
//	@Description	Fetches INFO commandstats from every replica, classifies each command as read or write, and computes percentages. A replica is flagged as overloaded when it carries more than 80% of total cluster read traffic.
//	@Tags			connections
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]connection.ReplicaDistribution}
//	@Failure		502	{object}	api.APIResponse{error=api.APIError}	"No sentinel reachable"
//	@Failure		503	{object}	api.APIResponse{error=api.APIError}	"No master found"
//	@Failure		504	{object}	api.APIResponse{error=api.APIError}	"Redis timeout"
//	@Router			/connections/distribution [get]
func GetDistribution(svc connection.Service) echo.HandlerFunc {
	return func(c echo.Context) error {
		ctx, cancel := context.WithTimeout(c.Request().Context(), 10*time.Second)
		defer cancel()

		result, err := svc.GetDistribution(ctx)
		if err != nil && result == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, result)
	}
}
