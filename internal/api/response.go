// Package api contains the Echo HTTP server, route registration, and shared
// response helpers.
package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/labstack/echo/v4"
)

// APIResponse is the unified envelope for every JSON response.
type APIResponse struct {
	Data  interface{} `json:"data"`
	Error *APIError   `json:"error"`
}

// APIError carries a machine-readable code alongside a human-readable message.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// OK writes a 200 JSON response with data wrapped in APIResponse.
func OK(c echo.Context, data interface{}) error {
	return c.JSON(http.StatusOK, APIResponse{Data: data, Error: nil})
}

// Err writes a JSON error response with the given HTTP status.
func Err(c echo.Context, status int, code, message string) error {
	return c.JSON(status, APIResponse{
		Data:  nil,
		Error: &APIError{Code: code, Message: message},
	})
}

// HandleErr maps domain errors to appropriate HTTP status codes and writes the
// error response. Unknown errors produce 500.
func HandleErr(c echo.Context, err error) error {
	switch {
	case errors.Is(err, sentinel.ErrNoMaster):
		return Err(c, http.StatusServiceUnavailable, "NO_MASTER", err.Error())
	case errors.Is(err, sentinel.ErrQuorumNotMet):
		return Err(c, http.StatusServiceUnavailable, "QUORUM_NOT_MET", err.Error())
	case errors.Is(err, sentinel.ErrNodeUnreachable):
		return Err(c, http.StatusBadGateway, "NODE_UNREACHABLE", err.Error())
	case errors.Is(err, sentinel.ErrFlapping):
		return Err(c, http.StatusServiceUnavailable, "NODE_FLAPPING", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return Err(c, http.StatusGatewayTimeout, "TIMEOUT", "Redis operation timed out")
	case errors.Is(err, context.Canceled):
		return Err(c, http.StatusGatewayTimeout, "TIMEOUT", "request cancelled")
	default:
		return Err(c, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
	}
}
