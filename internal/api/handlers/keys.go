package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/duydinhle/redis-sentinel-admin/internal/api"
	"github.com/duydinhle/redis-sentinel-admin/internal/keys"
	"github.com/labstack/echo/v4"
)

const defaultBigKeyThreshold = 1 << 20 // 1 MiB
const defaultTopN = 20

// StreamBigkeys godoc
//
//	@Summary		Big key scanner (SSE)
//	@Description	Streams big keys found via non-blocking SCAN as Server-Sent Events. Each event is a KeyReport JSON object. A final "summary" event contains per-namespace aggregates.
//	@Tags			keys
//	@Produce		text/event-stream
//	@Param			threshold_bytes	query	int	false	"Minimum key size in bytes to report (default 1048576)"
//	@Success		200	{string}	string	"stream of SSE events"
//	@Router			/keys/bigkeys [get]
func StreamBigkeys(svc keys.KeysService) echo.HandlerFunc {
	return func(c echo.Context) error {
		threshold := int64(defaultBigKeyThreshold)
		if v := c.QueryParam("threshold_bytes"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				threshold = n
			}
		}

		w := c.Response().Writer
		c.Response().Header().Set("Content-Type", "text/event-stream")
		c.Response().Header().Set("Cache-Control", "no-cache")
		c.Response().Header().Set("X-Accel-Buffering", "no")
		c.Response().WriteHeader(http.StatusOK)

		flush := func() {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}

		sendEvent := func(event string, data interface{}) {
			b, _ := json.Marshal(data)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
			flush()
		}

		onKey := func(kr keys.KeyReport) {
			sendEvent("key", kr)
		}

		stats, err := svc.ScanBigKeys(c.Request().Context(), threshold, onKey)
		if err != nil {
			sendEvent("error", map[string]string{"message": err.Error()})
			return nil
		}

		sendEvent("summary", stats)
		sendEvent("done", map[string]string{"status": "complete"})
		return nil
	}
}

// GetHotkeys godoc
//
//	@Summary		Hot key detector (LFU)
//	@Description	Returns the top-N keys by LFU access frequency. Requires maxmemory-policy to be an *-lfu variant.
//	@Tags			keys
//	@Produce		json
//	@Param			top	query	int	false	"Number of hot keys to return (default 20)"
//	@Success		200	{object}	api.APIResponse{data=[]keys.HotKeyReport}
//	@Failure		500	{object}	api.APIResponse{data=nil}
//	@Router			/keys/hotkeys [get]
func GetHotkeys(svc keys.KeysService) echo.HandlerFunc {
	return func(c echo.Context) error {
		topN := defaultTopN
		if v := c.QueryParam("top"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
				topN = n
			}
		}

		reports, err := svc.GetHotkeys(c.Request().Context(), topN)
		if err != nil && reports == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, reports)
	}
}

// GetTTLReport godoc
//
//	@Summary		TTL health report
//	@Description	Returns per-namespace percentage of keys without a TTL set.
//	@Tags			keys
//	@Produce		json
//	@Success		200	{object}	api.APIResponse{data=[]keys.NamespaceTTLReport}
//	@Failure		500	{object}	api.APIResponse{data=nil}
//	@Router			/keys/ttl-report [get]
func GetTTLReport(svc keys.KeysService) echo.HandlerFunc {
	return func(c echo.Context) error {
		reports, err := svc.GetTTLReport(c.Request().Context())
		if err != nil && reports == nil {
			return api.HandleErr(c, err)
		}
		return api.OK(c, reports)
	}
}
