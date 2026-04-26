package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/duydinhle/redis-sentinel-admin/internal/sentinel"
	"github.com/labstack/echo/v4"
)

// EventStream godoc
//
//	@Summary		Live Sentinel event stream
//	@Description	Server-Sent Events stream of real-time Sentinel events (+sdown, -sdown, +odown, +failover-*). Connect and keep the connection open to receive events as they happen. Each event line is: `event: sentinel\ndata: {json}\n\n`.
//	@Tags			events
//	@Produce		text/event-stream
//	@Success		200	{string}	string	"SSE stream"
//	@Router			/events/stream [get]
func EventStream(bus *sentinel.EventBus) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set(echo.HeaderContentType, "text/event-stream")
		c.Response().Header().Set("Cache-Control", "no-cache")
		c.Response().Header().Set("Connection", "keep-alive")
		c.Response().Header().Set("X-Accel-Buffering", "no") // disable nginx buffering
		c.Response().WriteHeader(http.StatusOK)

		sub := bus.Subscribe()
		defer bus.Unsubscribe(sub)

		ctx := c.Request().Context()
		for {
			select {
			case evt, ok := <-sub:
				if !ok {
					return nil
				}
				data, err := json.Marshal(evt)
				if err != nil {
					continue
				}
				fmt.Fprintf(c.Response(), "event: sentinel\ndata: %s\n\n", data)
				c.Response().Flush()
			case <-ctx.Done():
				return nil
			}
		}
	}
}
