package server

import (
	"context"
	"fmt"
	"mini-tsdb/internal/tsdb"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
)

// ingest payload
type ingestReq struct {
	Name      string  `json:"name" validate:"required"`
	Timestamp *int64  `json:"timestamp"` // optional epoch ms
	Value     float64 `json:"value"`
}

func ingestHandler(db *tsdb.TSDB) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req ingestReq
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "bad request"})
		}
		ts := time.Now().UnixMilli()
		if req.Timestamp != nil {
			ts = *req.Timestamp
		}
		if err := db.Append(req.Name, ts, req.Value); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusAccepted, map[string]string{"status": "ok"})
	}
}

// query handler
func queryHandler(db *tsdb.TSDB) echo.HandlerFunc {
	return func(c echo.Context) error {
		name := c.QueryParam("metric")
		from := c.QueryParam("from")
		to := c.QueryParam("to")
		// parse ints (epoch ms) - simple parse, return errors if missing
		// ... kept compact in canvas
		q, err := db.QueryRange(name, parseInt64OrZero(from), parseInt64OrZero(to))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusOK, q)
	}
}

func parseInt64OrZero(s string) int64 {
	if s == "" {
		return 0
	}
	var v int64
	_, _ = fmt.Sscan(s, &v)
	return v
}

type Server struct {
	e  *echo.Echo
	db *tsdb.TSDB
}

func New(e *echo.Echo, db *tsdb.TSDB) *Server {
	s := &Server{e: e, db: db}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.e.POST("/metrics", ingestHandler(s.db))
	s.e.GET("/query", queryHandler(s.db))
	s.e.GET("/health", func(c echo.Context) error { return c.JSON(http.StatusOK, map[string]string{"status": "ok"}) })
}

func (s *Server) Start(addr string) error {
	return s.e.Start(addr)
}

func (s *Server) Shutdown(ctx context.Context) error {
	// stop HTTP
	shutdownCh := make(chan error, 1)
	go func() { shutdownCh <- s.e.Shutdown(ctx) }()

	select {
	case err := <-shutdownCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
