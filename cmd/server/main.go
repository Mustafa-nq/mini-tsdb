package main

import (
	"context"
	"log"
	server "mini-tsdb/internal/ingest"
	"mini-tsdb/internal/tsdb"
	"os"
	"os/signal"
	"time"

	"github.com/labstack/echo/v4"
)

func main() {
	cfg := tsdb.DefaultConfig()
	db := tsdb.NewTSDB(cfg)

	e := echo.New()
	srv := server.New(e, db)
	go func() {
		if err := srv.Start(":8080"); err != nil {
			log.Fatalf("server start: %v", err)
		}
	}()

	// graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	db.Close()
}
