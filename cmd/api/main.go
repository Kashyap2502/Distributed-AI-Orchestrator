package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ai-orchestrator/service/internal/api"
	"github.com/ai-orchestrator/service/internal/config"
	"github.com/ai-orchestrator/service/internal/dispatch"
	"github.com/ai-orchestrator/service/internal/store"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--healthcheck" {
		client := &http.Client{Timeout: time.Second}
		response, err := client.Get("http://127.0.0.1:8080/healthz")
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			log.Fatalf("health check returned %s", response.Status)
		}
		return
	}

	cfg := config.Load()
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startupCancel()
	db, err := store.Open(startupCtx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go dispatch.RunOutboxPublisher(ctx, db, cfg.RabbitMQURL)

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewRouter(db, cfg.InternalToken),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	log.Printf("API listening on %s", server.Addr)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server: %v", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatal(err)
	}
}
