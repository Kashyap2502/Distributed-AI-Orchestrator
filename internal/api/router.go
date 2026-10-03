package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	db            *pgxpool.Pool
	internalToken string
}

func NewRouter(db *pgxpool.Pool, internalToken string) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	server := &Server{db: db, internalToken: internalToken}
	router.POST("/v1/runs", server.createRun)
	router.GET("/v1/runs/:runID", server.getRun)
	router.POST("/internal/tasks/:taskID/result", server.completeTask)
	router.GET("/readyz", server.ready)
	return router
}

func (s *Server) ready(c *gin.Context) {
	if s.db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}
