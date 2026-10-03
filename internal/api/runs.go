package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type createRunRequest struct {
	UserID string `json:"user_id" binding:"required"`
	Prompt string `json:"prompt" binding:"required"`
}

type queuedTask struct {
	TaskID string `json:"task_id"`
	RunID  string `json:"run_id"`
	Prompt string `json:"prompt"`
	Kind   string `json:"kind"`
}

func (s *Server) createRun(c *gin.Context) {
	if s.db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service not ready"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64*1024)
	var request createRunRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id and prompt are required"})
		return
	}
	userID, err := uuid.Parse(request.UserID)
	if err != nil || strings.TrimSpace(request.Prompt) == "" || len(request.Prompt) > 20000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id or prompt length"})
		return
	}

	runID, taskID := uuid.New(), uuid.New()
	message, err := json.Marshal(queuedTask{
		TaskID: taskID.String(), RunID: runID.String(),
		Prompt: strings.TrimSpace(request.Prompt), Kind: "logical",
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create task"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "storage unavailable"})
		return
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, userID)
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO runs (id, user_id, prompt, status) VALUES ($1, $2, $3, 'queued')`, runID, userID, strings.TrimSpace(request.Prompt))
	}
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO tasks (id, run_id, capability, status) VALUES ($1, $2, 'logical', 'queued')`, taskID, runID)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events (task_id, payload) VALUES ($1, $2)`, taskID, message)
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not persist run"})
		return
	}
	if err := tx.Commit(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not persist run"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"run_id": runID.String(), "task_id": taskID.String(), "status": "queued"})
}

func (s *Server) getRun(c *gin.Context) {
	if s.db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service not ready"})
		return
	}
	runID, err := uuid.Parse(c.Param("runID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid run_id"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	var status string
	var result *string
	err = s.db.QueryRow(ctx, `SELECT status, result FROM runs WHERE id = $1`, runID).Scan(&status, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "storage unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"run_id": runID.String(), "status": status, "result": result})
}

type taskResultRequest struct {
	Status string `json:"status" binding:"required,oneof=completed failed"`
	Result string `json:"result"`
	Error  string `json:"error"`
}

func (s *Server) completeTask(c *gin.Context) {
	if s.internalToken != "" && c.GetHeader("Authorization") != "Bearer "+s.internalToken {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	if s.db == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "service not ready"})
		return
	}
	taskID, err := uuid.Parse(c.Param("taskID"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task_id"})
		return
	}
	var request taskResultRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task result"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "storage unavailable"})
		return
	}
	defer tx.Rollback(ctx)
	var runID uuid.UUID
	err = tx.QueryRow(ctx, `UPDATE tasks SET status = $2, result = $3, error = $4, completed_at = now() WHERE id = $1 AND status NOT IN ('completed', 'failed') RETURNING run_id`, taskID, request.Status, nullableString(request.Result), nullableString(request.Error)).Scan(&runID)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusOK, gin.H{"status": "already_completed"})
		return
	}
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	_, err = tx.Exec(ctx, `UPDATE runs SET status = $2, result = $3, updated_at = now() WHERE id = $1`, runID, request.Status, nullableString(request.Result))
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not save task result"})
		return
	}
	c.Status(http.StatusNoContent)
}

func nullableString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
