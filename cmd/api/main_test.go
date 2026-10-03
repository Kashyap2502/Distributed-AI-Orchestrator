package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ai-orchestrator/service/internal/api"
)

func TestHealthz(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	api.NewRouter(nil, "").ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}
}
