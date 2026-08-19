package rest

import (
	"context"
	"fmt"
	"github.com/k4k3ru-hub/backpack/go/internal/transport"
	"net/http"
	"strings"
)

type SystemStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
type SystemClient struct{ executor transport.Executor }

// GetStatus gets the exchange-reported system state.
//
// Version:
//   - 2026-08-19: Added.
func (c *SystemClient) GetStatus(ctx context.Context) (*SystemStatus, error) {
	var out SystemStatus
	if err := get(ctx, c.executor, "get system status", "/api/v1/status", nil, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack system status: %w", err)
	}
	return &out, nil
}

// Ping checks public REST connectivity and returns the response text.
//
// Version:
//   - 2026-08-19: Added.
func (c *SystemClient) Ping(ctx context.Context) (string, error) {
	body, err := c.executor.Do(ctx, transport.Request{Operation: "ping", Method: http.MethodGet, Path: "/api/v1/ping", Raw: true}, nil)
	if err != nil {
		return "", fmt.Errorf("failed to ping backpack: %w", err)
	}
	return strings.TrimSpace(string(body)), nil
}
