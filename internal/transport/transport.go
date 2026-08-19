package transport

import (
	"context"
	"net/http"
	"net/url"
)

// Request describes an immutable REST request.
type Request struct {
	Operation string
	Method    string
	Path      string
	Query     url.Values
	Header    http.Header
	Raw       bool
}

// Executor executes REST requests.
type Executor interface {
	Do(ctx context.Context, request Request, result any) ([]byte, error)
}
