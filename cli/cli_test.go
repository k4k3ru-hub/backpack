package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type httpClientFunc func(*http.Request) (*http.Response, error)

// Do executes the injected HTTP client function.
//
// Version:
//   - 2026-08-20: Added.
func (f httpClientFunc) Do(request *http.Request) (*http.Response, error) {
	return f(request)
}

// TestRunMarkets verifies the public Markets REST command.
//
// Version:
//   - 2026-08-20: Added.
func TestRunMarkets(t *testing.T) {
	httpClient := httpClientFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/v1/markets" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		marketTypes := request.URL.Query()["marketType"]
		if len(marketTypes) != 2 || marketTypes[0] != "SPOT" || marketTypes[1] != "PERP" {
			t.Fatalf("marketType = %v", marketTypes)
		}
		body := `[{"symbol":"SOL_USDC","baseSymbol":"SOL","quoteSymbol":"USDC","marketType":"SPOT","filters":{"price":{"minPrice":"0.01","maxPrice":"10000","tickSize":"0.01"},"quantity":{"minQuantity":"0.001","maxQuantity":"1000","stepSize":"0.001"}},"orderBookState":"Open","createdAt":"2026-08-20T00:00:00Z","visible":true}]`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := Run(
		context.Background(),
		[]string{"rest", "markets", "--market-types", "SPOT,PERP", "--base-url", "http://backpack.test"},
		&stdout,
		&stderr,
		&Option{HTTPClient: httpClient},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), `"symbol": "SOL_USDC"`) || !strings.Contains(stdout.String(), `"tickSize": "0.01"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

// TestRunValidation verifies CLI and REST request validation.
//
// Version:
//   - 2026-08-20: Added.
func TestRunValidation(t *testing.T) {
	var output bytes.Buffer
	if err := Run(context.Background(), []string{"unknown"}, &output, &output, nil); err == nil {
		t.Fatal("expected command error")
	}
	if err := Run(context.Background(), []string{"rest", "markets", "--market-types", "SPOT,,PERP"}, &output, &output, nil); err == nil {
		t.Fatal("expected market types error")
	}
	if err := Run(nil, nil, &output, &output, nil); err == nil {
		t.Fatal("expected context error")
	}
}
