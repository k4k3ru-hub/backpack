package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/k4k3ru-hub/backpack/go/internal/transport"
)

const (
	DefaultBaseURL      = "https://api.backpack.exchange"
	defaultMaxBodyBytes = int64(8 << 20)
	defaultErrorBytes   = int64(64 << 10)
)

var ErrInvalidParameter = errors.New(`err_code="invalid_parameter"`)

// HTTPClient executes HTTP requests.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// ClientOption configures a REST client.
type ClientOption func(*clientConfig) error

type clientConfig struct {
	baseURL      string
	httpClient   HTTPClient
	maxBodyBytes int64
	userAgent    string
}

// Client is the REST composition root.
type Client struct {
	baseURL      string
	httpClient   HTTPClient
	maxBodyBytes int64
	userAgent    string
	markets      *MarketsClient
	trades       *TradesClient
	tickers      *TickersClient
	futures      *FuturesClient
	system       *SystemClient
}

// WithBaseURL sets the REST base URL.
//
// Version:
//   - 2026-08-19: Added.
func WithBaseURL(value string) ClientOption {
	return func(c *clientConfig) error {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("failed to configure rest client: %w: base_url=empty", ErrInvalidParameter)
		}
		u, err := url.Parse(value)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("failed to configure rest client: %w: base_url=invalid", ErrInvalidParameter)
		}
		c.baseURL = strings.TrimRight(value, "/")
		return nil
	}
}

// WithHTTPClient injects an HTTP client.
//
// Version:
//   - 2026-08-19: Added.
func WithHTTPClient(value HTTPClient) ClientOption {
	return func(c *clientConfig) error {
		if value == nil {
			return fmt.Errorf("failed to configure rest client: %w: http_client=null", ErrInvalidParameter)
		}
		c.httpClient = value
		return nil
	}
}

// WithMaxResponseBodyBytes sets the successful response body limit.
//
// Version:
//   - 2026-08-19: Added.
func WithMaxResponseBodyBytes(value int64) ClientOption {
	return func(c *clientConfig) error {
		if value <= 0 {
			return fmt.Errorf("failed to configure rest client: %w: max_response_body_bytes=out_of_range", ErrInvalidParameter)
		}
		c.maxBodyBytes = value
		return nil
	}
}

// NewClient creates a REST client and composes all public market-data API groups.
//
// Version:
//   - 2026-08-19: Added.
func NewClient(options ...ClientOption) (*Client, error) {
	cfg := clientConfig{baseURL: DefaultBaseURL, maxBodyBytes: defaultMaxBodyBytes, userAgent: "k4k3ru-backpack-go/1"}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("failed to create rest client: %w: option=null", ErrInvalidParameter)
		}
		if err := option(&cfg); err != nil {
			return nil, fmt.Errorf("failed to create rest client: %w", err)
		}
	}
	if cfg.httpClient == nil {
		cfg.httpClient = &http.Client{Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext}}
	}
	c := &Client{baseURL: cfg.baseURL, httpClient: cfg.httpClient, maxBodyBytes: cfg.maxBodyBytes, userAgent: cfg.userAgent}
	c.markets = &MarketsClient{executor: c}
	c.trades = &TradesClient{executor: c}
	c.tickers = &TickersClient{executor: c}
	c.futures = &FuturesClient{executor: c}
	c.system = &SystemClient{executor: c}
	return c, nil
}

// Markets returns the Markets API group.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Markets() *MarketsClient {
	if c == nil {
		return nil
	}
	return c.markets
}

// Trades returns the Trades API group.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Trades() *TradesClient {
	if c == nil {
		return nil
	}
	return c.trades
}

// Tickers returns the Ticker and Kline API group.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Tickers() *TickersClient {
	if c == nil {
		return nil
	}
	return c.tickers
}

// Futures returns the futures market-data API group.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Futures() *FuturesClient {
	if c == nil {
		return nil
	}
	return c.futures
}

// System returns the public System API group.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) System() *SystemClient {
	if c == nil {
		return nil
	}
	return c.system
}

// ResponseError represents a non-2xx Backpack response.
type ResponseError struct {
	Operation          string
	StatusCode         int
	Code               string
	Message            string
	RetryAfter         string
	RateLimitLimit     string
	RateLimitRemaining string
	Body               string
}

// Error returns a bounded, non-sensitive HTTP error description.
//
// Version:
//   - 2026-08-19: Added.
func (e *ResponseError) Error() string {
	if e == nil {
		return "failed to execute backpack request: response_error=null"
	}
	return fmt.Sprintf("failed to execute backpack request: unexpected HTTP status: status_code=%d operation=%q code=%q message=%q", e.StatusCode, e.Operation, e.Code, e.Message)
}

// Do executes one REST request and optionally decodes JSON.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Do(ctx context.Context, r transport.Request, result any) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("failed to execute backpack request: %w: client=null", ErrInvalidParameter)
	}
	if ctx == nil {
		return nil, fmt.Errorf("failed to execute backpack request: %w: context=null", ErrInvalidParameter)
	}
	u := c.baseURL + "/" + strings.TrimLeft(r.Path, "/")
	if len(r.Query) > 0 {
		u += "?" + r.Query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, u, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute backpack request: %w", err)
	}
	req.Header = r.Header.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	req.Header.Set("User-Agent", c.userAgent)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute backpack request: %w: operation=%q", err, r.Operation)
	}
	if resp == nil {
		return nil, fmt.Errorf("failed to execute backpack request: response=null: operation=%q", r.Operation)
	}
	if resp.Body == nil {
		return nil, fmt.Errorf("failed to execute backpack request: response_body=null: operation=%q", r.Operation)
	}
	defer resp.Body.Close()
	limit := c.maxBodyBytes
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		limit = defaultErrorBytes
	}
	body, err := readBounded(resp.Body, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to execute backpack request: %w: operation=%q", err, r.Operation)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		re := &ResponseError{Operation: r.Operation, StatusCode: resp.StatusCode, RetryAfter: resp.Header.Get("Retry-After"), RateLimitLimit: resp.Header.Get("X-RateLimit-Limit"), RateLimitRemaining: resp.Header.Get("X-RateLimit-Remaining")}
		var p struct {
			Code    any    `json:"code"`
			Message string `json:"message"`
			Error   string `json:"error"`
		}
		if json.Unmarshal(body, &p) == nil {
			re.Code = fmt.Sprint(p.Code)
			if p.Code == nil {
				re.Code = ""
			}
			re.Message = p.Message
			if re.Message == "" {
				re.Message = p.Error
			}
		} else {
			re.Body = safeBody(body)
		}
		return nil, re
	}
	if result != nil && !r.Raw && len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, result); err != nil {
			return nil, fmt.Errorf("failed to decode backpack response: %w: operation=%q", err, r.Operation)
		}
	}
	return body, nil
}

func readBounded(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("failed to read response body: body=too_long max_length=%d", max)
	}
	return b, nil
}
func safeBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 512 {
		s = s[:512]
	}
	return strconv.QuoteToASCII(s)
}
func get(ctx context.Context, e transport.Executor, operation, path string, q url.Values, out any) error {
	_, err := e.Do(ctx, transport.Request{Operation: operation, Method: http.MethodGet, Path: path, Query: q}, out)
	return err
}
func requiredSymbol(op, s string) error {
	if s == "" {
		return fmt.Errorf("failed to validate backpack %s parameters: %w: symbol=empty", op, ErrInvalidParameter)
	}
	if len(s) > 128 {
		return fmt.Errorf("failed to validate backpack %s parameters: %w: symbol=too_long actual_length=%d max_length=128", op, ErrInvalidParameter, len(s))
	}
	return nil
}
