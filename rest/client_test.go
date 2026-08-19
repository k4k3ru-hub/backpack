package rest

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(t *testing.T, handler http.HandlerFunc, options ...ClientOption) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	options = append([]ClientOption{WithBaseURL(server.URL)}, options...)
	client, err := NewClient(options...)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

// TestNewClientComposesAPIGroups verifies composition-root assembly.
//
// Version:
//   - 2026-08-19: Added.
func TestNewClientComposesAPIGroups(t *testing.T) {
	client, err := NewClient(WithHTTPClient(http.DefaultClient))
	if err != nil {
		t.Fatal(err)
	}
	if client.Markets() == nil || client.Trades() == nil || client.Tickers() == nil || client.Futures() == nil || client.System() == nil {
		t.Fatal("NewClient() did not compose every API group")
	}
}

// TestRESTOperations verifies paths, queries, and representative decoding.
//
// Version:
//   - 2026-08-19: Added.
func TestRESTOperations(t *testing.T) {
	tests := []struct {
		name, path, query, body string
		call                    func(*Client) error
	}{
		{"markets", "/api/v1/markets", "marketType=SPOT&marketType=PERP", `[{"symbol":"SOL_USDC","marketType":"FUTURE_NEW","filters":{"price":{"minPrice":"0.0001","maxPrice":"999","tickSize":"0.0001"},"quantity":{"minQuantity":"0.01","maxQuantity":"1000","stepSize":"0.01"}},"visible":true}]`, func(c *Client) error {
			v, e := c.Markets().GetMarkets(context.Background(), MarketsParams{MarketTypes: []MarketType{MarketTypeSpot, MarketTypePerpetual}})
			if e == nil && (v[0].MarketType != "FUTURE_NEW" || v[0].Filters.Price.TickSize != "0.0001") {
				return errors.New("market decode")
			}
			return e
		}},
		{"depth", "/api/v1/depth", "limit=100&symbol=SOL_USDC", `{"asks":[["1.00000001","2"]],"bids":[["0.9","3"]],"lastUpdateId":"99","timestamp":1694687965941000}`, func(c *Client) error {
			v, e := c.Markets().GetDepth(context.Background(), DepthParams{Symbol: "SOL_USDC", Limit: DepthLimit100})
			if e == nil && (v.Asks[0].Price != "1.00000001" || v.LastUpdateID != "99") {
				return errors.New("depth decode")
			}
			return e
		}},
		{"recent trades", "/api/v1/trades", "limit=2&symbol=SOL_USDC", `[{"id":7,"price":"1.2","quantity":"3.4","quoteQuantity":"4.08","timestamp":1694687965941000,"isBuyerMaker":true}]`, func(c *Client) error {
			v, e := c.Trades().GetRecentTrades(context.Background(), TradesParams{Symbol: "SOL_USDC", Limit: 2})
			if e == nil && (v[0].Price != "1.2" || !v[0].IsBuyerMaker) {
				return errors.New("trade decode")
			}
			return e
		}},
		{"historical trades", "/api/v1/trades/history", "limit=1000&offset=9000&symbol=SOL_USDC", `[]`, func(c *Client) error {
			_, e := c.Trades().GetHistoricalTrades(context.Background(), HistoricalTradesParams{Symbol: "SOL_USDC", Limit: 1000, Offset: 9000})
			return e
		}},
		{"ticker", "/api/v1/ticker", "interval=1d&source=External&symbol=AAPL.US_USDC", `{"symbol":"AAPL.US_USDC","firstPrice":"1","lastPrice":"2","trades":"9"}`, func(c *Client) error {
			v, e := c.Tickers().GetTicker(context.Background(), TickerParams{Symbol: "AAPL.US_USDC", Interval: TickerInterval1Day, Source: KlineSourceExternal})
			if e == nil && v.LastPrice != "2" {
				return errors.New("ticker decode")
			}
			return e
		}},
		{"tickers", "/api/v1/tickers", "interval=1w", `[]`, func(c *Client) error {
			_, e := c.Tickers().GetTickers(context.Background(), TickersParams{Interval: TickerInterval1Week})
			return e
		}},
		{"klines", "/api/v1/klines", "endTime=200&interval=1m&priceType=Mark&startTime=100&symbol=SOL_USDC_PERP", `[{"start":"2026-08-19T00:00:00Z","end":"2026-08-19T00:01:00Z","open":"1","high":"2","low":"0.5","close":"1.5","volume":"10","quoteVolume":"15","trades":"3"}]`, func(c *Client) error {
			end := int64(200)
			v, e := c.Tickers().GetKlines(context.Background(), KlinesParams{Symbol: "SOL_USDC_PERP", Interval: KlineInterval1Minute, StartTime: 100, EndTime: &end, PriceType: KlinePriceTypeMark})
			if e == nil && v[0].QuoteVolume != "15" {
				return errors.New("kline decode")
			}
			return e
		}},
		{"mark prices", "/api/v1/markPrices", "marketType=PERP&symbol=SOL_USDC_PERP", `[{"symbol":"SOL_USDC_PERP","markPrice":"2","indexPrice":null}]`, func(c *Client) error {
			v, e := c.Futures().GetMarkPrices(context.Background(), MarkPricesParams{Symbol: "SOL_USDC_PERP", MarketType: MarketTypePerpetual})
			if e == nil && (v[0].MarkPrice == nil || v[0].IndexPrice != nil) {
				return errors.New("mark decode")
			}
			return e
		}},
		{"open interest", "/api/v1/openInterest", "", `[{"symbol":"SOL_USDC_PERP","openInterest":"123","timestamp":1}]`, func(c *Client) error {
			v, e := c.Futures().GetOpenInterest(context.Background(), OpenInterestParams{})
			if e == nil && v[0].OpenInterest != "123" {
				return errors.New("oi decode")
			}
			return e
		}},
		{"funding rates", "/api/v1/fundingRates", "limit=10000&offset=2&symbol=SOL_USDC_PERP", `[{"symbol":"SOL_USDC_PERP","intervalEndTimestamp":"2026-08-19T00:00:00Z","fundingRate":"0.0001"}]`, func(c *Client) error {
			v, e := c.Futures().GetFundingRates(context.Background(), FundingRatesParams{Symbol: "SOL_USDC_PERP", Limit: 10000, Offset: 2})
			if e == nil && v[0].FundingRate != "0.0001" {
				return errors.New("funding decode")
			}
			return e
		}},
		{"status", "/api/v1/status", "", `{"status":"Maintenance","message":"planned"}`, func(c *Client) error {
			v, e := c.System().GetStatus(context.Background())
			if e == nil && v.Status != "Maintenance" {
				return errors.New("status decode")
			}
			return e
		}},
		{"ping", "/api/v1/ping", "", "pong", func(c *Client) error {
			v, e := c.System().Ping(context.Background())
			if e == nil && v != "pong" {
				return errors.New("ping decode")
			}
			return e
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != tt.path || r.URL.RawQuery != tt.query {
					t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				}
				_, _ = io.WriteString(w, tt.body)
			})
			if err := tt.call(client); err != nil {
				t.Fatalf("operation error = %v", err)
			}
		})
	}
}

// TestValidationDoesNotSendRequest verifies local validation and pagination overflow safety.
//
// Version:
//   - 2026-08-19: Added.
func TestValidationDoesNotSendRequest(t *testing.T) {
	var calls atomic.Int64
	client := newTestClient(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	cases := []func() error{func() error { _, e := client.Markets().GetDepth(context.Background(), DepthParams{}); return e }, func() error {
		_, e := client.Trades().GetRecentTrades(context.Background(), TradesParams{Symbol: "X", Limit: 1001})
		return e
	}, func() error {
		_, e := client.Trades().GetHistoricalTrades(context.Background(), HistoricalTradesParams{Symbol: "X", Limit: 1000, Offset: math.MaxUint64})
		return e
	}, func() error {
		_, e := client.Tickers().GetKlines(context.Background(), KlinesParams{Symbol: "X", Interval: "bad"})
		return e
	}, func() error {
		_, e := client.Futures().GetFundingRates(context.Background(), FundingRatesParams{Symbol: "X", Limit: 10001})
		return e
	}}
	for _, call := range cases {
		if err := call(); !errors.Is(err, ErrInvalidParameter) {
			t.Errorf("error = %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("request calls = %d", calls.Load())
	}
}

// TestRESTFailures verifies malformed JSON, bounded bodies, cancellation, and response errors.
//
// Version:
//   - 2026-08-19: Added.
func TestRESTFailures(t *testing.T) {
	t.Run("response error", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"code":"RATE_LIMIT","message":"slow"}`)
		})
		_, err := client.System().GetStatus(context.Background())
		var re *ResponseError
		if !errors.As(err, &re) || re.Code != "RATE_LIMIT" || re.RetryAfter != "3" {
			t.Fatalf("error = %#v", err)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{") })
		if _, err := client.System().GetStatus(context.Background()); err == nil {
			t.Fatal("error = nil")
		}
	})
	t.Run("too large", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "12345") }, WithMaxResponseBodyBytes(4))
		if _, err := client.System().Ping(context.Background()); err == nil {
			t.Fatal("error = nil")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := client.System().Ping(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	})
}

type trackingBody struct{ closed atomic.Bool }

func (b *trackingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *trackingBody) Close() error             { b.closed.Store(true); return nil }

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// TestResponseBodyClosed verifies response bodies are always closed.
//
// Version:
//   - 2026-08-19: Added.
func TestResponseBodyClosed(t *testing.T) {
	body := &trackingBody{}
	client, err := NewClient(WithHTTPClient(httpClientFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = client.System().Ping(context.Background())
	if !body.closed.Load() {
		t.Fatal("response body was not closed")
	}
}

// TestTimestampConversions verifies REST timestamp units.
//
// Version:
//   - 2026-08-19: Added.
func TestTimestampConversions(t *testing.T) {
	micro, _ := MicrosecondTimestamp(1_500_000).Time()
	milli, _ := MillisecondTimestamp(1_500).Time()
	want := time.Unix(1, 500_000_000)
	if !micro.Equal(want) || !milli.Equal(want) {
		t.Fatalf("micro=%v milli=%v", micro, milli)
	}
	if _, err := MicrosecondTimestamp(-1).Time(); err == nil {
		t.Fatal("negative microsecond accepted")
	}
}

var _ = strings.Builder{}
