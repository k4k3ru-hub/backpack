package rest

import (
	"context"
	"fmt"
	"github.com/k4k3ru-hub/backpack/go/internal/transport"
	"net/url"
	"strconv"
	"time"
)

type MarkPricesParams struct {
	Symbol     string
	MarketType MarketType
}
type MarkPrice struct {
	Symbol               string                `json:"symbol"`
	MarkPrice            *string               `json:"markPrice,omitempty"`
	IndexPrice           *string               `json:"indexPrice,omitempty"`
	FundingRate          *string               `json:"fundingRate,omitempty"`
	NextFundingTimestamp *MillisecondTimestamp `json:"nextFundingTimestamp,omitempty"`
}
type MillisecondTimestamp int64

// Time converts a non-negative Unix millisecond timestamp to time.Time.
//
// Version:
//   - 2026-08-19: Added.
func (t MillisecondTimestamp) Time() (time.Time, error) {
	if t < 0 {
		return time.Time{}, fmt.Errorf("failed to convert millisecond timestamp: %w: timestamp=out_of_range", ErrInvalidParameter)
	}
	return time.UnixMilli(int64(t)), nil
}

type OpenInterestParams struct{ Symbol string }
type OpenInterest struct {
	Symbol       string               `json:"symbol"`
	OpenInterest string               `json:"openInterest"`
	Timestamp    MicrosecondTimestamp `json:"timestamp"`
}
type FundingRatesParams struct {
	Symbol string
	Limit  uint64
	Offset uint64
}
type FundingRate struct {
	Symbol               string `json:"symbol"`
	IntervalEndTimestamp string `json:"intervalEndTimestamp"`
	FundingRate          string `json:"fundingRate"`
}
type FuturesClient struct{ executor transport.Executor }

// GetMarkPrices gets mark, index, and current funding data.
//
// Version:
//   - 2026-08-19: Added.
func (c *FuturesClient) GetMarkPrices(ctx context.Context, p MarkPricesParams) ([]MarkPrice, error) {
	if p.Symbol != "" && len(p.Symbol) > 128 {
		return nil, fmt.Errorf("failed to validate backpack mark prices parameters: %w: symbol=too_long actual_length=%d max_length=128", ErrInvalidParameter, len(p.Symbol))
	}
	if p.MarketType != "" && !validMarketType(p.MarketType) {
		return nil, fmt.Errorf("failed to validate backpack mark prices parameters: %w: market_type=invalid", ErrInvalidParameter)
	}
	q := url.Values{}
	if p.Symbol != "" {
		q.Set("symbol", p.Symbol)
	}
	if p.MarketType != "" {
		q.Set("marketType", string(p.MarketType))
	}
	var out []MarkPrice
	if err := get(ctx, c.executor, "get mark prices", "/api/v1/markPrices", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack mark prices: %w", err)
	}
	return out, nil
}

// GetOpenInterest gets current open interest, optionally for one symbol.
//
// Version:
//   - 2026-08-19: Added.
func (c *FuturesClient) GetOpenInterest(ctx context.Context, p OpenInterestParams) ([]OpenInterest, error) {
	if p.Symbol != "" && len(p.Symbol) > 128 {
		return nil, fmt.Errorf("failed to validate backpack open interest parameters: %w: symbol=too_long actual_length=%d max_length=128", ErrInvalidParameter, len(p.Symbol))
	}
	q := url.Values{}
	if p.Symbol != "" {
		q.Set("symbol", p.Symbol)
	}
	var out []OpenInterest
	if err := get(ctx, c.executor, "get open interest", "/api/v1/openInterest", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack open interest: %w", err)
	}
	return out, nil
}

// GetFundingRates gets paginated funding-rate history.
//
// Version:
//   - 2026-08-19: Added.
func (c *FuturesClient) GetFundingRates(ctx context.Context, p FundingRatesParams) ([]FundingRate, error) {
	if err := requiredSymbol("funding rates", p.Symbol); err != nil {
		return nil, err
	}
	if p.Limit > maxFundingLimit {
		return nil, fmt.Errorf("failed to validate backpack funding rates parameters: %w: limit=out_of_range max_value=%d", ErrInvalidParameter, maxFundingLimit)
	}
	q := url.Values{"symbol": {p.Symbol}}
	if p.Limit > 0 {
		q.Set("limit", strconv.FormatUint(p.Limit, 10))
	}
	if p.Offset > 0 {
		q.Set("offset", strconv.FormatUint(p.Offset, 10))
	}
	var out []FundingRate
	if err := get(ctx, c.executor, "get funding rates", "/api/v1/fundingRates", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack funding rates: %w: symbol=%q", err, p.Symbol)
	}
	return out, nil
}
