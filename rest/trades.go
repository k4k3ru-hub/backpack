package rest

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/k4k3ru-hub/backpack/go/internal/transport"
)

const (
	maxTradeLimit       uint64 = 1000
	maxHistoricalWindow uint64 = 10000
	maxFundingLimit     uint64 = 10000
)

type TradesParams struct {
	Symbol string
	Limit  uint64
}
type HistoricalTradesParams struct {
	Symbol string
	Limit  uint64
	Offset uint64
}
type Trade struct {
	ID            uint64               `json:"id"`
	Price         string               `json:"price"`
	Quantity      string               `json:"quantity"`
	QuoteQuantity string               `json:"quoteQuantity"`
	Timestamp     MicrosecondTimestamp `json:"timestamp"`
	IsBuyerMaker  bool                 `json:"isBuyerMaker"`
}
type TradesClient struct{ executor transport.Executor }

// GetRecentTrades gets recent public trades.
//
// Version:
//   - 2026-08-19: Added.
func (c *TradesClient) GetRecentTrades(ctx context.Context, p TradesParams) ([]Trade, error) {
	if err := requiredSymbol("recent trades", p.Symbol); err != nil {
		return nil, err
	}
	if p.Limit > maxTradeLimit {
		return nil, fmt.Errorf("failed to validate backpack recent trades parameters: %w: limit=out_of_range max_value=%d", ErrInvalidParameter, maxTradeLimit)
	}
	q := url.Values{"symbol": {p.Symbol}}
	if p.Limit > 0 {
		q.Set("limit", strconv.FormatUint(p.Limit, 10))
	}
	var out []Trade
	if err := get(ctx, c.executor, "get recent trades", "/api/v1/trades", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack recent trades: %w: symbol=%q", err, p.Symbol)
	}
	return out, nil
}

// GetHistoricalTrades gets paginated historical public trades.
//
// Version:
//   - 2026-08-19: Added.
func (c *TradesClient) GetHistoricalTrades(ctx context.Context, p HistoricalTradesParams) ([]Trade, error) {
	if err := requiredSymbol("historical trades", p.Symbol); err != nil {
		return nil, err
	}
	limit := p.Limit
	if limit == 0 {
		limit = 100
	}
	if limit > maxTradeLimit {
		return nil, fmt.Errorf("failed to validate backpack historical trades parameters: %w: limit=out_of_range max_value=%d", ErrInvalidParameter, maxTradeLimit)
	}
	if p.Offset > maxHistoricalWindow-limit {
		return nil, fmt.Errorf("failed to validate backpack historical trades parameters: %w: pagination_window=out_of_range max_value=%d", ErrInvalidParameter, maxHistoricalWindow)
	}
	q := url.Values{"symbol": {p.Symbol}}
	if p.Limit > 0 {
		q.Set("limit", strconv.FormatUint(p.Limit, 10))
	}
	if p.Offset > 0 {
		q.Set("offset", strconv.FormatUint(p.Offset, 10))
	}
	var out []Trade
	if err := get(ctx, c.executor, "get historical trades", "/api/v1/trades/history", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack historical trades: %w: symbol=%q", err, p.Symbol)
	}
	return out, nil
}
