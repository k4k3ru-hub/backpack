package rest

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/k4k3ru-hub/backpack/go/internal/transport"
)

type TickerInterval string

const (
	TickerInterval1Day  TickerInterval = "1d"
	TickerInterval1Week TickerInterval = "1w"
)

type KlineInterval string

const (
	KlineInterval1Second   KlineInterval = "1s"
	KlineInterval1Minute   KlineInterval = "1m"
	KlineInterval3Minutes  KlineInterval = "3m"
	KlineInterval5Minutes  KlineInterval = "5m"
	KlineInterval15Minutes KlineInterval = "15m"
	KlineInterval30Minutes KlineInterval = "30m"
	KlineInterval1Hour     KlineInterval = "1h"
	KlineInterval2Hours    KlineInterval = "2h"
	KlineInterval4Hours    KlineInterval = "4h"
	KlineInterval6Hours    KlineInterval = "6h"
	KlineInterval8Hours    KlineInterval = "8h"
	KlineInterval12Hours   KlineInterval = "12h"
	KlineInterval1Day      KlineInterval = "1d"
	KlineInterval3Days     KlineInterval = "3d"
	KlineInterval1Week     KlineInterval = "1w"
	KlineInterval1Month    KlineInterval = "1month"
)

type KlineSource string

const (
	KlineSourceVenue    KlineSource = "Venue"
	KlineSourceExternal KlineSource = "External"
)

type KlinePriceType string

const (
	KlinePriceTypeLast  KlinePriceType = "Last"
	KlinePriceTypeIndex KlinePriceType = "Index"
	KlinePriceTypeMark  KlinePriceType = "Mark"
)

type TickerParams struct {
	Symbol   string
	Interval TickerInterval
	Source   KlineSource
}
type TickersParams struct {
	Interval TickerInterval
	Source   KlineSource
}
type Ticker struct {
	Symbol             string `json:"symbol"`
	FirstPrice         string `json:"firstPrice"`
	LastPrice          string `json:"lastPrice"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	High               string `json:"high"`
	Low                string `json:"low"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	Trades             string `json:"trades"`
}
type KlinesParams struct {
	Symbol    string
	Interval  KlineInterval
	StartTime int64
	EndTime   *int64
	PriceType KlinePriceType
	Source    KlineSource
}
type Kline struct {
	Start       string `json:"start"`
	End         string `json:"end"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Close       string `json:"close"`
	Volume      string `json:"volume"`
	QuoteVolume string `json:"quoteVolume"`
	Trades      string `json:"trades"`
}
type TickersClient struct{ executor transport.Executor }

// GetTicker gets rolling statistics for one market.
//
// Version:
//   - 2026-08-19: Added.
func (c *TickersClient) GetTicker(ctx context.Context, p TickerParams) (*Ticker, error) {
	if err := requiredSymbol("ticker", p.Symbol); err != nil {
		return nil, err
	}
	q, err := tickerQuery(p.Interval, p.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to validate backpack ticker parameters: %w", err)
	}
	q.Set("symbol", p.Symbol)
	var out Ticker
	if err := get(ctx, c.executor, "get ticker", "/api/v1/ticker", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack ticker: %w: symbol=%q", err, p.Symbol)
	}
	return &out, nil
}

// GetTickers gets rolling statistics for all markets.
//
// Version:
//   - 2026-08-19: Added.
func (c *TickersClient) GetTickers(ctx context.Context, p TickersParams) ([]Ticker, error) {
	q, err := tickerQuery(p.Interval, p.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to validate backpack tickers parameters: %w", err)
	}
	var out []Ticker
	if err := get(ctx, c.executor, "get tickers", "/api/v1/tickers", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack tickers: %w", err)
	}
	return out, nil
}

// GetKlines gets historical candles.
//
// Version:
//   - 2026-08-19: Added.
func (c *TickersClient) GetKlines(ctx context.Context, p KlinesParams) ([]Kline, error) {
	if err := requiredSymbol("klines", p.Symbol); err != nil {
		return nil, err
	}
	if !validKlineInterval(p.Interval) {
		return nil, fmt.Errorf("failed to validate backpack klines parameters: %w: interval=invalid", ErrInvalidParameter)
	}
	if p.StartTime < 0 {
		return nil, fmt.Errorf("failed to validate backpack klines parameters: %w: start_time=out_of_range", ErrInvalidParameter)
	}
	if p.EndTime != nil && (*p.EndTime < 0 || *p.EndTime < p.StartTime) {
		return nil, fmt.Errorf("failed to validate backpack klines parameters: %w: time_range=invalid", ErrInvalidParameter)
	}
	if p.PriceType != "" && !validPriceType(p.PriceType) {
		return nil, fmt.Errorf("failed to validate backpack klines parameters: %w: price_type=invalid", ErrInvalidParameter)
	}
	if p.Source != "" && !validSource(p.Source) {
		return nil, fmt.Errorf("failed to validate backpack klines parameters: %w: source=invalid", ErrInvalidParameter)
	}
	if p.Source == KlineSourceExternal && p.PriceType != "" && p.PriceType != KlinePriceTypeLast {
		return nil, fmt.Errorf("failed to validate backpack klines parameters: %w: external_price_type=invalid", ErrInvalidParameter)
	}
	q := url.Values{"symbol": {p.Symbol}, "interval": {string(p.Interval)}, "startTime": {strconv.FormatInt(p.StartTime, 10)}}
	if p.EndTime != nil {
		q.Set("endTime", strconv.FormatInt(*p.EndTime, 10))
	}
	if p.PriceType != "" {
		q.Set("priceType", string(p.PriceType))
	}
	if p.Source != "" {
		q.Set("source", string(p.Source))
	}
	var out []Kline
	if err := get(ctx, c.executor, "get klines", "/api/v1/klines", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack klines: %w: symbol=%q", err, p.Symbol)
	}
	return out, nil
}
func tickerQuery(i TickerInterval, s KlineSource) (url.Values, error) {
	if i != "" && i != TickerInterval1Day && i != TickerInterval1Week {
		return nil, fmt.Errorf("%w: interval=invalid", ErrInvalidParameter)
	}
	if s != "" && !validSource(s) {
		return nil, fmt.Errorf("%w: source=invalid", ErrInvalidParameter)
	}
	if s == KlineSourceExternal && i != TickerInterval1Day {
		return nil, fmt.Errorf("%w: external_interval=invalid", ErrInvalidParameter)
	}
	q := url.Values{}
	if i != "" {
		q.Set("interval", string(i))
	}
	if s != "" {
		q.Set("source", string(s))
	}
	return q, nil
}
func validSource(v KlineSource) bool { return v == KlineSourceVenue || v == KlineSourceExternal }
func validPriceType(v KlinePriceType) bool {
	return v == KlinePriceTypeLast || v == KlinePriceTypeIndex || v == KlinePriceTypeMark
}
func validKlineInterval(v KlineInterval) bool {
	switch v {
	case KlineInterval1Second, KlineInterval1Minute, KlineInterval3Minutes, KlineInterval5Minutes, KlineInterval15Minutes, KlineInterval30Minutes, KlineInterval1Hour, KlineInterval2Hours, KlineInterval4Hours, KlineInterval6Hours, KlineInterval8Hours, KlineInterval12Hours, KlineInterval1Day, KlineInterval3Days, KlineInterval1Week, KlineInterval1Month:
		return true
	}
	return false
}
