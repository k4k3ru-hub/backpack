package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/k4k3ru-hub/backpack/go/internal/transport"
)

type MarketType string

const (
	MarketTypeSpot             MarketType = "SPOT"
	MarketTypePerpetual        MarketType = "PERP"
	MarketTypeInversePerpetual MarketType = "IPERP"
	MarketTypeDated            MarketType = "DATED"
	MarketTypePrediction       MarketType = "PREDICTION"
	MarketTypeRFQ              MarketType = "RFQ"
)

type OrderBookState string

const (
	OrderBookStateOpen   OrderBookState = "Open"
	OrderBookStateClosed OrderBookState = "Closed"
)

type MarketsParams struct{ MarketTypes []MarketType }
type PriceFilter struct {
	MinPrice            string  `json:"minPrice"`
	MaxPrice            string  `json:"maxPrice"`
	TickSize            string  `json:"tickSize"`
	MaxMultiplier       *string `json:"maxMultiplier,omitempty"`
	MinMultiplier       *string `json:"minMultiplier,omitempty"`
	MaxImpactMultiplier *string `json:"maxImpactMultiplier,omitempty"`
	MinImpactMultiplier *string `json:"minImpactMultiplier,omitempty"`
}
type QuantityFilter struct {
	MinQuantity string `json:"minQuantity"`
	MaxQuantity string `json:"maxQuantity"`
	StepSize    string `json:"stepSize"`
}
type MarketFilters struct {
	Price    PriceFilter    `json:"price"`
	Quantity QuantityFilter `json:"quantity"`
}
type Market struct {
	Symbol                string         `json:"symbol"`
	BaseSymbol            string         `json:"baseSymbol"`
	QuoteSymbol           string         `json:"quoteSymbol"`
	MarketType            MarketType     `json:"marketType"`
	Filters               MarketFilters  `json:"filters"`
	FundingInterval       *int64         `json:"fundingInterval,omitempty"`
	FundingRateUpperBound *string        `json:"fundingRateUpperBound,omitempty"`
	FundingRateLowerBound *string        `json:"fundingRateLowerBound,omitempty"`
	OpenInterestLimit     *string        `json:"openInterestLimit,omitempty"`
	OrderBookState        OrderBookState `json:"orderBookState"`
	CreatedAt             string         `json:"createdAt"`
	Visible               bool           `json:"visible"`
	PositionLimitWeight   *string        `json:"positionLimitWeight,omitempty"`
	RWAMarketType         *string        `json:"rwaMarketType,omitempty"`
}

type DepthLimit string

const (
	DepthLimit5    DepthLimit = "5"
	DepthLimit10   DepthLimit = "10"
	DepthLimit20   DepthLimit = "20"
	DepthLimit50   DepthLimit = "50"
	DepthLimit100  DepthLimit = "100"
	DepthLimit500  DepthLimit = "500"
	DepthLimit1000 DepthLimit = "1000"
)

type DepthParams struct {
	Symbol string
	Limit  DepthLimit
}
type Level struct {
	Price    string
	Quantity string
}

// UnmarshalJSON validates and decodes a two-element price/quantity level.
//
// Version:
//   - 2026-08-19: Added.
func (l *Level) UnmarshalJSON(data []byte) error {
	var v []string
	if err := json.Unmarshal(data, &v); err != nil {
		return fmt.Errorf("failed to decode depth level: %w", err)
	}
	if len(v) != 2 {
		return fmt.Errorf("failed to decode depth level: invalid array length: actual_length=%d expected_length=2", len(v))
	}
	l.Price = v[0]
	l.Quantity = v[1]
	return nil
}

type Depth struct {
	Asks         []Level              `json:"asks"`
	Bids         []Level              `json:"bids"`
	LastUpdateID string               `json:"lastUpdateId"`
	Timestamp    MicrosecondTimestamp `json:"timestamp"`
}
type MicrosecondTimestamp int64

// Time converts a non-negative Unix microsecond timestamp to time.Time.
//
// Version:
//   - 2026-08-19: Added.
func (t MicrosecondTimestamp) Time() (time.Time, error) {
	if t < 0 {
		return time.Time{}, fmt.Errorf("failed to convert microsecond timestamp: %w: timestamp=out_of_range", ErrInvalidParameter)
	}
	return time.UnixMicro(int64(t)), nil
}

type MarketsClient struct{ executor transport.Executor }

// GetMarkets gets supported markets.
//
// Version:
//   - 2026-08-19: Added.
func (c *MarketsClient) GetMarkets(ctx context.Context, p MarketsParams) ([]Market, error) {
	q := url.Values{}
	for _, v := range p.MarketTypes {
		if !validMarketType(v) {
			return nil, fmt.Errorf("failed to validate backpack markets parameters: %w: market_type=invalid", ErrInvalidParameter)
		}
		q.Add("marketType", string(v))
	}
	var out []Market
	if err := get(ctx, c.executor, "get markets", "/api/v1/markets", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack markets: %w", err)
	}
	return out, nil
}

// GetDepth gets an order book snapshot.
//
// Version:
//   - 2026-08-19: Added.
func (c *MarketsClient) GetDepth(ctx context.Context, p DepthParams) (*Depth, error) {
	if err := requiredSymbol("depth", p.Symbol); err != nil {
		return nil, err
	}
	if p.Limit != "" && !validDepthLimit(p.Limit) {
		return nil, fmt.Errorf("failed to validate backpack depth parameters: %w: limit=invalid", ErrInvalidParameter)
	}
	q := url.Values{"symbol": {p.Symbol}}
	if p.Limit != "" {
		q.Set("limit", string(p.Limit))
	}
	var out Depth
	if err := get(ctx, c.executor, "get depth", "/api/v1/depth", q, &out); err != nil {
		return nil, fmt.Errorf("failed to get backpack market depth: %w: symbol=%q", err, p.Symbol)
	}
	return &out, nil
}
func validMarketType(v MarketType) bool {
	switch v {
	case MarketTypeSpot, MarketTypePerpetual, MarketTypeInversePerpetual, MarketTypeDated, MarketTypePrediction, MarketTypeRFQ:
		return true
	}
	return false
}
func validDepthLimit(v DepthLimit) bool {
	switch v {
	case DepthLimit5, DepthLimit10, DepthLimit20, DepthLimit50, DepthLimit100, DepthLimit500, DepthLimit1000:
		return true
	}
	return false
}
