package websocket

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type MicrosecondTimestamp int64

// Time converts a non-negative Unix microsecond timestamp to time.Time.
//
// Version:
//   - 2026-08-19: Added.
func (t MicrosecondTimestamp) Time() (time.Time, error) {
	if t < 0 {
		return time.Time{}, fmt.Errorf("failed to convert websocket microsecond timestamp: timestamp=out_of_range")
	}
	return time.UnixMicro(int64(t)), nil
}

type MillisecondTimestamp int64

// Time converts a non-negative Unix millisecond timestamp to time.Time.
//
// Version:
//   - 2026-08-19: Added.
func (t MillisecondTimestamp) Time() (time.Time, error) {
	if t < 0 {
		return time.Time{}, fmt.Errorf("failed to convert websocket millisecond timestamp: timestamp=out_of_range")
	}
	return time.UnixMilli(int64(t)), nil
}

type Side string

const (
	SideBid Side = "Bid"
	SideAsk Side = "Ask"
)

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
		return fmt.Errorf("failed to decode websocket depth level: %w", err)
	}
	if len(v) != 2 {
		return fmt.Errorf("failed to decode websocket depth level: invalid array length: actual_length=%d expected_length=2", len(v))
	}
	l.Price = v[0]
	l.Quantity = v[1]
	return nil
}

type Event interface{ StreamName() string }
type eventBase struct{ stream string }

func (b eventBase) StreamName() string { return b.stream }

type BookTickerEvent struct {
	eventBase
	EventType       string               `json:"e"`
	EventTimestamp  MicrosecondTimestamp `json:"E"`
	Symbol          string               `json:"s"`
	BestAskPrice    *string              `json:"a"`
	BestAskQuantity *string              `json:"A"`
	BestBidPrice    *string              `json:"b"`
	BestBidQuantity *string              `json:"B"`
	UpdateID        string               `json:"u"`
	EngineTimestamp MicrosecondTimestamp `json:"T"`
}
type DepthEvent struct {
	eventBase
	EventType       string               `json:"e"`
	EventTimestamp  MicrosecondTimestamp `json:"E"`
	Symbol          string               `json:"s"`
	Asks            []Level              `json:"a"`
	Bids            []Level              `json:"b"`
	FirstUpdateID   uint64               `json:"U"`
	LastUpdateID    uint64               `json:"u"`
	EngineTimestamp MicrosecondTimestamp `json:"T"`
}
type TradeEvent struct {
	eventBase
	EventType       string               `json:"e"`
	EventTimestamp  MicrosecondTimestamp `json:"E"`
	Symbol          string               `json:"s"`
	Price           string               `json:"p"`
	Quantity        string               `json:"q"`
	BuyerOrderID    string               `json:"b"`
	SellerOrderID   string               `json:"a"`
	TradeID         uint64               `json:"t"`
	EngineTimestamp MicrosecondTimestamp `json:"T"`
	IsBuyerMaker    bool                 `json:"m"`
}
type TickerEvent struct {
	eventBase
	EventType      string               `json:"e"`
	EventTimestamp MicrosecondTimestamp `json:"E"`
	Symbol         string               `json:"s"`
	FirstPrice     string               `json:"o"`
	LastPrice      string               `json:"c"`
	High           string               `json:"h"`
	Low            string               `json:"l"`
	Volume         string               `json:"v"`
	QuoteVolume    string               `json:"V"`
	Trades         uint64               `json:"n"`
}
type ExternalTickerEvent struct {
	eventBase
	EventType      string               `json:"e"`
	EventTimestamp MicrosecondTimestamp `json:"E"`
	Symbol         string               `json:"s"`
	FirstPrice     string               `json:"o"`
	LastPrice      string               `json:"c"`
	High           string               `json:"h"`
	Low            string               `json:"l"`
	Volume         string               `json:"v"`
	QuoteVolume    string               `json:"V"`
	Trades         uint64               `json:"n"`
}
type KlineEvent struct {
	eventBase
	EventType      string               `json:"e"`
	EventTimestamp MicrosecondTimestamp `json:"E"`
	Symbol         string               `json:"s"`
	Start          string               `json:"t"`
	CloseTime      string               `json:"T"`
	Open           string               `json:"o"`
	Close          string               `json:"c"`
	High           string               `json:"h"`
	Low            string               `json:"l"`
	Volume         string               `json:"v"`
	Trades         uint64               `json:"n"`
	Final          bool                 `json:"X"`
}

// StartTime parses the ISO 8601 candle start time.
//
// Version:
//   - 2026-08-19: Added.
func (e *KlineEvent) StartTime() (time.Time, error) { return parseISOTime("start", e.Start) }

// EndTime parses the ISO 8601 candle close time.
//
// Version:
//   - 2026-08-19: Added.
func (e *KlineEvent) EndTime() (time.Time, error) { return parseISOTime("close", e.CloseTime) }

type MarkPriceEvent struct {
	eventBase
	EventType            string                `json:"e"`
	EventTimestamp       MicrosecondTimestamp  `json:"E"`
	Symbol               string                `json:"s"`
	MarkPrice            string                `json:"p"`
	EstimatedFundingRate *string               `json:"f"`
	IndexPrice           *string               `json:"i"`
	NextFundingTimestamp *MillisecondTimestamp `json:"n"`
	EngineTimestamp      MicrosecondTimestamp  `json:"T"`
}
type OpenInterestEvent struct {
	eventBase
	EventType      string               `json:"e"`
	EventTimestamp MicrosecondTimestamp `json:"E"`
	Symbol         string               `json:"s"`
	OpenInterest   string               `json:"o"`
}
type LiquidationEvent struct {
	eventBase
	EventType       string               `json:"e"`
	EventTimestamp  MicrosecondTimestamp `json:"E"`
	EngineTimestamp MicrosecondTimestamp `json:"T"`
	Symbol          string               `json:"s"`
	Side            Side                 `json:"S"`
	Price           string               `json:"p"`
	Quantity        string               `json:"q"`
}
type UnknownEvent struct {
	eventBase
	EventType string
	Data      json.RawMessage
}

// DecodeEvent routes and decodes a public stream envelope.
//
// Version:
//   - 2026-08-19: Added.
func DecodeEvent(message []byte) (Event, error) {
	var env struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(message, &env); err != nil {
		return nil, fmt.Errorf("failed to decode websocket envelope: %w", err)
	}
	if env.Stream == "" {
		return nil, fmt.Errorf("failed to decode websocket envelope: stream=empty")
	}
	if len(env.Data) == 0 {
		return nil, fmt.Errorf("failed to decode websocket envelope: data=empty")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(env.Data, &fields); err != nil {
		return nil, fmt.Errorf("failed to decode websocket event header: %w", err)
	}
	var eventType string
	if raw := fields["e"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &eventType); err != nil {
			return nil, fmt.Errorf("failed to decode websocket event header: %w", err)
		}
	}
	var target Event
	prefix := strings.SplitN(env.Stream, ".", 2)[0]
	switch prefix {
	case "bookTicker":
		target = &BookTickerEvent{}
	case "depth":
		target = &DepthEvent{}
	case "trade":
		target = &TradeEvent{}
	case "ticker":
		target = &TickerEvent{}
	case "externalTicker":
		target = &ExternalTickerEvent{}
	case "kline":
		target = &KlineEvent{}
	case "markPrice":
		target = &MarkPriceEvent{}
	case "openInterest":
		target = &OpenInterestEvent{}
	case "liquidation":
		target = &LiquidationEvent{}
	default:
		return &UnknownEvent{eventBase: eventBase{stream: env.Stream}, EventType: eventType, Data: append(json.RawMessage(nil), env.Data...)}, nil
	}
	if err := json.Unmarshal(env.Data, target); err != nil {
		return nil, fmt.Errorf("failed to decode websocket event: %w: stream=%q", err, env.Stream)
	}
	setStream(target, env.Stream)
	return target, nil
}
func setStream(v Event, s string) {
	switch e := v.(type) {
	case *BookTickerEvent:
		e.stream = s
	case *DepthEvent:
		e.stream = s
	case *TradeEvent:
		e.stream = s
	case *TickerEvent:
		e.stream = s
	case *ExternalTickerEvent:
		e.stream = s
	case *KlineEvent:
		e.stream = s
	case *MarkPriceEvent:
		e.stream = s
	case *OpenInterestEvent:
		e.stream = s
	case *LiquidationEvent:
		e.stream = s
	}
}
func parseISOTime(field, value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05"} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse kline %s time: value=invalid", field)
}
