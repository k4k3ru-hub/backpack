package websocket

import (
	"fmt"
	"github.com/k4k3ru-hub/backpack/go/rest"
	"strings"
)

// BookTickerStream returns a book-ticker stream name.
//
// Version:
//   - 2026-08-19: Added.
func BookTickerStream(symbol string) (string, error) { return symbolStream("bookTicker", symbol) }

// DepthStream returns a depth stream name.
//
// Version:
//   - 2026-08-19: Added.
func DepthStream(symbol string) (string, error) { return symbolStream("depth", symbol) }

// TradeStream returns a trade stream name.
//
// Version:
//   - 2026-08-19: Added.
func TradeStream(symbol string) (string, error) { return symbolStream("trade", symbol) }

// TickerStream returns a venue ticker stream name.
//
// Version:
//   - 2026-08-19: Added.
func TickerStream(symbol string) (string, error) { return symbolStream("ticker", symbol) }

// ExternalTickerStream returns a provider-derived ticker stream name.
//
// Version:
//   - 2026-08-19: Added.
func ExternalTickerStream(symbol string) (string, error) {
	return symbolStream("externalTicker", symbol)
}

// MarkPriceStream returns a mark-price stream name.
//
// Version:
//   - 2026-08-19: Added.
func MarkPriceStream(symbol string) (string, error) { return symbolStream("markPrice", symbol) }

// OpenInterestStream returns an open-interest stream name.
//
// Version:
//   - 2026-08-19: Added.
func OpenInterestStream(symbol string) (string, error) { return symbolStream("openInterest", symbol) }

// LiquidationStream returns a liquidation stream name.
//
// Version:
//   - 2026-08-19: Added.
func LiquidationStream(symbol string) (string, error) { return symbolStream("liquidation", symbol) }

// KlineStream returns a Kline stream name.
//
// Version:
//   - 2026-08-19: Added.
func KlineStream(interval rest.KlineInterval, symbol string) (string, error) {
	if !validWSInterval(interval) {
		return "", fmt.Errorf("failed to create websocket kline stream: interval=invalid")
	}
	s, err := validateSymbol(symbol)
	if err != nil {
		return "", fmt.Errorf("failed to create websocket kline stream: %w", err)
	}
	return "kline." + string(interval) + "." + s, nil
}
func symbolStream(prefix, symbol string) (string, error) {
	s, err := validateSymbol(symbol)
	if err != nil {
		return "", fmt.Errorf("failed to create websocket stream: %w", err)
	}
	return prefix + "." + s, nil
}
func validateSymbol(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("symbol=empty")
	}
	if len(s) > 128 {
		return "", fmt.Errorf("symbol=too_long actual_length=%d max_length=128", len(s))
	}
	if strings.ContainsAny(s, "\r\n\t ") {
		return "", fmt.Errorf("symbol=invalid")
	}
	return s, nil
}
func validWSInterval(v rest.KlineInterval) bool {
	switch v {
	case rest.KlineInterval1Second, rest.KlineInterval1Minute, rest.KlineInterval3Minutes, rest.KlineInterval5Minutes, rest.KlineInterval15Minutes, rest.KlineInterval30Minutes, rest.KlineInterval1Hour, rest.KlineInterval2Hours, rest.KlineInterval4Hours, rest.KlineInterval6Hours, rest.KlineInterval8Hours, rest.KlineInterval12Hours, rest.KlineInterval1Day, rest.KlineInterval3Days, rest.KlineInterval1Week, rest.KlineInterval1Month:
		return true
	}
	return false
}
