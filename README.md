# Backpack Exchange Go

`github.com/k4k3ru-hub/backpack/go` is an independent Go client for Backpack Exchange public market data. It covers public REST market data and public WebSocket streams only. It does not implement API keys, signing, accounts, balances, positions, orders, RFQs, deposits, withdrawals, private streams, persistence, normalization, or K4K3RU integration.

The implementation was checked against the official Backpack API documentation and OpenAPI presentation on 2026-08-19. The defaults are:

- REST: `https://api.backpack.exchange`
- WebSocket: `wss://ws.backpack.exchange`

The implemented public endpoints do not declare authentication parameters in the current official specification. If Backpack later requires authentication for one of them, that endpoint is outside this module's present scope until the public API is deliberately revised.

## Supported markets

`GET /api/v1/markets` is authoritative. `MarketType` preserves unknown response values and provides constants for `SPOT`, `PERP`, `IPERP`, `DATED`, `PREDICTION`, and `RFQ`. The primary supported use is SPOT, PERP, IPERP, and DATED. Prediction and RFQ market records decode without enabling prediction-specific or RFQ workflows.

Symbols are sent unchanged. The module never adds or removes `_PERP` and never converts `USDC`, `USD`, or `USDT`.

## API correspondence

| Concept | REST | WebSocket | Notes |
|---|---|---|---|
| Market metadata | `/api/v1/markets` | — | Spot and futures share the endpoint |
| Book snapshot | `/api/v1/depth` | — | Preserves `lastUpdateId` |
| Book updates | — | `depth.<symbol>` | Absolute quantities and `U`/`u` are preserved |
| Best bid/ask | — | `bookTicker.<symbol>` | Empty sides remain `nil` |
| Recent trades | `/api/v1/trades` | `trade.<symbol>` | Trade IDs are only assumed sequential per symbol |
| Historical trades | `/api/v1/trades/history` | — | Maximum window is 10,000 |
| Venue ticker | `/api/v1/ticker(s)` | `ticker.<symbol>` | Venue activity |
| External ticker | `/api/v1/ticker(s)?source=External` | `externalTicker.<symbol>` | Separate provider-derived event type |
| Kline | `/api/v1/klines` | `kline.<interval>.<symbol>` | REST range uses Unix seconds; WS candle times are ISO 8601 |
| Mark/index/current funding | `/api/v1/markPrices` | `markPrice.<symbol>` | Futures-oriented optional values |
| Funding history | `/api/v1/fundingRates` | `markPrice.<symbol>` | History and estimate have different meanings |
| Open interest | `/api/v1/openInterest` | `openInterest.<symbol>` | WS is documented as approximately every 60 seconds |
| Liquidations | — | `liquidation.<symbol>` | Backpack side is preserved unchanged |
| System state | `/api/v1/status` | — | Returned state is not converted to a boolean health claim |
| Connectivity | `/api/v1/ping` | — | Plain-text `pong` response |

The analogous Binance exchange-info/depth/trade/ticker/kline and Hyperliquid meta/L2-book/trade concepts informed API grouping, but Backpack-specific fields and semantics are not synthesized from those APIs. Backpack has no implemented equivalent here for unrelated existing-module features. Root-package aliases are intentionally not provided.

## REST usage

```go
client, err := rest.NewClient()
if err != nil {
    return err
}

markets, err := client.Markets().GetMarkets(ctx, rest.MarketsParams{
    MarketTypes: []rest.MarketType{rest.MarketTypeSpot, rest.MarketTypePerpetual},
})
depth, err := client.Markets().GetDepth(ctx, rest.DepthParams{
    Symbol: "SOL_USDC",
    Limit:  rest.DepthLimit100,
})
trades, err := client.Trades().GetRecentTrades(ctx, rest.TradesParams{
    Symbol: "SOL_USDC",
    Limit:  100,
})
ticker, err := client.Tickers().GetTicker(ctx, rest.TickerParams{
    Symbol:   "SOL_USDC",
    Interval: rest.TickerInterval1Day,
})
klines, err := client.Tickers().GetKlines(ctx, rest.KlinesParams{
    Symbol:    "SOL_USDC",
    Interval:  rest.KlineInterval1Minute,
    StartTime: time.Now().Add(-time.Hour).Unix(),
})
```

Inject a custom client or base URL with `rest.WithHTTPClient` and `rest.WithBaseURL`. Requests are immutable per call. Responses are bounded, bodies are always closed, and non-2xx responses are returned as `*rest.ResponseError`, including bounded Backpack error data, status, `Retry-After`, and common rate-limit headers.

Price, quantity, volume, rates, and open interest remain decimal strings. The SDK never converts them to `float64`.

Depth limits currently accepted by the official specification are 5, 10, 20, 50, 100, 500, and 1000. Omitting `limit` lets the server return up to 5000 levels. Recent and historical trade limits are at most 1000; historical `offset + limit` is at most 10000. Funding history currently permits a limit up to 10000.

Ticker intervals are `1d` and `1w`. `source=External` is sent only when explicitly requested and requires `1d`. Kline intervals are `1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, and `1month`. Klines require `startTime` in Unix seconds and optionally accept `endTime`, `priceType` (`Last`, `Index`, `Mark`), and source. External Klines require Last prices.

## WebSocket usage

```go
client, err := websocket.NewClient(websocket.WithEventBuffer(256))
if err != nil {
    return err
}
if err := client.Connect(ctx); err != nil {
    return err
}
defer client.Close()

depthStream, _ := websocket.DepthStream("SOL_USDC")
tradeStream, _ := websocket.TradeStream("SOL_USDC")
if err := client.Subscribe(ctx, depthStream, tradeStream); err != nil {
    return err
}

for {
    select {
    case event := <-client.Events():
        _ = event // Type-switch on the concrete public event type.
    case err := <-client.Errors():
        return err
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

`Subscribe` and `Unsubscribe` accept multiple streams and serialize writes. `Close` is idempotent. Cancellation closes the connection. Unknown streams produce `UnknownEvent`; malformed payloads are reported on `Errors()` without panicking.

The event channel is bounded. If it fills, the client reports `websocket.ErrSlowConsumer` and closes the connection instead of silently dropping market data. Callers therefore control throughput by selecting an appropriate buffer and draining events promptly.

Automatic reconnect is intentionally not implemented. A disconnect is reported through `Errors()`, and the caller must create/connect a new client and restore subscriptions. This avoids implying depth continuity across a disconnected session.

### Depth snapshot and updates

Start the `depth.<symbol>` stream and obtain a REST `/api/v1/depth` snapshot when constructing a local book. Stream quantities are absolute quantities at each price; quantity zero removes a level. Backpack documents snapshot use and the `U`/`u` fields but does not currently publish a precise snapshot-to-stream continuity condition. This module therefore exposes raw snapshots and updates and does not provide a synchronized local order book. It does not borrow Binance continuity rules by assumption.

## Timestamp units

- REST depth, trades, and open-interest timestamps are represented as named microsecond values according to the current schemas/examples.
- Ordinary WebSocket `E` event and `T` engine timestamps are Unix microseconds.
- Mark-price `n` next-funding timestamp is Unix milliseconds.
- WebSocket Kline `t` and `T` are ISO 8601 strings, not integer engine timestamps.
- REST Kline request `startTime` and `endTime` are Unix seconds; returned candle `start`/`end` remain wire strings.

Named timestamp types expose checked `Time()` helpers and reject negative values. Raw wire values remain available.

## Rate limits, terms, and operational use

The official support documentation currently states a default of 2000 standard REST requests per minute per subaccount and 30 requests per minute for historical endpoints, with HTTP 429 on excess. Public unauthenticated accounting and WebSocket connection/subscription limits are not clearly specified. This client performs no automatic retry; callers should honor `Retry-After`, rate-limit headers, context cancellation, and their own bounded backoff policy.

Technical public access is not permission for unlimited storage, redistribution, derived-data distribution, or commercial use. The reviewed official materials did not provide a sufficiently specific market-data license for those activities. Users must check the latest Backpack User Agreement, regional eligibility rules, market-data terms, provider restrictions (especially external tokenized-stock data), and applicable laws before use. Availability varies by region and jurisdiction.

Backpack may change endpoints, enums, fields, limits, timestamp formats, and terms. Recheck the official documentation and OpenAPI specification when upgrading or using this module operationally.

## Testing

The normal suite uses injected transports, `httptest.Server`, and fake WebSocket connections/dialers. It never contacts Backpack production:

```bash
go test ./...
go vet ./...
```

Do not add production connectivity to the ordinary or CI test suite. Live diagnostics, if introduced separately, must be explicit and opt-in.

## CLI

The CLI uses `github.com/k4k3ru-hub/cli/go` and currently exposes one public REST operation:

```bash
go run ./cli rest markets
go run ./cli rest markets --market-types SPOT,PERP
```

`--market-types` accepts a comma-separated list of official market types. The command prints the decoded Markets response as indented JSON. It requires no API key and supports normal cancellation through `SIGINT` or `SIGTERM`.

## Deliberately out of scope

Private APIs and streams, authentication/signing, orders, RFQs, accounts, balances, positions, capital operations, borrow/lend, securities and market-session APIs, prediction-specific APIs, stock-price streams, persistence, local-book synchronization, automatic reconnect, and K4K3RU integration are not implemented.
