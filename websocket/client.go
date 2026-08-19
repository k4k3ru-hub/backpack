package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	gorilla "github.com/gorilla/websocket"
)

const DefaultEndpointURL = "wss://ws.backpack.exchange"

var ErrSlowConsumer = errors.New("websocket event consumer is too slow")

// Connection is the private-library-neutral WebSocket connection contract.
type Connection interface {
	ReadMessage() (int, []byte, error)
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
	Close() error
}

// Dialer creates WebSocket connections.
type Dialer interface {
	DialContext(context.Context, string, http.Header) (Connection, *http.Response, error)
}
type gorillaDialer struct{ d *gorilla.Dialer }

func (g gorillaDialer) DialContext(ctx context.Context, u string, h http.Header) (Connection, *http.Response, error) {
	return g.d.DialContext(ctx, u, h)
}

// ClientOption configures a WebSocket client.
type ClientOption func(*clientConfig) error
type clientConfig struct {
	endpoint string
	dialer   Dialer
	buffer   int
	header   http.Header
}

// WithEndpointURL sets the WebSocket endpoint.
//
// Version:
//   - 2026-08-19: Added.
func WithEndpointURL(v string) ClientOption {
	return func(c *clientConfig) error {
		if v == "" {
			return fmt.Errorf("failed to configure websocket client: endpoint_url=empty")
		}
		c.endpoint = v
		return nil
	}
}

// WithDialer injects a WebSocket dialer.
//
// Version:
//   - 2026-08-19: Added.
func WithDialer(v Dialer) ClientOption {
	return func(c *clientConfig) error {
		if v == nil {
			return fmt.Errorf("failed to configure websocket client: dialer=null")
		}
		c.dialer = v
		return nil
	}
}

// WithEventBuffer sets the bounded event channel capacity.
//
// Version:
//   - 2026-08-19: Added.
func WithEventBuffer(v int) ClientOption {
	return func(c *clientConfig) error {
		if v <= 0 {
			return fmt.Errorf("failed to configure websocket client: event_buffer=out_of_range")
		}
		c.buffer = v
		return nil
	}
}

// Client manages one public WebSocket connection. It does not reconnect automatically.
type Client struct {
	endpoint      string
	dialer        Dialer
	header        http.Header
	events        chan Event
	errs          chan error
	mu            sync.Mutex
	writeMu       sync.Mutex
	conn          Connection
	cancel        context.CancelFunc
	subscriptions map[string]struct{}
	closeOnce     sync.Once
	done          chan struct{}
}

// NewClient creates a disconnected WebSocket client.
//
// Version:
//   - 2026-08-19: Added.
func NewClient(options ...ClientOption) (*Client, error) {
	cfg := clientConfig{endpoint: DefaultEndpointURL, dialer: gorillaDialer{d: gorilla.DefaultDialer}, buffer: 256}
	for _, o := range options {
		if o == nil {
			return nil, fmt.Errorf("failed to create websocket client: option=null")
		}
		if err := o(&cfg); err != nil {
			return nil, fmt.Errorf("failed to create websocket client: %w", err)
		}
	}
	return &Client{endpoint: cfg.endpoint, dialer: cfg.dialer, header: cfg.header.Clone(), events: make(chan Event, cfg.buffer), errs: make(chan error, 8), subscriptions: map[string]struct{}{}, done: make(chan struct{})}, nil
}

// Connect establishes the WebSocket connection and starts message delivery.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Connect(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("failed to connect websocket: client=null")
	}
	if ctx == nil {
		return fmt.Errorf("failed to connect websocket: context=null")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return fmt.Errorf("failed to connect websocket: connection=already_connected")
	}
	select {
	case <-c.done:
		return fmt.Errorf("failed to connect websocket: client=closed")
	default:
	}
	conn, resp, err := c.dialer.DialContext(ctx, c.endpoint, c.header.Clone())
	if err != nil {
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		return fmt.Errorf("failed to connect websocket: %w: status_code=%d", err, status)
	}
	readCtx, cancel := context.WithCancel(ctx)
	c.conn = conn
	c.cancel = cancel
	go c.readLoop(readCtx, conn)
	go func() {
		<-readCtx.Done()
		_ = c.Close()
	}()
	return nil
}

// Events returns the bounded public event channel.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Events() <-chan Event {
	if c == nil {
		return nil
	}
	return c.events
}

// Errors returns asynchronous decode, disconnect, and slow-consumer errors.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Errors() <-chan error {
	if c == nil {
		return nil
	}
	return c.errs
}

// Subscribe subscribes to one or more public streams in one serialized request.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Subscribe(ctx context.Context, streams ...string) error {
	return c.changeSubscriptions(ctx, "SUBSCRIBE", streams)
}

// Unsubscribe unsubscribes from one or more public streams in one serialized request.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Unsubscribe(ctx context.Context, streams ...string) error {
	return c.changeSubscriptions(ctx, "UNSUBSCRIBE", streams)
}
func (c *Client) changeSubscriptions(ctx context.Context, method string, streams []string) error {
	if c == nil {
		return fmt.Errorf("failed to change websocket subscriptions: client=null")
	}
	if ctx == nil {
		return fmt.Errorf("failed to change websocket subscriptions: context=null")
	}
	if len(streams) == 0 {
		return fmt.Errorf("failed to validate websocket subscription parameters: streams=empty")
	}
	seen := map[string]struct{}{}
	for _, s := range streams {
		if s == "" {
			return fmt.Errorf("failed to validate websocket subscription parameters: stream=empty")
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
	}
	params := make([]string, 0, len(seen))
	for _, s := range streams {
		if _, ok := seen[s]; ok {
			params = append(params, s)
			delete(seen, s)
		}
	}
	payload, err := json.Marshal(struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
	}{method, params})
	if err != nil {
		return fmt.Errorf("failed to encode websocket subscription request: %w", err)
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("failed to change websocket subscriptions: connection=null")
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("failed to change websocket subscriptions: %w", ctx.Err())
	default:
	}
	c.writeMu.Lock()
	err = conn.WriteMessage(gorilla.TextMessage, payload)
	c.writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("failed to send websocket subscription request: %w", err)
	}
	c.mu.Lock()
	for _, s := range params {
		if method == "SUBSCRIBE" {
			c.subscriptions[s] = struct{}{}
		} else {
			delete(c.subscriptions, s)
		}
	}
	c.mu.Unlock()
	return nil
}

// Close closes the connection and is safe to call repeatedly.
//
// Version:
//   - 2026-08-19: Added.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	var closeErr error
	c.closeOnce.Do(func() {
		close(c.done)
		c.mu.Lock()
		if c.cancel != nil {
			c.cancel()
		}
		conn := c.conn
		c.conn = nil
		c.mu.Unlock()
		if conn != nil {
			c.writeMu.Lock()
			_ = conn.WriteControl(gorilla.CloseMessage, gorilla.FormatCloseMessage(gorilla.CloseNormalClosure, ""), time.Now().Add(time.Second))
			closeErr = conn.Close()
			c.writeMu.Unlock()
		}
	})
	if closeErr != nil {
		return fmt.Errorf("failed to close websocket: %w", closeErr)
	}
	return nil
}
func (c *Client) readLoop(ctx context.Context, conn Connection) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				c.notifyError(fmt.Errorf("failed to read websocket message: %w", err))
				_ = c.Close()
				return
			}
		}
		event, err := DecodeEvent(message)
		if err != nil {
			c.notifyError(err)
			continue
		}
		select {
		case c.events <- event:
		case <-ctx.Done():
			return
		default:
			c.notifyError(ErrSlowConsumer)
			_ = c.Close()
			return
		}
	}
}
func (c *Client) notifyError(err error) {
	select {
	case c.errs <- err:
	default:
	}
}
