package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/k4k3ru-hub/backpack/go/rest"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeConn struct {
	reads  chan []byte
	writes chan []byte
	closed chan struct{}
	once   sync.Once
}

func newFakeConn() *fakeConn {
	return &fakeConn{reads: make(chan []byte, 8), writes: make(chan []byte, 8), closed: make(chan struct{})}
}
func (f *fakeConn) ReadMessage() (int, []byte, error) {
	select {
	case b := <-f.reads:
		return 1, b, nil
	case <-f.closed:
		return 0, nil, io.EOF
	}
}
func (f *fakeConn) WriteMessage(_ int, b []byte) error {
	f.writes <- append([]byte(nil), b...)
	return nil
}
func (f *fakeConn) WriteControl(int, []byte, time.Time) error { return nil }
func (f *fakeConn) Close() error                              { f.once.Do(func() { close(f.closed) }); return nil }

type fakeDialer struct {
	conn  Connection
	calls int
}

func (d *fakeDialer) DialContext(context.Context, string, http.Header) (Connection, *http.Response, error) {
	d.calls++
	return d.conn, nil, nil
}

// TestClientLifecycleAndSubscriptions verifies injected dialing, batch subscription, unsubscription, and idempotent close.
//
// Version:
//   - 2026-08-19: Added.
func TestClientLifecycleAndSubscriptions(t *testing.T) {
	conn := newFakeConn()
	dialer := &fakeDialer{conn: conn}
	client, err := NewClient(WithDialer(dialer), WithEventBuffer(2))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if dialer.calls != 1 {
		t.Fatalf("dial calls=%d", dialer.calls)
	}
	if err := client.Subscribe(ctx, "depth.SOL_USDC", "trade.SOL_USDC"); err != nil {
		t.Fatal(err)
	}
	var req struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
	}
	_ = json.Unmarshal(<-conn.writes, &req)
	if req.Method != "SUBSCRIBE" || len(req.Params) != 2 {
		t.Fatalf("subscribe=%+v", req)
	}
	if err := client.Unsubscribe(ctx, "depth.SOL_USDC"); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(<-conn.writes, &req)
	if req.Method != "UNSUBSCRIBE" {
		t.Fatalf("unsubscribe=%+v", req)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestDecodeEvents verifies routing and Backpack-specific wire semantics.
//
// Version:
//   - 2026-08-19: Added.
func TestDecodeEvents(t *testing.T) {
	tests := []struct {
		name, message string
		check         func(Event) bool
	}{
		{"book nullable", `{"stream":"bookTicker.X","data":{"e":"bookTicker","E":1694687965941000,"s":"X","a":null,"A":null,"b":"1","B":"2","u":"9","T":1694687965940999}}`, func(e Event) bool { v := e.(*BookTickerEvent); return v.BestAskPrice == nil && v.BestBidPrice != nil }},
		{"depth", `{"stream":"depth.X","data":{"e":"depth","E":1,"s":"X","a":[["2","0"]],"b":[["1","3"]],"U":8,"u":9,"T":2}}`, func(e Event) bool {
			v := e.(*DepthEvent)
			return v.FirstUpdateID == 8 && v.LastUpdateID == 9 && v.Asks[0].Quantity == "0"
		}},
		{"trade", `{"stream":"trade.X","data":{"e":"trade","E":1,"s":"X","p":"1","q":"2","b":"3","a":"4","t":5,"T":6,"m":true}}`, func(e Event) bool { v := e.(*TradeEvent); return v.TradeID == 5 && v.IsBuyerMaker }},
		{"ticker", `{"stream":"ticker.X","data":{"e":"ticker","E":1,"s":"X","o":"1","c":"2","h":"3","l":"0","v":"4","V":"5","n":6}}`, func(e Event) bool { return e.(*TickerEvent).QuoteVolume == "5" }},
		{"external", `{"stream":"externalTicker.X","data":{"e":"externalTicker","E":1,"s":"X","o":"1","c":"2","h":"3","l":"0","v":"4","V":"5","n":6}}`, func(e Event) bool { _, ok := e.(*ExternalTickerEvent); return ok }},
		{"kline", `{"stream":"kline.1m.X","data":{"e":"kline","E":1,"s":"X","t":"2026-08-19T00:00:00Z","T":"2026-08-19T00:01:00Z","o":"1","c":"2","h":"3","l":"0","v":"4","n":5,"X":true}}`, func(e Event) bool { v := e.(*KlineEvent); _, err := v.StartTime(); return err == nil && v.Final }},
		{"mark optional", `{"stream":"markPrice.X","data":{"e":"markPrice","E":1,"s":"X","p":"1","T":2}}`, func(e Event) bool {
			v := e.(*MarkPriceEvent)
			return v.EstimatedFundingRate == nil && v.IndexPrice == nil && v.NextFundingTimestamp == nil
		}},
		{"oi", `{"stream":"openInterest.X","data":{"e":"openInterest","E":1,"s":"X","o":"100"}}`, func(e Event) bool { return e.(*OpenInterestEvent).OpenInterest == "100" }},
		{"liquidation", `{"stream":"liquidation.X","data":{"e":"liquidation","E":1,"T":2,"s":"X","S":"Bid","p":"3","q":"4"}}`, func(e Event) bool { return e.(*LiquidationEvent).Side == SideBid }},
		{"unknown", `{"stream":"future.X","data":{"e":"future","x":1}}`, func(e Event) bool { _, ok := e.(*UnknownEvent); return ok }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := DecodeEvent([]byte(tt.message))
			if err != nil {
				t.Fatal(err)
			}
			if event.StreamName() == "" || !tt.check(event) {
				t.Fatalf("event=%#v", event)
			}
		})
	}
	if _, err := DecodeEvent([]byte("{")); err == nil {
		t.Fatal("malformed envelope accepted")
	}
	if _, err := DecodeEvent([]byte(`{"stream":"depth.X","data":{"a":[["1"]]}}`)); err == nil {
		t.Fatal("malformed level accepted")
	}
}

// TestReadLoopDeliveryAndFailures verifies delivery, malformed-payload errors, cancellation, and slow-consumer behavior.
//
// Version:
//   - 2026-08-19: Added.
func TestReadLoopDeliveryAndFailures(t *testing.T) {
	conn := newFakeConn()
	client, _ := NewClient(WithDialer(&fakeDialer{conn: conn}), WithEventBuffer(1))
	ctx, cancel := context.WithCancel(context.Background())
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	conn.reads <- []byte(`{"stream":"openInterest.X","data":{"e":"openInterest","E":1,"s":"X","o":"1"}}`)
	select {
	case <-client.Events():
	case <-time.After(time.Second):
		t.Fatal("event timeout")
	}
	conn.reads <- []byte("{")
	select {
	case err := <-client.Errors():
		if err == nil {
			t.Fatal("nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("decode error timeout")
	}
	cancel()
	_ = client.Close()
	conn2 := newFakeConn()
	client2, _ := NewClient(WithDialer(&fakeDialer{conn: conn2}), WithEventBuffer(1))
	_ = client2.Connect(context.Background())
	msg := []byte(`{"stream":"openInterest.X","data":{"e":"openInterest","E":1,"s":"X","o":"1"}}`)
	conn2.reads <- msg
	conn2.reads <- msg
	select {
	case err := <-client2.Errors():
		if !errors.Is(err, ErrSlowConsumer) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow consumer timeout")
	}
	_ = client2.Close()
}

// TestStreamBuildersAndTimestamps verifies stream validation and timestamp units.
//
// Version:
//   - 2026-08-19: Added.
func TestStreamBuildersAndTimestamps(t *testing.T) {
	if s, err := KlineStream(rest.KlineInterval1Minute, "SOL_USDC"); err != nil || s != "kline.1m.SOL_USDC" {
		t.Fatalf("stream=%q err=%v", s, err)
	}
	if _, err := DepthStream(""); err == nil {
		t.Fatal("empty symbol accepted")
	}
	micro, _ := MicrosecondTimestamp(1_500_000).Time()
	milli, _ := MillisecondTimestamp(1_500).Time()
	if !micro.Equal(milli) {
		t.Fatalf("micro=%v milli=%v", micro, milli)
	}
}
