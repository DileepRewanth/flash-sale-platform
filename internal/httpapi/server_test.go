package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/metrics"
	"github.com/DileepRewanth/flash-sale-platform/internal/psp"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
	"github.com/DileepRewanth/flash-sale-platform/internal/token"
	"github.com/DileepRewanth/flash-sale-platform/internal/worker"
)

var secret = []byte(strings.Repeat("w", 32))

func newTestServer(t *testing.T, units int, maxInFlight int) (*httptest.Server, *Server) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	ids := make([]string, units)
	for i := range ids {
		ids[i] = "u" + strconv.Itoa(i)
	}
	st := store.NewMemory(store.Config{PaymentGrace: 2 * time.Minute, HardDeadline: 30 * time.Minute, Fungible: true})
	st.SeedSale("s", ids)
	gate := inventory.NewGate(rdb, 2)
	if err := gate.Seed(context.Background(), "s", ids); err != nil {
		t.Fatal(err)
	}
	signer, _ := token.NewSigner([]byte(strings.Repeat("k", 32)))
	fake := psp.NewFake()
	w := &worker.Worker{Store: st, Gate: gate, PSP: fake, Sale: "s", Now: time.Now, ResendWindow: 5 * time.Minute, SweepMargin: 10 * time.Second}
	s := New(Config{Sale: "s", HoldTTL: 5 * time.Minute, HardDeadline: 30 * time.Minute, WebhookSecret: secret, AdminToken: "admin", MaxInFlight: maxInFlight, MaxWebhooks: 8},
		gate, st, signer, w, metrics.New(), fake.NetCharges, time.Now)
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, s
}

func call(t *testing.T, method, url string, headers map[string]string, body []byte) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestReserveOrderAndConfirmOverHTTP(t *testing.T) {
	ts, s := newTestServer(t, 3, 0)
	code, hold := call(t, "POST", ts.URL+"/v1/sales/s/holds", map[string]string{"X-User-ID": "alice"}, nil)
	if code != http.StatusCreated {
		t.Fatalf("reserve: %d %v", code, hold)
	}
	body, _ := json.Marshal(map[string]string{"hold_token": hold["hold_token"].(string)})
	hdr := map[string]string{"X-User-ID": "alice", "Idempotency-Key": "k1"}
	code, first := call(t, "POST", ts.URL+"/v1/orders", hdr, body)
	if code != http.StatusAccepted {
		t.Fatalf("order: %d %v", code, first)
	}
	code, again := call(t, "POST", ts.URL+"/v1/orders", hdr, body)
	if code != http.StatusOK || again["order_id"] != first["order_id"] {
		t.Fatalf("idempotent replay: %d %v", code, again)
	}
	s.worker.DispatchOnce(context.Background())
	code, got := call(t, "GET", ts.URL+"/v1/orders/"+first["order_id"].(string), map[string]string{"X-User-ID": "alice"}, nil)
	if code != http.StatusOK || got["state"] != "CONFIRMED" {
		t.Fatalf("get order: %d %v", code, got)
	}
	code, audit := call(t, "GET", ts.URL+"/v1/admin/sales/s/audit", map[string]string{"Authorization": "Bearer admin"}, nil)
	if code != http.StatusOK || audit["oversold_units"] != float64(0) || audit["sold_units"] != float64(1) {
		t.Fatalf("audit: %d %v", code, audit)
	}
}

func TestSoldOutIsAnAnswerNotAnError(t *testing.T) {
	ts, _ := newTestServer(t, 1, 0)
	call(t, "POST", ts.URL+"/v1/sales/s/holds", map[string]string{"X-User-ID": "alice"}, nil)
	code, out := call(t, "POST", ts.URL+"/v1/sales/s/holds", map[string]string{"X-User-ID": "bob"}, nil)
	if code != http.StatusConflict || out["error"] != "sold_out" {
		t.Fatalf("got %d %v, want 409 sold_out", code, out)
	}
}

func TestWebhookSignatureAndDuplicates(t *testing.T) {
	ts, s := newTestServer(t, 1, 0)
	o, a, _, err := s.store.CreateOrder(store.CreateOrderRequest{Sale: "s", Unit: "u0", User: "alice", IdemKey: "k", HoldExpires: time.Now().Add(time.Minute)}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	s.store.ClaimNextAttempt(time.Now())
	ev, _ := json.Marshal(map[string]string{"event_id": "evt-1", "attempt_id": a.ID, "status": "SUCCEEDED"})
	if code, _ := call(t, "POST", ts.URL+"/webhooks/psp", map[string]string{"X-Webhook-Timestamp": "1", "X-Webhook-Signature": "bad"}, ev); code != http.StatusUnauthorized {
		t.Fatalf("unsigned webhook: got %d, want 401", code)
	}
	ts1 := strconv.FormatInt(time.Now().Unix(), 10)
	hdr := map[string]string{"X-Webhook-Timestamp": ts1, "X-Webhook-Signature": SignWebhook(secret, ts1, ev)}
	if code, out := call(t, "POST", ts.URL+"/webhooks/psp", hdr, ev); code != http.StatusOK || out["applied"] != true {
		t.Fatalf("first delivery: %d %v", code, out)
	}
	if code, out := call(t, "POST", ts.URL+"/webhooks/psp", hdr, ev); code != http.StatusOK || out["duplicate"] != true {
		t.Fatalf("redelivery: %d %v", code, out)
	}
	if got, _ := s.store.Order(o.ID); got.State != "CONFIRMED" {
		t.Fatalf("order %s, want CONFIRMED", got.State)
	}
}

func TestLoadSheddingRejectsFastWhenFull(t *testing.T) {
	ts, s := newTestServer(t, 1, 1)
	s.customer <- struct{}{} // occupy the only customer slot
	defer func() { <-s.customer }()
	req, _ := http.NewRequest("POST", ts.URL+"/v1/sales/s/holds", nil)
	req.Header.Set("X-User-ID", "alice")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d Retry-After=%q, want 503 with Retry-After", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	ev, _ := json.Marshal(map[string]string{"event_id": "e", "attempt_id": "none", "status": "FAILED"})
	now := strconv.FormatInt(time.Now().Unix(), 10)
	code, _ := call(t, "POST", ts.URL+"/webhooks/psp", map[string]string{"X-Webhook-Timestamp": now, "X-Webhook-Signature": SignWebhook(secret, now, ev)}, ev)
	if code == http.StatusServiceUnavailable {
		t.Fatal("webhooks have their own pool and must not be shed by a customer flood")
	}
}

func TestPauseKillSwitch(t *testing.T) {
	ts, _ := newTestServer(t, 2, 0)
	body, _ := json.Marshal(map[string]bool{"paused": true})
	if code, _ := call(t, "POST", ts.URL+"/v1/admin/pause", map[string]string{"Authorization": "Bearer admin"}, body); code != http.StatusOK {
		t.Fatalf("pause: %d", code)
	}
	if code, _ := call(t, "POST", ts.URL+"/v1/sales/s/holds", map[string]string{"X-User-ID": "alice"}, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("reserve while paused: %d, want 503", code)
	}
}
