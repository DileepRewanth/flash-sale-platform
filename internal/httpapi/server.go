// Package httpapi exposes the reservation, order, webhook and admin endpoints.
//
// Customer routes and the webhook route draw from separate concurrency pools, a
// bulkhead, so a browse or reservation flood can never starve payment webhooks.
// When a pool is full the request is shed at once with Retry-After, because a
// queued request would only time out later and waste capacity on the way.
package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/metrics"
	"github.com/DileepRewanth/flash-sale-platform/internal/reconcile"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
	"github.com/DileepRewanth/flash-sale-platform/internal/token"
	"github.com/DileepRewanth/flash-sale-platform/internal/worker"
)

// Config holds the server settings.
type Config struct {
	Sale          string
	HoldTTL       time.Duration
	HardDeadline  time.Duration
	WebhookSecret []byte
	AdminToken    string
	MaxInFlight   int // customer requests per instance; 0 means unlimited
	MaxWebhooks   int // webhook requests per instance; 0 means unlimited
}

// Server wires the HTTP layer to the gate, the store and the worker.
type Server struct {
	cfg      Config
	gate     *inventory.Gate
	store    *store.Memory
	signer   *token.Signer
	worker   *worker.Worker
	metrics  *metrics.M
	ledger   func() map[string]int
	now      func() time.Time
	paused   atomic.Bool
	customer chan struct{}
	hooks    chan struct{}
	drift    reconcile.DriftTracker
	mux      *http.ServeMux
}

// New builds a server and registers its routes.
func New(cfg Config, gate *inventory.Gate, st *store.Memory, signer *token.Signer, w *worker.Worker,
	m *metrics.M, ledger func() map[string]int, now func() time.Time) *Server {
	s := &Server{cfg: cfg, gate: gate, store: st, signer: signer, worker: w, metrics: m, ledger: ledger, now: now, mux: http.NewServeMux()}
	if cfg.MaxInFlight > 0 {
		s.customer = make(chan struct{}, cfg.MaxInFlight)
	}
	if cfg.MaxWebhooks > 0 {
		s.hooks = make(chan struct{}, cfg.MaxWebhooks)
	}
	s.route("POST /v1/sales/{sale}/holds", "reserve", s.customer, s.reserve)
	s.route("POST /v1/orders", "create_order", s.customer, s.createOrder)
	s.route("GET /v1/orders/{id}", "get_order", s.customer, s.getOrder)
	s.route("POST /v1/orders/{id}/cancel", "cancel_order", s.customer, s.cancelOrder)
	s.route("POST /webhooks/psp", "webhook", s.hooks, s.webhook)
	s.route("GET /v1/admin/sales/{sale}/audit", "audit", nil, s.audit)
	s.route("POST /v1/admin/sales/{sale}/repair", "repair", nil, s.repair)
	s.route("POST /v1/admin/sales/{sale}/rebuild", "rebuild", nil, s.rebuild)
	s.route("POST /v1/admin/pause", "pause", nil, s.pause)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) route(pattern, name string, slots chan struct{}, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if slots != nil {
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				s.metrics.Shed.WithLabelValues(name).Inc()
				s.metrics.Requests.WithLabelValues(name, "shed").Inc()
				w.Header().Set("Retry-After", strconv.Itoa(1+rand.Intn(3))) // jittered, so clients do not retry in lockstep
				writeErr(w, http.StatusServiceUnavailable, "overloaded, retry later")
				return
			}
		}
		s.metrics.InFlight.Inc()
		defer s.metrics.InFlight.Dec()
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		h(rec, r)
		s.metrics.Duration.WithLabelValues(name).Observe(time.Since(start).Seconds())
		s.metrics.Requests.WithLabelValues(name, strconv.Itoa(rec.code)).Inc()
	})
}

var userPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// userID reads the caller. Real authentication belongs at the gateway; it is out of scope here.
func userID(r *http.Request) (string, bool) {
	u := r.Header.Get("X-User-ID")
	return u, userPattern.MatchString(u)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// reserve is step 1: a Redis-only hold that returns a signed hold token.
func (s *Server) reserve(w http.ResponseWriter, r *http.Request) {
	user, ok := userID(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "missing or invalid X-User-ID")
		return
	}
	if r.PathValue("sale") != s.cfg.Sale {
		writeErr(w, http.StatusNotFound, "unknown sale")
		return
	}
	if s.paused.Load() {
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusServiceUnavailable, "reservations paused")
		return
	}
	h, replay, err := s.gate.Reserve(r.Context(), s.cfg.Sale, user, s.cfg.HoldTTL, s.now())
	switch {
	case errors.Is(err, inventory.ErrSoldOut):
		s.metrics.Reservations.WithLabelValues("sold_out").Inc()
		writeErr(w, http.StatusConflict, "sold_out") // a correct answer, so it does not count against availability
		return
	case errors.Is(err, inventory.ErrInProgress):
		s.metrics.Reservations.WithLabelValues("in_progress").Inc()
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusConflict, "in_progress")
		return
	case err != nil:
		s.metrics.Reservations.WithLabelValues("unavailable").Inc()
		w.Header().Set("Retry-After", strconv.Itoa(1+rand.Intn(3)))
		writeErr(w, http.StatusServiceUnavailable, "inventory unavailable, retry")
		return
	}
	tok, err := s.signer.Sign(token.Hold{HoldID: h.ID, Sale: h.Sale, Unit: h.Unit, User: h.User, Bucket: h.Bucket, Expires: h.Expires.UnixMilli()})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not sign hold")
		return
	}
	result := "held"
	if replay {
		result = "replay"
	}
	s.metrics.Reservations.WithLabelValues(result).Inc()
	writeJSON(w, http.StatusCreated, map[string]any{"hold_token": tok, "unit": h.Unit, "expires_at": h.Expires})
}

type orderView struct {
	OrderID   string `json:"order_id"`
	State     string `json:"state"`
	Unit      string `json:"unit"`
	AttemptID string `json:"attempt_id,omitempty"`
	Payment   string `json:"payment,omitempty"`
	Money     string `json:"money,omitempty"`
}

func view(o store.Order, a store.Attempt) orderView {
	return orderView{OrderID: o.ID, State: string(o.State), Unit: o.Unit, AttemptID: a.ID, Payment: string(a.Outcome), Money: string(a.Disposition)}
}

// createOrder is step 2: verify the hold token, then claim the unit durably.
// The client gets 202 at once and polls; it never waits on the provider call.
func (s *Server) createOrder(w http.ResponseWriter, r *http.Request) {
	user, ok := userID(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "missing or invalid X-User-ID")
		return
	}
	idem := r.Header.Get("Idempotency-Key")
	if idem == "" || len(idem) > 128 {
		writeErr(w, http.StatusBadRequest, "missing or invalid Idempotency-Key")
		return
	}
	var body struct {
		HoldToken string `json:"hold_token"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	h, err := s.signer.Verify(body.HoldToken, s.now())
	if errors.Is(err, token.ErrExpired) {
		writeErr(w, http.StatusGone, "hold expired, reserve again")
		return
	}
	if err != nil || h.User != user {
		writeErr(w, http.StatusUnauthorized, "invalid hold token")
		return
	}
	sum := sha256.Sum256([]byte(body.HoldToken)) // the request fingerprint stored with the idempotency key
	o, a, replay, err := s.store.CreateOrder(store.CreateOrderRequest{
		Sale: h.Sale, Unit: h.Unit, User: user, HoldID: h.HoldID, IdemKey: idem,
		RequestHash: hex.EncodeToString(sum[:]), HoldExpires: h.ExpiresAt(),
	}, s.now())
	hold := inventory.Hold{ID: h.HoldID, Sale: h.Sale, Unit: h.Unit, User: h.User, Bucket: h.Bucket}
	switch {
	case errors.Is(err, store.ErrIdempotencyReuse):
		writeErr(w, http.StatusUnprocessableEntity, "idempotency key reused with a different request")
		return
	case errors.Is(err, store.ErrHoldExpired):
		writeErr(w, http.StatusGone, "hold expired, reserve again")
		return
	case errors.Is(err, store.ErrUserHasOrder):
		writeErr(w, http.StatusConflict, "user already has an active order in this sale")
		return
	case errors.Is(err, store.ErrUnitTaken):
		_ = s.gate.Forget(r.Context(), hold) // the durable store gave this unit to someone else
		writeErr(w, http.StatusConflict, "unit no longer available, reserve again")
		return
	case err != nil:
		writeErr(w, http.StatusInternalServerError, "could not create order")
		return
	}
	code := http.StatusAccepted
	if replay {
		code = http.StatusOK
	} else {
		_ = s.gate.Forget(r.Context(), hold) // the durable store owns the unit now
	}
	writeJSON(w, code, view(o, a))
}

func (s *Server) getOrder(w http.ResponseWriter, r *http.Request) {
	user, ok := userID(r)
	o, found := s.store.Order(r.PathValue("id"))
	if !ok || !found || o.User != user {
		writeErr(w, http.StatusNotFound, "order not found")
		return
	}
	var last store.Attempt
	if as := s.store.Attempts(o.ID); len(as) > 0 {
		last = as[len(as)-1]
	}
	writeJSON(w, http.StatusOK, view(o, last))
}

func (s *Server) cancelOrder(w http.ResponseWriter, r *http.Request) {
	user, ok := userID(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "missing or invalid X-User-ID")
		return
	}
	o, err := s.store.CancelOrder(r.PathValue("id"), user)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "order not found")
		return
	case errors.Is(err, store.ErrNotAllowed):
		writeErr(w, http.StatusConflict, "order can no longer be cancelled")
		return
	}
	s.worker.RelayOnce(r.Context())
	writeJSON(w, http.StatusOK, view(o, store.Attempt{}))
}

type webhookEvent struct {
	EventID   string `json:"event_id"`
	AttemptID string `json:"attempt_id"`
	Status    string `json:"status"`
}

// SignWebhook computes the signature a provider sends: hex HMAC-SHA256 of "timestamp.body".
func SignWebhook(secret []byte, timestamp string, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(timestamp + "."))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func (s *Server) validSignature(ts, sig string, body []byte) bool {
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}
	if d := s.now().Sub(time.Unix(sec, 0)); d > 5*time.Minute || d < -5*time.Minute {
		return false // stale or future timestamps are replays
	}
	return hmac.Equal([]byte(SignWebhook(s.cfg.WebhookSecret, ts, body)), []byte(sig))
}

// webhook applies an asynchronous provider event through the same state machine
// as the worker and the reconciler.
func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "unreadable body")
		return
	}
	if !s.validSignature(r.Header.Get("X-Webhook-Timestamp"), r.Header.Get("X-Webhook-Signature"), body) {
		writeErr(w, http.StatusUnauthorized, "invalid signature")
		return
	}
	var ev webhookEvent
	if json.Unmarshal(body, &ev) != nil || ev.EventID == "" || ev.AttemptID == "" {
		writeErr(w, http.StatusBadRequest, "invalid event")
		return
	}
	var res store.Result
	switch ev.Status {
	case "SUCCEEDED":
		res = store.ResultSucceeded
	case "FAILED":
		res = store.ResultFailed
	case "CANCELLED":
		res = store.ResultCancelled
	default:
		writeErr(w, http.StatusBadRequest, "unknown status")
		return
	}
	out, err := s.store.ApplyWebhook(ev.EventID, ev.AttemptID, res, s.now())
	if errors.Is(err, store.ErrNotFound) {
		// A charge we have no record of: accept it so the provider stops retrying.
		// The orphan-charge detector flags it from the provider ledger.
		writeJSON(w, http.StatusAccepted, map[string]any{"applied": false, "reason": "unknown attempt"})
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not apply event")
		return
	}
	if !out.Duplicate {
		s.metrics.PaymentResults.WithLabelValues(string(res)).Inc()
	}
	s.worker.RelayOnce(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"applied": !out.Duplicate, "duplicate": out.Duplicate})
}

func (s *Server) admin(r *http.Request) bool {
	want := "Bearer " + s.cfg.AdminToken
	return s.cfg.AdminToken != "" && hmac.Equal([]byte(r.Header.Get("Authorization")), []byte(want))
}

func (s *Server) adminSale(w http.ResponseWriter, r *http.Request) bool {
	if !s.admin(r) {
		writeErr(w, http.StatusUnauthorized, "admin token required")
		return false
	}
	if r.PathValue("sale") != s.cfg.Sale {
		writeErr(w, http.StatusNotFound, "unknown sale")
		return false
	}
	return true
}

func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	if !s.adminSale(w, r) {
		return
	}
	rep, err := s.Detect(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) repair(w http.ResponseWriter, r *http.Request) {
	if !s.adminSale(w, r) {
		return
	}
	rep, err := s.Detect(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	added, removed := reconcile.Repair(r.Context(), s.gate, s.store, s.cfg.Sale, rep.MissingFromRedis, rep.StaleInRedis)
	s.metrics.DriftRepaired.Add(float64(added + removed))
	writeJSON(w, http.StatusOK, map[string]int{"added": added, "removed": removed})
}

func (s *Server) rebuild(w http.ResponseWriter, r *http.Request) {
	if !s.adminSale(w, r) {
		return
	}
	free := s.store.FreeUnits(s.cfg.Sale)
	if err := s.gate.Rebuild(r.Context(), s.cfg.Sale, free); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "redis unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"free_units": len(free)})
}

// pause is the reservation kill switch. Orders already in payment continue.
func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	if !s.admin(r) {
		writeErr(w, http.StatusUnauthorized, "admin token required")
		return
	}
	var body struct {
		Paused bool `json:"paused"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<10)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	s.paused.Store(body.Paused)
	writeJSON(w, http.StatusOK, map[string]bool{"reservations_paused": body.Paused})
}

// Detect runs the detectors and publishes the result as metrics.
func (s *Server) Detect(ctx context.Context) (reconcile.Report, error) {
	free, held, err := s.gate.State(ctx, s.cfg.Sale)
	if err != nil {
		return reconcile.Report{}, err
	}
	rep := reconcile.Detect(reconcile.Input{
		Snapshot: s.store.Snapshot(s.cfg.Sale), RedisFree: free, RedisHeld: held,
		NetCharges: s.ledger(), Now: s.now(), HardDeadline: s.cfg.HardDeadline,
	})
	s.metrics.SetReport(rep)
	return rep, nil
}

// RunDetectors runs the detectors on a schedule. With autoRepair, drift seen in
// two consecutive scans is repaired: the recovery path for a lost relay event.
func (s *Server) RunDetectors(ctx context.Context, every time.Duration, autoRepair bool) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rep, err := s.Detect(ctx)
		if err != nil || !autoRepair {
			continue
		}
		missing, stale := s.drift.Stable(rep)
		added, removed := reconcile.Repair(ctx, s.gate, s.store, s.cfg.Sale, missing, stale)
		s.metrics.DriftRepaired.Add(float64(added + removed))
	}
}
