// Package store is the durable commit point for unit ownership and payments.
//
// The prototype keeps it in memory behind one mutex, so it runs anywhere with no
// database. Each exported method mirrors one Postgres transaction from the target
// schema in db/schema.sql: the mutex plays the role of the row locks, and each
// check mirrors a WHERE clause or a unique index. A unit changes owner only through
// a guarded update from FREE, which is the rule that makes double-selling impossible.
package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/DileepRewanth/flash-sale-platform/internal/payment"
)

var (
	ErrUnitTaken        = errors.New("unit is no longer available")
	ErrHoldExpired      = errors.New("hold has expired")
	ErrUserHasOrder     = errors.New("user already has an active order in this sale")
	ErrIdempotencyReuse = errors.New("idempotency key reused with a different request")
	ErrNotFound         = errors.New("not found")
	ErrNotAllowed       = errors.New("operation not allowed in the current state")
)

// UnitState is the durable ownership state of one unit.
type UnitState string

const (
	UnitFree     UnitState = "FREE"
	UnitReserved UnitState = "RESERVED"
	UnitSold     UnitState = "SOLD"
)

// Unit is one sellable item. Version rises on every ownership change.
type Unit struct {
	Sale    string
	ID      string
	State   UnitState
	OrderID string
	Version int64
}

// Order is the customer-visible purchase.
type Order struct {
	ID              string
	Sale            string
	Unit            string
	User            string
	HoldID          string
	State           payment.OrderState
	PayBy           time.Time // hold expiry plus the payment start grace
	CancelRequested bool
	CreatedAt       time.Time
}

// Attempt is one try at charging for an order.
type Attempt struct {
	ID              string
	OrderID         string
	User            string
	IdemKey         string
	RequestHash     string
	Outcome         payment.Outcome
	Disposition     payment.Disposition
	FirstDispatchAt time.Time
	Sends           int
}

// Release is an outbox event: a unit became FREE at Version and must return to Redis.
type Release struct {
	Sale    string
	Unit    string
	Version int64
}

// Take is an outbox event: a late payment re-acquired a unit, so Redis must stop offering it.
type Take struct {
	Sale string
	Unit string
}

// Config holds the payment timing rules.
type Config struct {
	PaymentGrace time.Duration // after hold expiry, how long a queued attempt may still be sent
	HardDeadline time.Duration // how long an unresolved payment protects its unit
	Fungible     bool          // whether another unit can replace the original on late success
}

// Result is a provider answer, as reported by the worker, a webhook or the reconciler.
type Result string

const (
	ResultSucceeded    Result = "SUCCEEDED"
	ResultFailed       Result = "FAILED"
	ResultCancelled    Result = "CANCELLED"
	ResultNotProcessed Result = "NOT_PROCESSED"
)

// Applied reports what ApplyResult changed.
type Applied struct {
	Action    payment.SuccessAction
	Duplicate bool
	Order     Order
	Attempt   Attempt
}

// Memory is the in-memory durable store.
type Memory struct {
	mu           sync.Mutex
	cfg          Config
	units        map[string]*Unit
	orders       map[string]*Order
	attempts     map[string]*Attempt
	byOrder      map[string][]string
	idem         map[string]string
	activeByUser map[string]string
	webhooks     map[string]bool
	queue        []string            // NOT_SENT attempts: the payment outbox
	unknown      map[string]struct{} // attempts whose outcome is UNKNOWN
	refunds      map[string]struct{} // attempts whose disposition is REFUND_PENDING
	releases     []Release
	takes        []Take
	seq          int64
}

// NewMemory returns an empty store.
func NewMemory(cfg Config) *Memory {
	return &Memory{
		cfg:          cfg,
		units:        map[string]*Unit{},
		orders:       map[string]*Order{},
		attempts:     map[string]*Attempt{},
		byOrder:      map[string][]string{},
		idem:         map[string]string{},
		activeByUser: map[string]string{},
		webhooks:     map[string]bool{},
		unknown:      map[string]struct{}{},
		refunds:      map[string]struct{}{},
	}
}

func key(a, b string) string { return a + "|" + b }

func (m *Memory) nextID(prefix string) string {
	m.seq++
	return fmt.Sprintf("%s-%07d", prefix, m.seq)
}

// SeedSale creates FREE units for a sale.
func (m *Memory) SeedSale(sale string, units []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, u := range units {
		m.units[key(sale, u)] = &Unit{Sale: sale, ID: u, State: UnitFree}
	}
}

// CreateOrderRequest is step 2 of the payment sequence.
type CreateOrderRequest struct {
	Sale, Unit, User, HoldID, IdemKey, RequestHash string
	HoldExpires                                    time.Time
}

// CreateOrder is one transaction: it records the idempotency key, claims the unit
// with a guarded update from FREE, and creates the order and its first attempt.
// The attempt starts NOT_SENT, which doubles as the outbox entry for the worker.
func (m *Memory) CreateOrder(req CreateOrderRequest, now time.Time) (Order, Attempt, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.idem[key(req.User, req.IdemKey)]; ok {
		a := m.attempts[id]
		if a.RequestHash != req.RequestHash {
			return Order{}, Attempt{}, false, ErrIdempotencyReuse
		}
		return *m.orders[a.OrderID], *a, true, nil
	}
	// The hold token's expiry is checked against the store's clock, not the caller's.
	if !now.Before(req.HoldExpires) {
		return Order{}, Attempt{}, false, ErrHoldExpired
	}
	if _, busy := m.activeByUser[key(req.Sale, req.User)]; busy {
		return Order{}, Attempt{}, false, ErrUserHasOrder
	}
	u, ok := m.units[key(req.Sale, req.Unit)]
	if !ok || u.State != UnitFree {
		return Order{}, Attempt{}, false, ErrUnitTaken
	}
	o := &Order{
		ID: m.nextID("ord"), Sale: req.Sale, Unit: req.Unit, User: req.User, HoldID: req.HoldID,
		State: payment.OrderPending, PayBy: req.HoldExpires.Add(m.cfg.PaymentGrace), CreatedAt: now,
	}
	a := m.newAttemptLocked(o, req.User, req.IdemKey, req.RequestHash)
	u.State, u.OrderID = UnitReserved, o.ID
	u.Version++
	m.orders[o.ID] = o
	m.activeByUser[key(o.Sale, o.User)] = o.ID
	return *o, *a, false, nil
}

func (m *Memory) newAttemptLocked(o *Order, user, idemKey, hash string) *Attempt {
	a := &Attempt{
		ID: m.nextID("att"), OrderID: o.ID, User: user, IdemKey: idemKey, RequestHash: hash,
		Outcome: payment.NotSent, Disposition: payment.DispositionNone,
	}
	m.attempts[a.ID] = a
	m.byOrder[o.ID] = append(m.byOrder[o.ID], a.ID)
	m.idem[key(user, idemKey)] = a.ID
	m.queue = append(m.queue, a.ID)
	return a
}

// RetryPayment adds an attempt after a decline, while the hold is still valid.
// Only one attempt per order may be in flight, mirroring a partial unique index.
func (m *Memory) RetryPayment(orderID, user, idemKey, hash string, holdExpires, now time.Time) (Attempt, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.idem[key(user, idemKey)]; ok {
		a := m.attempts[id]
		if a.RequestHash != hash {
			return Attempt{}, false, ErrIdempotencyReuse
		}
		return *a, true, nil
	}
	o, ok := m.orders[orderID]
	if !ok || o.User != user {
		return Attempt{}, false, ErrNotFound
	}
	if o.State != payment.OrderPending || o.CancelRequested || !now.Before(holdExpires) || m.inFlightLocked(o.ID) {
		return Attempt{}, false, ErrNotAllowed
	}
	return *m.newAttemptLocked(o, user, idemKey, hash), false, nil
}

// ClaimNextAttempt is step 3: it takes the next queued attempt and marks it UNKNOWN
// before the provider call, so a crash after this point is resolved by lookup.
// An attempt whose order is past its deadline is cancelled without ever being sent.
func (m *Memory) ClaimNextAttempt(now time.Time) (Attempt, Order, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.queue) > 0 {
		id := m.queue[0]
		m.queue = m.queue[1:]
		a := m.attempts[id]
		if a == nil || a.Outcome != payment.NotSent {
			continue
		}
		o := m.orders[a.OrderID]
		if o.State != payment.OrderPending || o.CancelRequested || !now.Before(o.PayBy) {
			a.Outcome = payment.Cancelled
			if o.State == payment.OrderPending && !m.inFlightLocked(o.ID) {
				state := payment.OrderExpired
				if o.CancelRequested {
					state = payment.OrderCancelled
				}
				m.finishUnpaidLocked(o, state)
			}
			continue
		}
		a.Outcome = payment.Unknown
		a.Sends++
		if a.FirstDispatchAt.IsZero() {
			a.FirstDispatchAt = now
		}
		m.unknown[a.ID] = struct{}{}
		return *a, *o, true
	}
	return Attempt{}, Order{}, false
}

// NoteResend counts another dispatch of the same attempt with the same key.
func (m *Memory) NoteResend(attemptID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if a, ok := m.attempts[attemptID]; ok {
		a.Sends++
	}
}

// ApplyResult is step 5: it applies a provider answer through the state machine.
func (m *Memory) ApplyResult(attemptID string, r Result, now time.Time) (Applied, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyLocked(attemptID, r, now)
}

// ApplyWebhook deduplicates by provider event ID and applies the result in the same
// transaction, so a redelivered event is a no-op. The state machine is the real
// guard: even two copies of one event racing past the dedupe settle identically.
func (m *Memory) ApplyWebhook(eventID, attemptID string, r Result, now time.Time) (Applied, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.webhooks[eventID] {
		return Applied{Duplicate: true}, nil
	}
	out, err := m.applyLocked(attemptID, r, now)
	if err == nil {
		m.webhooks[eventID] = true
	}
	return out, err
}

func (m *Memory) applyLocked(attemptID string, r Result, now time.Time) (Applied, error) {
	a, ok := m.attempts[attemptID]
	if !ok {
		return Applied{}, ErrNotFound
	}
	o := m.orders[a.OrderID]
	var out Applied
	switch r {
	case ResultNotProcessed:
		if a.Outcome == payment.Unknown && payment.NotProcessedResets(a.Sends) {
			a.Outcome = payment.NotSent
			delete(m.unknown, a.ID)
			m.queue = append(m.queue, a.ID)
		} else {
			out.Duplicate = true
		}
	case ResultFailed, ResultCancelled:
		target := payment.Failed
		if r == ResultCancelled {
			target = payment.Cancelled
		}
		if !payment.CanMoveOutcome(a.Outcome, target) {
			out.Duplicate = true // already final, or an older event arriving late
			break
		}
		a.Outcome = target
		delete(m.unknown, a.ID)
		if o.State == payment.OrderPending && o.CancelRequested && !m.inFlightLocked(o.ID) {
			m.finishUnpaidLocked(o, payment.OrderCancelled)
		}
	case ResultSucceeded:
		if a.Outcome == payment.Succeeded {
			out.Duplicate = true
			break
		}
		a.Outcome = payment.Succeeded // the provider is the source of truth for money
		delete(m.unknown, a.ID)
		out.Action = m.settleSuccessLocked(o, a)
	default:
		return Applied{}, fmt.Errorf("unknown result %q", r)
	}
	out.Order, out.Attempt = *o, *a
	return out, nil
}

// settleSuccessLocked records SUCCEEDED and sets the disposition in the same step,
// so a charge the order cannot keep goes straight to REFUND_PENDING.
func (m *Memory) settleSuccessLocked(o *Order, a *Attempt) payment.SuccessAction {
	u := m.units[key(o.Sale, o.Unit)]
	active := m.activeByUser[key(o.Sale, o.User)]
	c := payment.SuccessContext{
		Order:              o.State,
		CancelRequested:    o.CancelRequested,
		OrderHasRetained:   m.hasDispositionLocked(o.ID, payment.Retained),
		UnitStillOwned:     u != nil && u.OrderID == o.ID && u.State == UnitReserved,
		SameUnitFree:       u != nil && u.State == UnitFree,
		UserHasOtherActive: active != "" && active != o.ID,
	}
	var other *Unit
	if m.cfg.Fungible && o.State == payment.OrderExpired && !c.SameUnitFree {
		other = m.anyFreeUnitLocked(o.Sale)
		c.OtherUnitFree = other != nil
	}
	action := payment.DecideSuccess(c)
	switch action {
	case payment.Confirm:
		m.sellLocked(o, u)
		a.Disposition = payment.Retained
	case payment.ReacquireSame:
		m.sellLocked(o, u)
		m.takes = append(m.takes, Take{Sale: o.Sale, Unit: u.ID})
		a.Disposition = payment.Retained
	case payment.ReacquireOther:
		o.Unit = other.ID
		m.sellLocked(o, other)
		m.takes = append(m.takes, Take{Sale: o.Sale, Unit: other.ID})
		a.Disposition = payment.Retained
	default:
		a.Disposition = payment.RefundPending
		m.refunds[a.ID] = struct{}{}
		if o.State == payment.OrderPending && !m.inFlightLocked(o.ID) {
			m.finishUnpaidLocked(o, payment.OrderCancelled) // cancelled while this charge was in flight
		}
		if o.State == payment.OrderExpired || o.State == payment.OrderCancelled {
			o.State = payment.OrderRefunding
		}
	}
	return action
}

func (m *Memory) sellLocked(o *Order, u *Unit) {
	u.State, u.OrderID = UnitSold, o.ID
	u.Version++
	o.State = payment.OrderConfirmed
	m.activeByUser[key(o.Sale, o.User)] = o.ID
}

// finishUnpaidLocked ends an order that will never be paid and frees its unit.
func (m *Memory) finishUnpaidLocked(o *Order, state payment.OrderState) {
	o.State = state
	if u := m.units[key(o.Sale, o.Unit)]; u != nil && u.OrderID == o.ID && u.State == UnitReserved {
		u.State, u.OrderID = UnitFree, ""
		u.Version++
		m.releases = append(m.releases, Release{Sale: o.Sale, Unit: u.ID, Version: u.Version})
	}
	if m.activeByUser[key(o.Sale, o.User)] == o.ID {
		delete(m.activeByUser, key(o.Sale, o.User))
	}
}

func (m *Memory) inFlightLocked(orderID string) bool {
	for _, id := range m.byOrder[orderID] {
		if payment.InFlight(m.attempts[id].Outcome) {
			return true
		}
	}
	return false
}

func (m *Memory) hasDispositionLocked(orderID string, d payment.Disposition) bool {
	for _, id := range m.byOrder[orderID] {
		if m.attempts[id].Disposition == d {
			return true
		}
	}
	return false
}

func (m *Memory) anyFreeUnitLocked(sale string) *Unit {
	for _, u := range m.units {
		if u.Sale == sale && u.State == UnitFree {
			return u
		}
	}
	return nil
}

// ExpireDue ends orders whose payment window closed. An order with an unresolved
// charge keeps its unit until the hard deadline, because money may have moved.
// It returns how many orders expired and how many were released past the deadline.
func (m *Memory) ExpireDue(now time.Time) (expired, pastDeadline int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, o := range m.orders {
		if o.State != payment.OrderPending || now.Before(o.PayBy) {
			continue
		}
		protected, late := false, false
		for _, id := range m.byOrder[o.ID] {
			a := m.attempts[id]
			switch {
			case a.Outcome == payment.NotSent:
				a.Outcome = payment.Cancelled // never sent, so safe to cancel
			case a.Outcome == payment.Unknown && now.Sub(a.FirstDispatchAt) < m.cfg.HardDeadline:
				protected = true
			case a.Outcome == payment.Unknown:
				late = true // released anyway: a later success is re-acquired or refunded
			}
		}
		if protected {
			continue
		}
		state := payment.OrderExpired
		if o.CancelRequested {
			state = payment.OrderCancelled
		}
		m.finishUnpaidLocked(o, state)
		expired++
		if late {
			pastDeadline++
		}
	}
	return expired, pastDeadline
}

// CancelOrder handles an explicit customer cancellation. With a charge in flight
// the order is only marked: the provider's answer decides between cancel and refund.
func (m *Memory) CancelOrder(orderID, user string) (Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[orderID]
	if !ok || o.User != user {
		return Order{}, ErrNotFound
	}
	if o.State != payment.OrderPending {
		return *o, ErrNotAllowed
	}
	o.CancelRequested = true
	for _, id := range m.byOrder[o.ID] {
		if a := m.attempts[id]; a.Outcome == payment.NotSent {
			a.Outcome = payment.Cancelled
		}
	}
	if !m.inFlightLocked(o.ID) {
		m.finishUnpaidLocked(o, payment.OrderCancelled)
	}
	return *o, nil
}

// RequestRefund starts a customer refund of a confirmed order.
func (m *Memory) RequestRefund(orderID, user string) (Order, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[orderID]
	if !ok || o.User != user {
		return Order{}, ErrNotFound
	}
	if o.State != payment.OrderConfirmed {
		return *o, ErrNotAllowed
	}
	for _, id := range m.byOrder[o.ID] {
		if a := m.attempts[id]; a.Disposition == payment.Retained {
			a.Disposition = payment.RefundPending
			m.refunds[a.ID] = struct{}{}
		}
	}
	o.State = payment.OrderRefunding
	return *o, nil
}

// RefundCompleted records a finished refund. A customer refund of a sold unit
// returns that unit to the pool only after the money is back.
func (m *Memory) RefundCompleted(attemptID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.attempts[attemptID]
	if !ok {
		return ErrNotFound
	}
	if a.Disposition != payment.RefundPending {
		return nil // duplicate completion
	}
	a.Disposition = payment.Refunded
	delete(m.refunds, a.ID)
	o := m.orders[a.OrderID]
	if o.State == payment.OrderRefunding && !m.hasDispositionLocked(o.ID, payment.RefundPending) {
		o.State = payment.OrderRefunded
		if u := m.units[key(o.Sale, o.Unit)]; u != nil && u.OrderID == o.ID && u.State == UnitSold {
			u.State, u.OrderID = UnitFree, ""
			u.Version++
			m.releases = append(m.releases, Release{Sale: o.Sale, Unit: u.ID, Version: u.Version})
		}
		if m.activeByUser[key(o.Sale, o.User)] == o.ID {
			delete(m.activeByUser, key(o.Sale, o.User))
		}
	}
	return nil
}

// UnknownAttempts returns attempts still UNKNOWN that were first sent before cutoff.
func (m *Memory) UnknownAttempts(cutoff time.Time) []Attempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Attempt
	for id := range m.unknown {
		if a := m.attempts[id]; !a.FirstDispatchAt.After(cutoff) {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// PendingRefunds returns attempts whose money must be returned.
func (m *Memory) PendingRefunds() []Attempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Attempt
	for id := range m.refunds {
		out = append(out, *m.attempts[id])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// DrainReleases returns and clears pending release events.
func (m *Memory) DrainReleases() []Release {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.releases
	m.releases = nil
	return out
}

// DrainTakes returns and clears pending take events.
func (m *Memory) DrainTakes() []Take {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.takes
	m.takes = nil
	return out
}

// UnitState returns a unit's durable state and version.
func (m *Memory) UnitState(sale, unit string) (UnitState, int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.units[key(sale, unit)]
	if !ok {
		return "", 0, false
	}
	return u.State, u.Version, true
}

// FreeUnits lists a sale's FREE units, used to rebuild Redis.
func (m *Memory) FreeUnits(sale string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, u := range m.units {
		if u.Sale == sale && u.State == UnitFree {
			out = append(out, u.ID)
		}
	}
	sort.Strings(out)
	return out
}

// Order returns one order.
func (m *Memory) Order(id string) (Order, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.orders[id]
	if !ok {
		return Order{}, false
	}
	return *o, true
}

// Attempts returns an order's attempts in creation order.
func (m *Memory) Attempts(orderID string) []Attempt {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Attempt
	for _, id := range m.byOrder[orderID] {
		out = append(out, *m.attempts[id])
	}
	return out
}

// Snapshot is a consistent copy of one sale's durable state, used by the detectors.
type Snapshot struct {
	Units    []Unit
	Orders   []Order
	Attempts []Attempt
}

// Snapshot copies one sale's state under the lock.
func (m *Memory) Snapshot(sale string) Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s Snapshot
	for _, u := range m.units {
		if u.Sale == sale {
			s.Units = append(s.Units, *u)
		}
	}
	for _, o := range m.orders {
		if o.Sale != sale {
			continue
		}
		s.Orders = append(s.Orders, *o)
		for _, id := range m.byOrder[o.ID] {
			s.Attempts = append(s.Attempts, *m.attempts[id])
		}
	}
	return s
}
