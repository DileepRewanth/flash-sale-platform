// Package psp models the third-party payment provider.
//
// Fake is a deterministic stand-in with the failure modes that matter for
// correctness: declines, timeouts after and before a charge, explicit
// not-processed answers, charges that settle late, and idempotency keys that the
// provider forgets. Its ledger records every real money movement, so tests can
// prove "no double charge" directly. A real integration would implement Provider
// against a provider's API; that part is design only.
package psp

import (
	"context"
	"errors"
	"sync"
)

// Status is the provider's view of a charge.
type Status string

const (
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
	StatusPending   Status = "PENDING"
	StatusNotFound  Status = "NOT_FOUND"
)

var (
	ErrDeclined     = errors.New("psp: card declined")
	ErrTimeout      = errors.New("psp: timeout, outcome unknown")
	ErrNotProcessed = errors.New("psp: request was not processed")
)

// Meta travels with every charge, so orphan charges can be traced to an order.
type Meta struct {
	Sale    string
	Unit    string
	OrderID string
}

// Provider is the interface the payment worker uses.
type Provider interface {
	Charge(ctx context.Context, key string, meta Meta) error
	Status(ctx context.Context, key string) (Status, error)
	Cancel(ctx context.Context, key string) (Status, error)
	Refund(ctx context.Context, refundKey, chargeKey string) error
}

// Behavior scripts how the fake answers one charge request.
type Behavior int

const (
	Succeed             Behavior = iota
	Decline                      // explicit decline
	TimeoutAfterCharge           // the charge happens, the response is lost
	TimeoutBeforeCharge          // nothing happens, the response is lost
	NotProcessed                 // explicit rejection before processing, such as a rate limit
	SettleLater                  // accepted, decided later through Settle: a late success or failure
)

// LedgerEntry is one real money movement: +1 for a charge, -1 for a refund.
type LedgerEntry struct {
	Key     string
	OrderID string
	Amount  int
}

type record struct {
	status Status
	meta   Meta
}

// Fake is an in-process provider.
type Fake struct {
	mu       sync.Mutex
	byKey    map[string]*record // idempotency index, which the provider may prune
	byRef    map[string]*record // permanent record by our reference, used for lookups
	refunds  map[string]bool
	ledger   []LedgerEntry
	plan     map[string][]Behavior
	fallback Behavior
	charges  int
}

// NewFake returns a provider that succeeds unless scripted otherwise.
func NewFake() *Fake {
	return &Fake{byKey: map[string]*record{}, byRef: map[string]*record{}, refunds: map[string]bool{}, plan: map[string][]Behavior{}}
}

// Script queues behaviours for one idempotency key; they are consumed in order.
func (f *Fake) Script(key string, b ...Behavior) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plan[key] = append(f.plan[key], b...)
}

// SetDefault sets the behaviour for unscripted requests.
func (f *Fake) SetDefault(b Behavior) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fallback = b
}

// ForgetKeys simulates the provider pruning idempotency keys. Charge records stay
// visible to lookups, but a re-send with an old key would now create a new charge.
func (f *Fake) ForgetKeys() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byKey = map[string]*record{}
}

func (f *Fake) next(key string) Behavior {
	if q := f.plan[key]; len(q) > 0 {
		f.plan[key] = q[1:]
		return q[0]
	}
	return f.fallback
}

func (f *Fake) put(key string, s Status, meta Meta) {
	r := &record{status: s, meta: meta}
	f.byKey[key], f.byRef[key] = r, r
	if s == StatusSucceeded {
		f.ledger = append(f.ledger, LedgerEntry{Key: key, OrderID: meta.OrderID, Amount: 1})
	}
}

// Charge creates a charge. The same key returns the saved outcome and never a second charge.
func (f *Fake) Charge(_ context.Context, key string, meta Meta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.charges++
	if r, ok := f.byKey[key]; ok {
		switch r.status {
		case StatusSucceeded:
			return nil
		case StatusFailed, StatusCancelled:
			return ErrDeclined
		default:
			return ErrTimeout
		}
	}
	switch f.next(key) {
	case NotProcessed:
		return ErrNotProcessed
	case TimeoutBeforeCharge:
		return ErrTimeout
	case Decline:
		f.put(key, StatusFailed, meta)
		return ErrDeclined
	case SettleLater:
		f.put(key, StatusPending, meta)
		return ErrTimeout
	case TimeoutAfterCharge:
		f.put(key, StatusSucceeded, meta)
		return ErrTimeout
	default:
		f.put(key, StatusSucceeded, meta)
		return nil
	}
}

// Settle decides a SettleLater charge, simulating a late success or failure.
func (f *Fake) Settle(key string, success bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.byRef[key]
	if !ok || r.status != StatusPending {
		return
	}
	if success {
		r.status = StatusSucceeded
		f.ledger = append(f.ledger, LedgerEntry{Key: key, OrderID: r.meta.OrderID, Amount: 1})
		return
	}
	r.status = StatusFailed
}

// Status looks a charge up by our reference.
func (f *Fake) Status(_ context.Context, key string) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.byRef[key]; ok {
		return r.status, nil
	}
	return StatusNotFound, nil
}

// Cancel cancels by our reference. A cancelled reference can never be charged later.
func (f *Fake) Cancel(_ context.Context, key string) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.byRef[key]
	if !ok {
		f.put(key, StatusCancelled, Meta{})
		return StatusCancelled, nil
	}
	if r.status == StatusPending {
		r.status = StatusCancelled
	}
	return r.status, nil
}

// Refund returns the money for a successful charge. It is idempotent by refund key.
func (f *Fake) Refund(_ context.Context, refundKey, chargeKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refunds[refundKey] {
		return nil
	}
	r, ok := f.byRef[chargeKey]
	if !ok || r.status != StatusSucceeded {
		return errors.New("psp: nothing to refund")
	}
	f.refunds[refundKey] = true
	f.ledger = append(f.ledger, LedgerEntry{Key: chargeKey, OrderID: r.meta.OrderID, Amount: -1})
	return nil
}

// NetCharges returns successful charges minus refunds, per order.
func (f *Fake) NetCharges() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for _, e := range f.ledger {
		out[e.OrderID] += e.Amount
	}
	return out
}

// GrossCharges returns how many successful charges an order received in total.
func (f *Fake) GrossCharges(orderID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.ledger {
		if e.OrderID == orderID && e.Amount > 0 {
			n++
		}
	}
	return n
}

// ChargeCalls returns how many charge requests reached the provider.
func (f *Fake) ChargeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.charges
}
