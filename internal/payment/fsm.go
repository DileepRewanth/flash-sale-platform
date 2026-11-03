// Package payment holds the payment state machine.
//
// It is deliberately pure: no I/O, no clocks, no storage. Every caller that
// learns something about a payment (the dispatch worker, the webhook handler,
// the reconciler and the expiry job) funnels that fact through these rules, so
// racing or duplicate events always settle on the same outcome.
package payment

import "time"

// Outcome records only what the payment provider has said about an attempt.
type Outcome string

const (
	NotSent   Outcome = "NOT_SENT"  // no charge request has left the system
	Unknown   Outcome = "UNKNOWN"   // a request was sent: money may or may not have moved
	Succeeded Outcome = "SUCCEEDED" // the provider confirmed the charge
	Failed    Outcome = "FAILED"    // the provider explicitly declined
	Cancelled Outcome = "CANCELLED" // the provider confirmed cancellation, or it was never sent
)

// Disposition records what the platform decided to do with the money.
// Keeping it separate from Outcome makes "money moved" and "we keep the money"
// two different facts, which is what makes duplicate charges easy to handle.
type Disposition string

const (
	DispositionNone Disposition = "NONE"
	Retained        Disposition = "RETAINED"
	RefundPending   Disposition = "REFUND_PENDING"
	Refunded        Disposition = "REFUNDED"
)

// OrderState is the customer-visible state of an order.
type OrderState string

const (
	OrderPending   OrderState = "PENDING"
	OrderConfirmed OrderState = "CONFIRMED"
	OrderCancelled OrderState = "CANCELLED"
	OrderExpired   OrderState = "EXPIRED"
	OrderRefunding OrderState = "REFUNDING"
	OrderRefunded  OrderState = "REFUNDED"
)

var outcomeEdges = map[Outcome][]Outcome{
	NotSent:   {Unknown, Cancelled},
	Unknown:   {Succeeded, Failed, Cancelled, NotSent},
	Failed:    {Succeeded},
	Cancelled: {Succeeded},
}

var dispositionEdges = map[Disposition][]Disposition{
	DispositionNone: {Retained, RefundPending},
	Retained:        {RefundPending},
	RefundPending:   {Refunded},
}

var orderEdges = map[OrderState][]OrderState{
	OrderPending:   {OrderConfirmed, OrderCancelled, OrderExpired},
	OrderExpired:   {OrderConfirmed, OrderRefunding},
	OrderCancelled: {OrderRefunding},
	OrderConfirmed: {OrderRefunding},
	OrderRefunding: {OrderRefunded},
}

func allowed[T comparable](edges map[T][]T, from, to T) bool {
	for _, next := range edges[from] {
		if next == to {
			return true
		}
	}
	return false
}

// CanMoveOutcome reports whether the provider outcome may change from one value to another.
func CanMoveOutcome(from, to Outcome) bool { return allowed(outcomeEdges, from, to) }

// CanMoveDisposition reports whether the money decision may change.
func CanMoveDisposition(from, to Disposition) bool { return allowed(dispositionEdges, from, to) }

// CanMoveOrder reports whether the order state may change.
func CanMoveOrder(from, to OrderState) bool { return allowed(orderEdges, from, to) }

// InFlight reports whether an attempt may still move money.
func InFlight(o Outcome) bool { return o == NotSent || o == Unknown }

// NotProcessedResets reports whether an explicit "not processed" answer may move
// an attempt from UNKNOWN back to NOT_SENT. It is only safe when the attempt has
// been dispatched exactly once: a not-processed reply to a re-send proves nothing
// about the first send.
func NotProcessedResets(sends int) bool { return sends == 1 }

// ResendAllowed reports whether a same-key re-send is still safe. Providers prune
// idempotency keys (Stripe after at least 24 hours), so re-sends stop after a
// window far inside that retention. After it, only lookups and cancellation remain.
func ResendAllowed(firstDispatch, now time.Time, window time.Duration) bool {
	return !firstDispatch.IsZero() && now.Sub(firstDispatch) < window
}

// SuccessContext describes the world at the moment the provider reports a charge.
type SuccessContext struct {
	Order              OrderState
	CancelRequested    bool // the customer explicitly asked to cancel
	OrderHasRetained   bool // another attempt's charge is already kept for this order
	UnitStillOwned     bool // the order's unit is still RESERVED to this order
	SameUnitFree       bool // the order's original unit is FREE again
	OtherUnitFree      bool // some other unit is FREE and stock is interchangeable
	UserHasOtherActive bool // the user has since placed another active order in the sale
}

// SuccessAction is what the platform does with a successful charge.
type SuccessAction string

const (
	Confirm        SuccessAction = "CONFIRM"
	ReacquireSame  SuccessAction = "REACQUIRE_SAME_UNIT"
	ReacquireOther SuccessAction = "REACQUIRE_OTHER_UNIT"
	Refund         SuccessAction = "REFUND"
)

// DecideSuccess is the late-success policy. A charged customer outranks an unpaid
// holder, but never at the cost of a second owner, a second kept charge, the
// one-unit limit, or an explicit cancellation.
func DecideSuccess(c SuccessContext) SuccessAction {
	switch {
	case c.OrderHasRetained:
		return Refund // a second charge on one order is never kept
	case c.CancelRequested || c.Order == OrderCancelled:
		return Refund // an explicit cancellation always ends in a refund
	case c.Order == OrderPending && c.UnitStillOwned:
		return Confirm
	case c.Order == OrderExpired && !c.UserHasOtherActive && c.SameUnitFree:
		return ReacquireSame
	case c.Order == OrderExpired && !c.UserHasOtherActive && c.OtherUnitFree:
		return ReacquireOther
	default:
		return Refund
	}
}
