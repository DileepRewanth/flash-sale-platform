package payment

import (
	"testing"
	"time"
)

func TestDecideSuccess(t *testing.T) {
	cases := []struct {
		name string
		c    SuccessContext
		want SuccessAction
	}{
		{"pending order that still owns its unit is confirmed", SuccessContext{Order: OrderPending, UnitStillOwned: true}, Confirm},
		{"second charge on one order is refunded", SuccessContext{Order: OrderConfirmed, OrderHasRetained: true}, Refund},
		{"explicit cancellation in flight always refunds", SuccessContext{Order: OrderPending, UnitStillOwned: true, CancelRequested: true}, Refund},
		{"cancelled order refunds even when its unit is free", SuccessContext{Order: OrderCancelled, SameUnitFree: true}, Refund},
		{"expired order re-acquires its own unit", SuccessContext{Order: OrderExpired, SameUnitFree: true}, ReacquireSame},
		{"expired order takes another interchangeable unit", SuccessContext{Order: OrderExpired, OtherUnitFree: true}, ReacquireOther},
		{"expired order with no stock left refunds", SuccessContext{Order: OrderExpired}, Refund},
		{"one-unit limit beats re-acquisition", SuccessContext{Order: OrderExpired, SameUnitFree: true, UserHasOtherActive: true}, Refund},
		{"refunded order refunds a new charge", SuccessContext{Order: OrderRefunded}, Refund},
		{"pending order without its unit refunds defensively", SuccessContext{Order: OrderPending}, Refund},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecideSuccess(tc.c); got != tc.want {
				t.Fatalf("DecideSuccess(%+v) = %s, want %s", tc.c, got, tc.want)
			}
		})
	}
}

func TestTransitions(t *testing.T) {
	checks := []struct {
		name string
		ok   bool
		want bool
	}{
		{"unknown may return to not sent", CanMoveOutcome(Unknown, NotSent), true},
		{"a late success may follow a failure", CanMoveOutcome(Failed, Succeeded), true},
		{"a success is never undone by a failure", CanMoveOutcome(Succeeded, Failed), false},
		{"not sent cannot jump to succeeded", CanMoveOutcome(NotSent, Succeeded), false},
		{"expired order may be confirmed after re-acquisition", CanMoveOrder(OrderExpired, OrderConfirmed), true},
		{"cancelled order is never confirmed", CanMoveOrder(OrderCancelled, OrderConfirmed), false},
		{"refunded money is never retained again", CanMoveDisposition(Refunded, Retained), false},
		{"a retained charge may be refunded", CanMoveDisposition(Retained, RefundPending), true},
	}
	for _, c := range checks {
		if c.ok != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.ok, c.want)
		}
	}
}

func TestNotProcessedResetsOnlyAfterOneDispatch(t *testing.T) {
	if !NotProcessedResets(1) {
		t.Fatal("a single dispatch answered as not processed must reset to NOT_SENT")
	}
	if NotProcessedResets(2) {
		t.Fatal("a not-processed reply to a re-send proves nothing about the first send")
	}
}

func TestResendWindow(t *testing.T) {
	first := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	window := 5 * time.Minute
	if ResendAllowed(time.Time{}, first, window) {
		t.Fatal("an attempt never dispatched has nothing to re-send")
	}
	if !ResendAllowed(first, first.Add(4*time.Minute), window) {
		t.Fatal("re-send must be allowed inside the window")
	}
	if ResendAllowed(first, first.Add(window), window) {
		t.Fatal("re-send must stop at the end of the window")
	}
}
