package worker_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/payment"
	"github.com/DileepRewanth/flash-sale-platform/internal/psp"
	"github.com/DileepRewanth/flash-sale-platform/internal/reconcile"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
	"github.com/DileepRewanth/flash-sale-platform/internal/worker"
)

// env wires the real gate, store, fake provider and worker with a controllable clock.
type env struct {
	t    *testing.T
	ctx  context.Context
	mr   *miniredis.Miniredis
	st   *store.Memory
	gate *inventory.Gate
	fake *psp.Fake
	w    *worker.Worker
	now  time.Time
	mu   sync.Mutex
}

func newEnv(t *testing.T, units ...string) *env {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), PoolSize: 32})
	t.Cleanup(func() { _ = rdb.Close() })
	e := &env{
		t: t, ctx: context.Background(), mr: mr,
		st:   store.NewMemory(store.Config{PaymentGrace: 2 * time.Minute, HardDeadline: 30 * time.Minute, Fungible: true}),
		gate: inventory.NewGate(rdb, 2), fake: psp.NewFake(),
		now: time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC),
	}
	e.st.SeedSale("s", units)
	if err := e.gate.Seed(e.ctx, "s", units); err != nil {
		t.Fatal(err)
	}
	e.w = &worker.Worker{Store: e.st, Gate: e.gate, PSP: e.fake, Sale: "s", Now: e.clock,
		ResendWindow: 5 * time.Minute, LookupAfter: 0, SweepMargin: 10 * time.Second}
	return e
}

func (e *env) clock() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.now
}

func (e *env) advance(d time.Duration) {
	e.mu.Lock()
	e.now = e.now.Add(d)
	e.mu.Unlock()
}

// buy runs steps 1 and 2: a Redis hold, then the durable claim.
func (e *env) buy(user string) (store.Order, store.Attempt) {
	e.t.Helper()
	h, _, err := e.gate.Reserve(e.ctx, "s", user, 5*time.Minute, e.clock())
	if err != nil {
		e.t.Fatalf("reserve %s: %v", user, err)
	}
	o, a, _, err := e.st.CreateOrder(store.CreateOrderRequest{
		Sale: "s", Unit: h.Unit, User: user, HoldID: h.ID, IdemKey: user + "-k1", RequestHash: "h", HoldExpires: h.Expires,
	}, e.clock())
	if err != nil {
		e.t.Fatalf("create order %s: %v", user, err)
	}
	_ = e.gate.Forget(e.ctx, h)
	return o, a
}

func (e *env) order(id string) store.Order {
	o, _ := e.st.Order(id)
	return o
}

func (e *env) audit() reconcile.Report {
	free, held, _ := e.gate.State(e.ctx, "s")
	return reconcile.Detect(reconcile.Input{Snapshot: e.st.Snapshot("s"), RedisFree: free, RedisHeld: held,
		NetCharges: e.fake.NetCharges(), Now: e.clock(), HardDeadline: 30 * time.Minute})
}

func (e *env) assertClean() {
	e.t.Helper()
	r := e.audit()
	if r.OversoldUnits != 0 || r.DuplicateCharges != 0 || r.ChargedWithoutOrder != 0 {
		e.t.Fatalf("detectors not clean: %+v", r)
	}
}

func TestHappyPathChargesOnce(t *testing.T) {
	e := newEnv(t, "u1")
	o, _ := e.buy("alice")
	e.w.DispatchOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderConfirmed {
		t.Fatalf("order %s, want CONFIRMED", got.State)
	}
	if n := e.fake.NetCharges()[o.ID]; n != 1 {
		t.Fatalf("charged %d times, want 1", n)
	}
	e.assertClean()
}

func TestTimeoutAfterChargeIsResolvedByLookupNotResend(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.TimeoutAfterCharge)
	e.w.DispatchOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderPending {
		t.Fatalf("a timeout must leave the order pending, got %s", got.State)
	}
	e.w.ReconcileOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderConfirmed {
		t.Fatalf("lookup should confirm, got %s", got.State)
	}
	if e.fake.ChargeCalls() != 1 || e.fake.GrossCharges(o.ID) != 1 {
		t.Fatalf("calls=%d gross=%d: no re-send when the lookup already knows the answer", e.fake.ChargeCalls(), e.fake.GrossCharges(o.ID))
	}
}

func TestNotFoundIsResentInsideWindow(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.TimeoutBeforeCharge)
	e.w.DispatchOnce(e.ctx)
	e.w.ReconcileOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderConfirmed || e.fake.GrossCharges(o.ID) != 1 {
		t.Fatalf("order %s gross=%d, want CONFIRMED with one charge", got.State, e.fake.GrossCharges(o.ID))
	}
}

func TestNoResendAfterWindowEvenWhenKeysAreForgotten(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.TimeoutBeforeCharge)
	e.w.DispatchOnce(e.ctx)
	e.advance(6 * time.Minute)
	e.fake.ForgetKeys() // the provider pruned keys: a re-send now would be a brand new charge
	e.w.ReconcileOnce(e.ctx)
	atts := e.st.Attempts(o.ID)
	if atts[0].Outcome != payment.Cancelled || e.fake.GrossCharges(o.ID) != 0 {
		t.Fatalf("outcome %s gross=%d: after the window the attempt is cancelled, never re-sent", atts[0].Outcome, e.fake.GrossCharges(o.ID))
	}
}

func TestForgottenKeysStillResolveByLookup(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.TimeoutAfterCharge)
	e.w.DispatchOnce(e.ctx)
	e.advance(6 * time.Minute)
	e.fake.ForgetKeys()
	e.w.ReconcileOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderConfirmed || e.fake.GrossCharges(o.ID) != 1 {
		t.Fatalf("order %s gross=%d, want CONFIRMED with exactly one charge", got.State, e.fake.GrossCharges(o.ID))
	}
}

func TestNotProcessedGoesBackToQueue(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.NotProcessed, psp.Succeed)
	e.w.DispatchOnce(e.ctx)
	if got := e.st.Attempts(o.ID)[0].Outcome; got != payment.NotSent {
		t.Fatalf("outcome %s, want NOT_SENT after an explicit not-processed answer", got)
	}
	e.w.DispatchOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderConfirmed || e.fake.GrossCharges(o.ID) != 1 {
		t.Fatalf("order %s gross=%d", got.State, e.fake.GrossCharges(o.ID))
	}
}

func TestLateSuccessAfterExpiryReacquiresSameUnit(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.SettleLater)
	e.w.DispatchOnce(e.ctx)
	e.advance(31 * time.Minute) // past the hard deadline
	e.w.ExpireOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderExpired {
		t.Fatalf("order %s, want EXPIRED after the hard deadline", got.State)
	}
	e.fake.Settle(a.ID, true) // the provider confirms the charge late
	e.w.ReconcileOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderConfirmed {
		t.Fatalf("order %s, want CONFIRMED by re-acquiring the free unit", got.State)
	}
	if free, _, _ := e.gate.State(e.ctx, "s"); free[o.Unit] {
		t.Fatal("a re-acquired unit must leave the Redis free set")
	}
	e.assertClean()
}

func TestLateSuccessAfterUnitResoldIsRefunded(t *testing.T) {
	e := newEnv(t, "u1")
	alice, a := e.buy("alice")
	e.fake.Script(a.ID, psp.SettleLater)
	e.w.DispatchOnce(e.ctx)
	e.advance(31 * time.Minute)
	e.w.ExpireOnce(e.ctx)
	bob, _ := e.buy("bob") // the only unit is resold
	e.w.DispatchOnce(e.ctx)
	e.fake.Settle(a.ID, true)
	e.w.ReconcileOnce(e.ctx)
	if got := e.order(alice.ID); got.State != payment.OrderRefunding {
		t.Fatalf("alice %s, want REFUNDING: her unit belongs to bob now", got.State)
	}
	e.w.RefundOnce(e.ctx)
	if got := e.order(alice.ID); got.State != payment.OrderRefunded || e.fake.NetCharges()[alice.ID] != 0 {
		t.Fatalf("alice %s net=%d", got.State, e.fake.NetCharges()[alice.ID])
	}
	if got := e.order(bob.ID); got.State != payment.OrderConfirmed {
		t.Fatalf("bob %s, want CONFIRMED", got.State)
	}
	e.assertClean()
}

// TestCompetingSuccessesKeepOneCharge races a late success on attempt A against a
// success on retry attempt B, many times, under the race detector.
func TestCompetingSuccessesKeepOneCharge(t *testing.T) {
	for i := 0; i < 50; i++ {
		e := newEnv(t, "u1")
		o, a := e.buy("alice")
		e.fake.Script(a.ID, psp.SettleLater)
		e.w.DispatchOnce(e.ctx)
		_, _ = e.st.ApplyResult(a.ID, store.ResultFailed, e.clock()) // an early failure notice for A
		if _, _, err := e.st.RetryPayment(o.ID, "alice", "alice-k2", "h2", e.clock().Add(5*time.Minute), e.clock()); err != nil {
			t.Fatalf("retry: %v", err)
		}
		e.fake.Settle(a.ID, true) // ...but A actually succeeded at the provider
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); e.w.Apply(e.ctx, a.ID, store.ResultSucceeded) }() // A's late success webhook
		go func() { defer wg.Done(); e.w.DispatchOnce(e.ctx) }()                       // B is sent and succeeds
		wg.Wait()
		e.w.RefundOnce(e.ctx)
		retained := 0
		for _, at := range e.st.Attempts(o.ID) {
			if at.Disposition == payment.Retained {
				retained++
			}
		}
		if got := e.order(o.ID); got.State != payment.OrderConfirmed || retained != 1 || e.fake.NetCharges()[o.ID] != 1 {
			t.Fatalf("run %d: order %s retained=%d net=%d", i, got.State, retained, e.fake.NetCharges()[o.ID])
		}
		e.assertClean()
	}
}

func TestCancelDuringInFlightPayment(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprintf("provider_success_%v", success), func(t *testing.T) {
			e := newEnv(t, "u1")
			o, a := e.buy("alice")
			e.fake.Script(a.ID, psp.SettleLater)
			e.w.DispatchOnce(e.ctx)
			if _, err := e.st.CancelOrder(o.ID, "alice"); err != nil {
				t.Fatal(err)
			}
			if got := e.order(o.ID); got.State != payment.OrderPending {
				t.Fatalf("order %s: with a charge in flight, cancel waits for the provider", got.State)
			}
			e.fake.Settle(a.ID, success)
			e.w.ReconcileOnce(e.ctx)
			e.w.RefundOnce(e.ctx)
			want := payment.OrderCancelled
			if success {
				want = payment.OrderRefunded
			}
			if got := e.order(o.ID); got.State != want || e.fake.NetCharges()[o.ID] != 0 {
				t.Fatalf("order %s net=%d, want %s with no kept charge", got.State, e.fake.NetCharges()[o.ID], want)
			}
			if free, _, _ := e.gate.State(e.ctx, "s"); !free["u1"] {
				t.Fatal("the cancelled unit must return to Redis")
			}
		})
	}
}

func TestQueuedAttemptPastDeadlineIsNeverCharged(t *testing.T) {
	e := newEnv(t, "u1")
	o, _ := e.buy("alice")
	e.advance(8 * time.Minute) // past the 5 minute hold plus 2 minute grace
	e.w.DispatchOnce(e.ctx)
	e.w.RelayOnce(e.ctx)
	if got := e.order(o.ID); got.State != payment.OrderExpired || e.fake.ChargeCalls() != 0 {
		t.Fatalf("order %s calls=%d: a stale queued attempt must be cancelled unsent", got.State, e.fake.ChargeCalls())
	}
	if free, _, _ := e.gate.State(e.ctx, "s"); !free["u1"] {
		t.Fatal("the unit must return to Redis")
	}
}

func TestDuplicateWebhookAppliesOnce(t *testing.T) {
	e := newEnv(t, "u1")
	o, a := e.buy("alice")
	e.fake.Script(a.ID, psp.TimeoutAfterCharge)
	e.w.DispatchOnce(e.ctx)
	first, err := e.st.ApplyWebhook("evt-1", a.ID, store.ResultSucceeded, e.clock())
	second, _ := e.st.ApplyWebhook("evt-1", a.ID, store.ResultSucceeded, e.clock())
	if err != nil || first.Duplicate || !second.Duplicate {
		t.Fatalf("first=%+v second=%+v err=%v", first.Duplicate, second.Duplicate, err)
	}
	if got := e.order(o.ID); got.State != payment.OrderConfirmed || e.fake.GrossCharges(o.ID) != 1 {
		t.Fatalf("order %s gross=%d", got.State, e.fake.GrossCharges(o.ID))
	}
}

func TestRedisWipeAndRebuildCausesNoOversell(t *testing.T) {
	units := []string{"u0", "u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8", "u9"}
	e := newEnv(t, units...)
	for i := 0; i < 5; i++ {
		e.buy(fmt.Sprintf("early%d", i))
	}
	for e.w.DispatchOnce(e.ctx) {
	}
	e.mr.FlushAll() // Redis loses everything mid-sale
	if err := e.gate.Rebuild(e.ctx, "s", e.st.FreeUnits("s")); err != nil {
		t.Fatal(err)
	}
	bought := 0
	for i := 0; i < 20; i++ {
		h, _, err := e.gate.Reserve(e.ctx, "s", fmt.Sprintf("late%d", i), 5*time.Minute, e.clock())
		if err != nil {
			continue
		}
		if _, _, _, err := e.st.CreateOrder(store.CreateOrderRequest{Sale: "s", Unit: h.Unit, User: h.User, HoldID: h.ID,
			IdemKey: h.User, RequestHash: "h", HoldExpires: h.Expires}, e.clock()); err == nil {
			bought++
		}
	}
	for e.w.DispatchOnce(e.ctx) {
	}
	r := e.audit()
	if bought != 5 || r.SoldUnits != 10 || r.OversoldUnits != 0 {
		t.Fatalf("bought=%d report=%+v: rebuild must offer only the 5 durable FREE units", bought, r)
	}
}
