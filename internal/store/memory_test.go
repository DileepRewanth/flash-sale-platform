package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/DileepRewanth/flash-sale-platform/internal/payment"
)

var t0 = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func newStore(units ...string) *Memory {
	m := NewMemory(Config{PaymentGrace: 2 * time.Minute, HardDeadline: 30 * time.Minute, Fungible: true})
	m.SeedSale("s", units)
	return m
}

func req(unit, user, key, hash string) CreateOrderRequest {
	return CreateOrderRequest{Sale: "s", Unit: unit, User: user, HoldID: "h-" + user, IdemKey: key, RequestHash: hash, HoldExpires: t0.Add(5 * time.Minute)}
}

func TestOnlyOneOrderWinsAUnit(t *testing.T) {
	m := newStore("u1")
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, taken := 0, 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, _, err := m.CreateOrder(req("u1", fmt.Sprintf("user%d", i), "k", "h"), t0)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrUnitTaken):
				taken++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 || taken != 99 {
		t.Fatalf("wins=%d taken=%d, want 1 and 99", wins, taken)
	}
}

func TestCreateOrderIsIdempotent(t *testing.T) {
	m := newStore("u1")
	o1, a1, replay, err := m.CreateOrder(req("u1", "alice", "k1", "hash-a"), t0)
	if err != nil || replay {
		t.Fatalf("first create: %v replay=%v", err, replay)
	}
	o2, a2, replay, err := m.CreateOrder(req("u1", "alice", "k1", "hash-a"), t0)
	if err != nil || !replay || o2.ID != o1.ID || a2.ID != a1.ID {
		t.Fatalf("same key and body must replay the same order: %v replay=%v", err, replay)
	}
	if _, _, _, err := m.CreateOrder(req("u1", "alice", "k1", "hash-b"), t0); !errors.Is(err, ErrIdempotencyReuse) {
		t.Fatalf("same key with a different body: got %v, want ErrIdempotencyReuse", err)
	}
}

func TestOneActiveOrderPerUser(t *testing.T) {
	m := newStore("u1", "u2")
	if _, _, _, err := m.CreateOrder(req("u1", "alice", "k1", "h"), t0); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := m.CreateOrder(req("u2", "alice", "k2", "h"), t0); !errors.Is(err, ErrUserHasOrder) {
		t.Fatalf("second active order: got %v, want ErrUserHasOrder", err)
	}
}

func TestExpiredHoldCannotClaim(t *testing.T) {
	m := newStore("u1")
	if _, _, _, err := m.CreateOrder(req("u1", "alice", "k1", "h"), t0.Add(5*time.Minute)); !errors.Is(err, ErrHoldExpired) {
		t.Fatalf("got %v, want ErrHoldExpired: the store's clock decides expiry", err)
	}
}

func TestExpiryReleasesUnitThroughOutbox(t *testing.T) {
	m := newStore("u1")
	o, _, _, _ := m.CreateOrder(req("u1", "alice", "k1", "h"), t0)
	if n, _ := m.ExpireDue(o.PayBy); n != 1 {
		t.Fatalf("expired %d orders, want 1", n)
	}
	got, _ := m.Order(o.ID)
	st, ver, _ := m.UnitState("s", "u1")
	rel := m.DrainReleases()
	if got.State != payment.OrderExpired || st != UnitFree || len(rel) != 1 || rel[0].Version != ver {
		t.Fatalf("order %s unit %s releases %+v", got.State, st, rel)
	}
}

func TestUnresolvedPaymentProtectsUnitUntilHardDeadline(t *testing.T) {
	m := newStore("u1")
	o, _, _, _ := m.CreateOrder(req("u1", "alice", "k1", "h"), t0)
	if _, _, ok := m.ClaimNextAttempt(t0); !ok {
		t.Fatal("expected a queued attempt")
	}
	m.ExpireDue(o.PayBy.Add(time.Minute))
	if got, _ := m.Order(o.ID); got.State != payment.OrderPending {
		t.Fatalf("order %s: an unresolved charge must keep its unit", got.State)
	}
	_, late := m.ExpireDue(t0.Add(31 * time.Minute))
	if got, _ := m.Order(o.ID); got.State != payment.OrderExpired || late != 1 {
		t.Fatalf("order %s late=%d: past the hard deadline the unit is released", got.State, late)
	}
}
