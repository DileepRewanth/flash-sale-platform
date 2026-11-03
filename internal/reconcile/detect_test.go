package reconcile

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/payment"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
)

// These snapshots are corrupted on purpose: the detectors must catch states that
// the write path is designed never to produce.
func TestDetectsOversellDuplicateAndOrphanCharges(t *testing.T) {
	snap := store.Snapshot{
		Units: []store.Unit{{ID: "u1", State: store.UnitSold, OrderID: "o1"}, {ID: "u2", State: store.UnitFree}},
		Orders: []store.Order{
			{ID: "o1", Unit: "u1", State: payment.OrderConfirmed},
			{ID: "o2", Unit: "u1", State: payment.OrderConfirmed}, // the same unit sold twice
			{ID: "o3", Unit: "u2", State: payment.OrderExpired},   // charged but never confirmed
		},
	}
	r := Detect(Input{Snapshot: snap, RedisFree: map[string]bool{"u2": true}, NetCharges: map[string]int{"o1": 2, "o2": 1, "o3": 1}})
	if r.OversoldUnits != 1 || r.DuplicateCharges != 1 || r.ChargedWithoutOrder != 1 || r.DriftUnits != 0 {
		t.Fatalf("report %+v", r)
	}
}

func TestPendingRefundIsNotADuplicate(t *testing.T) {
	snap := store.Snapshot{
		Units:    []store.Unit{{ID: "u1", State: store.UnitSold, OrderID: "o1"}},
		Orders:   []store.Order{{ID: "o1", Unit: "u1", State: payment.OrderConfirmed}},
		Attempts: []store.Attempt{{ID: "a1", OrderID: "o1", Disposition: payment.Retained}, {ID: "a2", OrderID: "o1", Disposition: payment.RefundPending}},
	}
	if r := Detect(Input{Snapshot: snap, NetCharges: map[string]int{"o1": 2}}); r.DuplicateCharges != 0 {
		t.Fatalf("a charge already queued for refund is not a duplicate: %+v", r)
	}
}

func TestDetectsDriftBothWays(t *testing.T) {
	snap := store.Snapshot{Units: []store.Unit{
		{ID: "u1", State: store.UnitFree},     // missing from Redis: undersell
		{ID: "u2", State: store.UnitSold},     // offered by Redis: a stale positive
		{ID: "u3", State: store.UnitFree},     // held in Redis: fine
		{ID: "u4", State: store.UnitReserved}, // not offered: fine
	}}
	r := Detect(Input{Snapshot: snap, RedisFree: map[string]bool{"u2": true}, RedisHeld: map[string]bool{"u3": true}})
	if r.DriftUnits != 2 || len(r.MissingFromRedis) != 1 || r.MissingFromRedis[0] != "u1" || r.StaleInRedis[0] != "u2" {
		t.Fatalf("report %+v", r)
	}
}

func TestDriftTrackerNeedsTwoScans(t *testing.T) {
	var tr DriftTracker
	r := Report{MissingFromRedis: []string{"u1"}, StaleInRedis: []string{"u2"}}
	if m, s := tr.Stable(r); len(m)+len(s) != 0 {
		t.Fatal("drift seen once may be an in-flight relay and must not be repaired")
	}
	if m, s := tr.Stable(r); len(m) != 1 || len(s) != 1 {
		t.Fatalf("drift seen twice must be reported: %v %v", m, s)
	}
	if m, s := tr.Stable(Report{}); len(m)+len(s) != 0 {
		t.Fatal("resolved drift must clear")
	}
}

func TestRepairFixesRedisAfterRechecking(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	g := inventory.NewGate(rdb, 2)
	ctx := context.Background()
	st := store.NewMemory(store.Config{})
	st.SeedSale("s", []string{"u1", "u2"})
	_ = g.Seed(ctx, "s", []string{"u2"})
	if _, _, _, err := st.CreateOrder(store.CreateOrderRequest{Sale: "s", Unit: "u2", User: "alice", IdemKey: "k", HoldExpires: farFuture}, farPast); err != nil {
		t.Fatal(err)
	}
	added, removed := Repair(ctx, g, st, "s", []string{"u1"}, []string{"u2"})
	free, _, _ := g.State(ctx, "s")
	if added != 1 || removed != 1 || !free["u1"] || free["u2"] {
		t.Fatalf("added=%d removed=%d free=%v", added, removed, free)
	}
}
