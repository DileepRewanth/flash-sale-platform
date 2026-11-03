// Package reconcile holds the independent detectors: oversell, duplicate charges,
// charges without an order, stuck payments and Redis inventory drift. They read
// state directly instead of trusting the code paths that wrote it, so they catch
// bugs that the write-path rules miss.
package reconcile

import (
	"context"
	"sort"
	"time"

	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/payment"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
)

// Report is one detector run. Every count except the unit totals should be zero.
type Report struct {
	TotalUnits          int      `json:"total_units"`
	SoldUnits           int      `json:"sold_units"`
	ReservedUnits       int      `json:"reserved_units"`
	FreeUnits           int      `json:"free_units"`
	ConfirmedOrders     int      `json:"confirmed_orders"`
	OversoldUnits       int      `json:"oversold_units"`
	DuplicateCharges    int      `json:"duplicate_charges"`
	ChargedWithoutOrder int      `json:"charged_without_order"`
	StuckPayments       int      `json:"stuck_payments"`
	DriftUnits          int      `json:"drift_units"`
	MissingFromRedis    []string `json:"missing_from_redis,omitempty"`
	StaleInRedis        []string `json:"stale_in_redis,omitempty"`
}

// Input is everything the detectors read.
type Input struct {
	Snapshot     store.Snapshot
	RedisFree    map[string]bool
	RedisHeld    map[string]bool
	NetCharges   map[string]int // provider ledger: successful charges minus refunds, per order
	Now          time.Time
	HardDeadline time.Duration
}

// Detect runs every detector over one consistent snapshot.
func Detect(in Input) Report {
	var r Report
	durable := map[string]store.UnitState{}
	for _, u := range in.Snapshot.Units {
		durable[u.ID] = u.State
		switch u.State {
		case store.UnitSold:
			r.SoldUnits++
		case store.UnitReserved:
			r.ReservedUnits++
		case store.UnitFree:
			r.FreeUnits++
		}
	}
	r.TotalUnits = len(durable)

	// Oversell: a unit with more than one confirmed order, or a confirmed order for a unit that does not exist.
	perUnit := map[string]int{}
	orderState := map[string]payment.OrderState{}
	for _, o := range in.Snapshot.Orders {
		orderState[o.ID] = o.State
		if o.State == payment.OrderConfirmed {
			perUnit[o.Unit]++
			r.ConfirmedOrders++
		}
	}
	for unit, n := range perUnit {
		if n > 1 {
			r.OversoldUnits += n - 1
		}
		if _, ok := durable[unit]; !ok {
			r.OversoldUnits += n
		}
	}

	// Money: compare what the provider ledger says with what the platform decided.
	pendingRefund := map[string]int{}
	for _, a := range in.Snapshot.Attempts {
		if a.Disposition == payment.RefundPending {
			pendingRefund[a.OrderID]++
		}
		if a.Outcome == payment.Unknown && !a.FirstDispatchAt.IsZero() && in.Now.Sub(a.FirstDispatchAt) > in.HardDeadline {
			r.StuckPayments++
		}
	}
	for orderID, net := range in.NetCharges {
		kept := net - pendingRefund[orderID]
		if kept > 1 {
			r.DuplicateCharges += kept - 1
		}
		if kept >= 1 && orderState[orderID] != payment.OrderConfirmed {
			r.ChargedWithoutOrder++
		}
	}

	// Drift: Redis must offer exactly the durable FREE units that nobody holds.
	for id, st := range durable {
		if st == store.UnitFree && !in.RedisFree[id] && !in.RedisHeld[id] {
			r.MissingFromRedis = append(r.MissingFromRedis, id)
		}
	}
	for id := range in.RedisFree {
		if durable[id] != store.UnitFree {
			r.StaleInRedis = append(r.StaleInRedis, id)
		}
	}
	sort.Strings(r.MissingFromRedis)
	sort.Strings(r.StaleInRedis)
	r.DriftUnits = len(r.MissingFromRedis) + len(r.StaleInRedis)
	return r
}

// DriftTracker reports only units that drifted in two consecutive scans, so the
// normal in-flight gap between a durable change and its Redis relay never
// triggers a repair.
type DriftTracker struct {
	prevMissing, prevStale map[string]bool
}

// Stable returns the drift seen in both this scan and the previous one.
func (t *DriftTracker) Stable(r Report) (missing, stale []string) {
	missing = intersect(r.MissingFromRedis, t.prevMissing)
	stale = intersect(r.StaleInRedis, t.prevStale)
	t.prevMissing, t.prevStale = toSet(r.MissingFromRedis), toSet(r.StaleInRedis)
	return missing, stale
}

func intersect(now []string, prev map[string]bool) []string {
	var out []string
	for _, id := range now {
		if prev[id] {
			out = append(out, id)
		}
	}
	return out
}

func toSet(ids []string) map[string]bool {
	s := make(map[string]bool, len(ids))
	for _, id := range ids {
		s[id] = true
	}
	return s
}

// Repair re-reads the durable state of each unit, then fixes Redis: missing FREE
// units are added back, and stale entries for units that are not FREE are removed.
func Repair(ctx context.Context, g *inventory.Gate, st *store.Memory, sale string, missing, stale []string) (added, removed int) {
	for _, id := range missing {
		if s, _, ok := st.UnitState(sale, id); ok && s == store.UnitFree && g.AddFree(ctx, sale, id) == nil {
			added++
		}
	}
	for _, id := range stale {
		if s, _, ok := st.UnitState(sale, id); (!ok || s != store.UnitFree) && g.Take(ctx, sale, id) == nil {
			removed++
		}
	}
	return added, removed
}
