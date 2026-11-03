// Package worker runs the background jobs: payment dispatch, reconciliation,
// expiry, refunds and the outbox relay back to Redis. Each job is a RunOnce
// function so tests can drive it step by step; Run loops them on a ticker.
package worker

import (
	"context"
	"errors"
	"time"

	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/payment"
	"github.com/DileepRewanth/flash-sale-platform/internal/psp"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
)

// Worker holds the dependencies shared by all background jobs.
type Worker struct {
	Store        *store.Memory
	Gate         *inventory.Gate
	PSP          psp.Provider
	Sale         string
	Now          func() time.Time
	ResendWindow time.Duration // same-key re-sends stop after this, far inside key retention
	LookupAfter  time.Duration // how long an UNKNOWN attempt waits before a status lookup
	SweepMargin  time.Duration // grace after hold expiry before the sweeper acts
	OnResult     func(store.Result)
}

// DispatchOnce sends one queued attempt to the provider. No database transaction
// is open during the call: the attempt was marked UNKNOWN beforehand.
func (w *Worker) DispatchOnce(ctx context.Context) bool {
	a, o, ok := w.Store.ClaimNextAttempt(w.Now())
	if !ok {
		return false
	}
	err := w.PSP.Charge(ctx, a.ID, psp.Meta{Sale: o.Sale, Unit: o.Unit, OrderID: o.ID})
	w.applyChargeErr(ctx, a.ID, err)
	return true
}

// applyChargeErr maps a provider response to a result. A timeout or unknown
// error changes nothing: the attempt stays UNKNOWN until a lookup resolves it.
func (w *Worker) applyChargeErr(ctx context.Context, attemptID string, err error) {
	switch {
	case err == nil:
		w.Apply(ctx, attemptID, store.ResultSucceeded)
	case errors.Is(err, psp.ErrDeclined):
		w.Apply(ctx, attemptID, store.ResultFailed)
	case errors.Is(err, psp.ErrNotProcessed):
		w.Apply(ctx, attemptID, store.ResultNotProcessed)
	}
}

// Apply records a result and relays any inventory changes to Redis.
func (w *Worker) Apply(ctx context.Context, attemptID string, r store.Result) {
	res, err := w.Store.ApplyResult(attemptID, r, w.Now())
	if err == nil && !res.Duplicate && w.OnResult != nil {
		w.OnResult(r)
	}
	w.RelayOnce(ctx)
}

// ReconcileOnce resolves UNKNOWN attempts using only what the provider says.
// "Not found" is never treated as failure. Inside the re-send window the same key
// is re-sent, which the provider deduplicates. After it, the attempt is cancelled
// by reference, so a delayed request cannot succeed later unnoticed.
func (w *Worker) ReconcileOnce(ctx context.Context) {
	now := w.Now()
	for _, a := range w.Store.UnknownAttempts(now.Add(-w.LookupAfter)) {
		st, err := w.PSP.Status(ctx, a.ID)
		if err != nil {
			continue
		}
		switch st {
		case psp.StatusSucceeded:
			w.Apply(ctx, a.ID, store.ResultSucceeded)
		case psp.StatusFailed:
			w.Apply(ctx, a.ID, store.ResultFailed)
		case psp.StatusCancelled:
			w.Apply(ctx, a.ID, store.ResultCancelled)
		case psp.StatusNotFound:
			if payment.ResendAllowed(a.FirstDispatchAt, now, w.ResendWindow) {
				o, _ := w.Store.Order(a.OrderID)
				w.Store.NoteResend(a.ID)
				err := w.PSP.Charge(ctx, a.ID, psp.Meta{Sale: o.Sale, Unit: o.Unit, OrderID: o.ID})
				w.applyChargeErr(ctx, a.ID, err)
				continue
			}
			if cst, err := w.PSP.Cancel(ctx, a.ID); err == nil {
				switch cst {
				case psp.StatusCancelled:
					w.Apply(ctx, a.ID, store.ResultCancelled)
				case psp.StatusSucceeded:
					w.Apply(ctx, a.ID, store.ResultSucceeded)
				case psp.StatusFailed:
					w.Apply(ctx, a.ID, store.ResultFailed)
				}
			}
		}
	}
}

// ExpireOnce expires unpaid orders, then sweeps expired unpaid holds in Redis.
func (w *Worker) ExpireOnce(ctx context.Context) {
	now := w.Now()
	w.Store.ExpireDue(now)
	_, _, _ = w.Gate.Sweep(ctx, w.Sale, now, w.SweepMargin, func(sale, unit string) bool {
		st, _, ok := w.Store.UnitState(sale, unit)
		return ok && st == store.UnitFree
	})
	w.RelayOnce(ctx)
}

// RefundOnce returns money for every charge the platform decided not to keep.
// The refund key is derived from the attempt, so a retried refund is never doubled.
func (w *Worker) RefundOnce(ctx context.Context) {
	for _, a := range w.Store.PendingRefunds() {
		if err := w.PSP.Refund(ctx, "refund-"+a.ID, a.ID); err == nil {
			_ = w.Store.RefundCompleted(a.ID)
		}
	}
	w.RelayOnce(ctx)
}

// RelayOnce is the outbox relay. A release is applied only if the unit is still
// FREE at the event's version, so a stale event cannot put a re-claimed unit back
// on sale. A lost relay leaves drift, which the drift detector finds and repairs.
func (w *Worker) RelayOnce(ctx context.Context) {
	for _, t := range w.Store.DrainTakes() {
		_ = w.Gate.Take(ctx, t.Sale, t.Unit)
	}
	for _, r := range w.Store.DrainReleases() {
		st, ver, ok := w.Store.UnitState(r.Sale, r.Unit)
		if ok && st == store.UnitFree && ver == r.Version {
			_ = w.Gate.AddFree(ctx, r.Sale, r.Unit)
		}
	}
}

// Run loops every job until ctx is cancelled. Dispatch runs every tick; the
// scanning jobs run every tenth tick so they do not compete with requests.
func (w *Worker) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for i := 0; ; i++ {
		for w.DispatchOnce(ctx) {
		}
		if i%10 == 0 {
			w.ReconcileOnce(ctx)
			w.ExpireOnce(ctx)
			w.RefundOnce(ctx)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
