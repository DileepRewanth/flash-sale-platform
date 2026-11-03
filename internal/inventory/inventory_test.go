package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newGate(t *testing.T, buckets int) (*Gate, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), PoolSize: 64})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewGate(rdb, buckets), mr
}

func unitIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("u%04d", i)
	}
	return out
}

var t0 = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)

func TestNoOversellUnderConcurrency(t *testing.T) {
	g, _ := newGate(t, 4)
	ctx := context.Background()
	if err := g.Seed(ctx, "s", unitIDs(50)); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	holders := map[string]string{}
	soldOut := 0
	var wg sync.WaitGroup
	for i := 0; i < 400; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, _, err := g.Reserve(ctx, "s", fmt.Sprintf("user%d", i), time.Minute, t0)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, ErrSoldOut):
				soldOut++
			case err != nil:
				t.Errorf("reserve: %v", err)
			default:
				if prev, dup := holders[h.Unit]; dup {
					t.Errorf("unit %s held twice, by %s and %s", h.Unit, prev, h.User)
				}
				holders[h.Unit] = h.User
			}
		}(i)
	}
	wg.Wait()
	if len(holders) != 50 || soldOut != 350 {
		t.Fatalf("got %d holds and %d sold out, want 50 and 350", len(holders), soldOut)
	}
}

func TestRepeatReserveReturnsSameHold(t *testing.T) {
	g, _ := newGate(t, 2)
	ctx := context.Background()
	_ = g.Seed(ctx, "s", unitIDs(5))
	first, replay, err := g.Reserve(ctx, "s", "alice", time.Minute, t0)
	if err != nil || replay {
		t.Fatalf("first reserve: %v replay=%v", err, replay)
	}
	second, replay, err := g.Reserve(ctx, "s", "alice", time.Minute, t0)
	if err != nil || !replay || second.ID != first.ID || second.Unit != first.Unit {
		t.Fatalf("double click took a second unit: %+v then %+v, err %v", first, second, err)
	}
}

func TestEmptyHomeBucketProbesOtherBuckets(t *testing.T) {
	g, _ := newGate(t, 8)
	ctx := context.Background()
	_ = g.Seed(ctx, "s", []string{"only-unit"})
	held := 0
	for i := 0; i < 20; i++ {
		_, _, err := g.Reserve(ctx, "s", fmt.Sprintf("user%d", i), time.Minute, t0)
		if err == nil {
			held++
		} else if !errors.Is(err, ErrSoldOut) {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if held != 1 {
		t.Fatalf("got %d holds for one unit across 8 buckets, want 1", held)
	}
}

func TestBrokenBucketIsNotReportedAsSoldOut(t *testing.T) {
	g, mr := newGate(t, 2)
	ctx := context.Background()
	// Make bucket 0 fail: a wrong-type key makes SPOP error, like an unreachable shard.
	if err := mr.Set(keys("s", 0)[0], "not-a-set"); err != nil {
		t.Fatal(err)
	}
	_, _, err := g.Reserve(ctx, "s", "alice", time.Minute, t0)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable rather than a false sold out", err)
	}
}

func TestSweepNeverReturnsDurablyClaimedUnit(t *testing.T) {
	g, _ := newGate(t, 1)
	ctx := context.Background()
	_ = g.Seed(ctx, "s", []string{"paid", "unpaid"})
	a, _, _ := g.Reserve(ctx, "s", "alice", time.Second, t0)
	b, _, _ := g.Reserve(ctx, "s", "bob", time.Second, t0)
	claimed := map[string]bool{a.Unit: true} // alice started payment; bob never did
	returned, forgotten, err := g.Sweep(ctx, "s", t0.Add(time.Hour), 10*time.Second, func(_, unit string) bool { return !claimed[unit] })
	if err != nil || returned != 1 || forgotten != 1 {
		t.Fatalf("sweep returned=%d forgotten=%d err=%v, want 1 and 1", returned, forgotten, err)
	}
	free, _, _ := g.State(ctx, "s")
	if free[a.Unit] || !free[b.Unit] {
		t.Fatalf("free set %v: the durably claimed unit must stay out, the unpaid one must return", free)
	}
}

func TestReleaseAndAddFreeAreIdempotent(t *testing.T) {
	g, _ := newGate(t, 1)
	ctx := context.Background()
	_ = g.Seed(ctx, "s", []string{"u1"})
	h, _, _ := g.Reserve(ctx, "s", "alice", time.Minute, t0)
	_ = g.Release(ctx, h)
	_ = g.Release(ctx, h)
	_ = g.AddFree(ctx, "s", "u1")
	free, _, _ := g.State(ctx, "s")
	if len(free) != 1 || !free["u1"] {
		t.Fatalf("free set %v, want exactly u1 once", free)
	}
}
