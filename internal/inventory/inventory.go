// Package inventory is the fast reservation gate. It keeps free unit IDs in
// Redis sets split into buckets, and takes holds with one atomic Lua script per
// bucket.
//
// Redis decides who may try to buy. It never decides who owns a unit: that is
// the durable store's job. Losing Redis data can therefore reject a hold, but it
// cannot double-sell a unit.
package inventory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrSoldOut     = errors.New("sold out")
	ErrInProgress  = errors.New("a reservation for this user is already in progress")
	ErrUnavailable = errors.New("inventory temporarily unavailable, retry")
)

// reserveScript pops one free unit and records the hold atomically.
// KEYS: free set, holds hash, expiry sorted set. ARGV: hold id, user, expiry in unix ms.
var reserveScript = redis.NewScript(`
local unit = redis.call('SPOP', KEYS[1])
if not unit then return false end
redis.call('HSET', KEYS[2], ARGV[1], unit .. '|' .. ARGV[2] .. '|' .. ARGV[3])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return unit
`)

// releaseScript removes a hold. With ARGV[2] == "1" it also returns the unit to
// the free set, but only while this hold still exists, so a duplicate or stale
// release cannot add a unit twice.
var releaseScript = redis.NewScript(`
local v = redis.call('HGET', KEYS[2], ARGV[1])
if not v then return 0 end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if ARGV[2] == '1' then
  local unit = string.match(v, '^([^|]+)')
  redis.call('SADD', KEYS[1], unit)
end
return 1
`)

// Gate is the Redis reservation gate for one or more sales.
type Gate struct {
	rdb      redis.UniversalClient
	buckets  int
	claimTTL time.Duration
}

// NewGate returns a gate that spreads each sale over the given number of buckets.
func NewGate(rdb redis.UniversalClient, buckets int) *Gate {
	if buckets < 1 {
		buckets = 1
	}
	return &Gate{rdb: rdb, buckets: buckets, claimTTL: 10 * time.Second}
}

// Buckets returns the bucket count.
func (g *Gate) Buckets() int { return g.buckets }

// BucketOf maps a unit or user to a bucket. It is deterministic, so the durable
// store never has to remember where a released unit belongs.
func BucketOf(id string, buckets int) int {
	h := fnv.New32a()
	h.Write([]byte(id))
	return int(h.Sum32() % uint32(buckets))
}

// keys returns the free set, holds hash and expiry set for one bucket. The hash
// tag keeps all three in one Redis Cluster slot, so the Lua script stays legal.
func keys(sale string, b int) []string {
	tag := fmt.Sprintf("{%s:%d}", sale, b)
	return []string{"inv:" + tag + ":free", "inv:" + tag + ":holds", "inv:" + tag + ":exp"}
}

func claimKey(sale, user string) string { return "claim:{" + sale + "}:" + user }

// Hold is a provisional reservation. Only Redis knows about it until payment starts.
type Hold struct {
	ID      string    `json:"id"`
	Sale    string    `json:"sale"`
	Unit    string    `json:"unit"`
	User    string    `json:"user"`
	Bucket  int       `json:"bucket"`
	Expires time.Time `json:"expires"`
}

// Seed adds units to their home buckets.
func (g *Gate) Seed(ctx context.Context, sale string, units []string) error {
	byBucket := make(map[int][]any)
	for _, u := range units {
		b := BucketOf(u, g.buckets)
		byBucket[b] = append(byBucket[b], u)
	}
	for b, us := range byBucket {
		if err := g.rdb.SAdd(ctx, keys(sale, b)[0], us...).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Reserve takes a hold for user. A repeat call by the same user returns the same
// hold, so a double click never takes two units. The boolean reports a replay.
func (g *Gate) Reserve(ctx context.Context, sale, user string, ttl time.Duration, now time.Time) (Hold, bool, error) {
	ck := claimKey(sale, user)
	ok, err := g.rdb.SetNX(ctx, ck, "pending", g.claimTTL).Result()
	if err != nil {
		return Hold{}, false, ErrUnavailable
	}
	if !ok {
		v, err := g.rdb.Get(ctx, ck).Result()
		if err != nil || v == "pending" {
			return Hold{}, false, ErrInProgress
		}
		var h Hold
		if json.Unmarshal([]byte(v), &h) != nil || !now.Before(h.Expires) {
			return Hold{}, false, ErrInProgress
		}
		return h, true, nil
	}

	id := newID()
	exp := now.Add(ttl)
	start := BucketOf(user, g.buckets) // users start at different buckets, so no bucket is hot
	sawError := false
	for i := 0; i < g.buckets; i++ {
		b := (start + i) % g.buckets
		unit, err := reserveScript.Run(ctx, g.rdb, keys(sale, b), id, user, exp.UnixMilli()).Text()
		if errors.Is(err, redis.Nil) {
			continue // this bucket is empty: probe the next one
		}
		if err != nil {
			sawError = true // an unreachable bucket must never turn into a false sold out
			continue
		}
		h := Hold{ID: id, Sale: sale, Unit: unit, User: user, Bucket: b, Expires: exp}
		raw, _ := json.Marshal(h)
		g.rdb.Set(ctx, ck, raw, ttl+g.claimTTL)
		return h, false, nil
	}
	g.rdb.Del(ctx, ck)
	if sawError {
		return Hold{}, false, ErrUnavailable
	}
	return Hold{}, false, ErrSoldOut
}

func (g *Gate) release(ctx context.Context, h Hold, returnUnit bool) (bool, error) {
	flag := "0"
	if returnUnit {
		flag = "1"
	}
	n, err := releaseScript.Run(ctx, g.rdb, keys(h.Sale, h.Bucket), h.ID, flag).Int()
	return n == 1, err
}

// Forget drops a hold without returning its unit. It is called once the durable
// store owns the unit, so Redis expiry can never hand that unit to someone else.
func (g *Gate) Forget(ctx context.Context, h Hold) error {
	_, err := g.release(ctx, h, false)
	return err
}

// Release cancels an unpaid hold and returns its unit to the pool.
func (g *Gate) Release(ctx context.Context, h Hold) error {
	_, err := g.release(ctx, h, true)
	g.rdb.Del(ctx, claimKey(h.Sale, h.User))
	return err
}

// AddFree returns a durably released unit to its home bucket. SADD is idempotent,
// so a duplicate release event is harmless.
func (g *Gate) AddFree(ctx context.Context, sale, unit string) error {
	return g.rdb.SAdd(ctx, keys(sale, BucketOf(unit, g.buckets))[0], unit).Err()
}

// Take stops offering a unit, for example when a late payment re-acquires it.
func (g *Gate) Take(ctx context.Context, sale, unit string) error {
	return g.rdb.SRem(ctx, keys(sale, BucketOf(unit, g.buckets))[0], unit).Err()
}

// DurableCheck reports whether the durable store still shows a unit as FREE.
type DurableCheck func(sale, unit string) bool

// Sweep handles unpaid holds whose tokens expired more than margin ago. It reads
// the durable store before acting: a unit that payment already claimed is never
// put back, because Redis expiry must not override a durable decision.
func (g *Gate) Sweep(ctx context.Context, sale string, now time.Time, margin time.Duration, durablyFree DurableCheck) (returned, forgotten int, err error) {
	cutoff := strconv.FormatInt(now.Add(-margin).UnixMilli(), 10)
	for b := 0; b < g.buckets; b++ {
		k := keys(sale, b)
		ids, err := g.rdb.ZRangeByScore(ctx, k[2], &redis.ZRangeBy{Min: "-inf", Max: cutoff, Offset: 0, Count: 1000}).Result()
		if err != nil {
			return returned, forgotten, err
		}
		for _, id := range ids {
			v, err := g.rdb.HGet(ctx, k[1], id).Result()
			if err != nil {
				g.rdb.ZRem(ctx, k[2], id)
				continue
			}
			unit, user, _ := parseHold(v)
			h := Hold{ID: id, Sale: sale, Unit: unit, User: user, Bucket: b}
			if durablyFree(sale, unit) {
				if ok, _ := g.release(ctx, h, true); ok {
					returned++
				}
			} else if ok, _ := g.release(ctx, h, false); ok {
				forgotten++
			}
		}
	}
	return returned, forgotten, nil
}

// State returns the units Redis currently offers and the units it currently holds.
func (g *Gate) State(ctx context.Context, sale string) (free, held map[string]bool, err error) {
	free, held = map[string]bool{}, map[string]bool{}
	for b := 0; b < g.buckets; b++ {
		k := keys(sale, b)
		members, err := g.rdb.SMembers(ctx, k[0]).Result()
		if err != nil {
			return nil, nil, err
		}
		for _, m := range members {
			free[m] = true
		}
		vals, err := g.rdb.HVals(ctx, k[1]).Result()
		if err != nil {
			return nil, nil, err
		}
		for _, v := range vals {
			u, _, _ := parseHold(v)
			held[u] = true
		}
	}
	return free, held, nil
}

// Rebuild replaces a sale's Redis inventory with the durable store's FREE units.
// Unpaid holds are lost, which is acceptable: their tokens still work, and the
// durable claim settles any collision before a charge is sent.
func (g *Gate) Rebuild(ctx context.Context, sale string, durableFree []string) error {
	for b := 0; b < g.buckets; b++ {
		if err := g.rdb.Del(ctx, keys(sale, b)...).Err(); err != nil {
			return err
		}
	}
	if len(durableFree) == 0 {
		return nil
	}
	return g.Seed(ctx, sale, durableFree)
}

func parseHold(v string) (unit, user string, exp int64) {
	parts := strings.SplitN(v, "|", 3)
	if len(parts) != 3 {
		return v, "", 0
	}
	exp, _ = strconv.ParseInt(parts[2], 10, 64)
	return parts[0], parts[1], exp
}

func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
