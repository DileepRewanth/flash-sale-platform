// Command api runs the reservation and payment prototype: HTTP API, background
// worker and detectors in one process. With DEV_MODE=true and no REDIS_ADDR it
// starts an embedded in-process Redis, so it runs with no external services.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/DileepRewanth/flash-sale-platform/internal/httpapi"
	"github.com/DileepRewanth/flash-sale-platform/internal/inventory"
	"github.com/DileepRewanth/flash-sale-platform/internal/metrics"
	"github.com/DileepRewanth/flash-sale-platform/internal/psp"
	"github.com/DileepRewanth/flash-sale-platform/internal/store"
	"github.com/DileepRewanth/flash-sale-platform/internal/token"
	"github.com/DileepRewanth/flash-sale-platform/internal/worker"
)

type config struct {
	Addr, RedisAddr, Sale, AdminToken string
	DevMode, AutoRepair               bool
	Units, Buckets, MaxInFlight       int
	HoldTTL, Grace, Hard              time.Duration
	TokenKey, WebhookKey              []byte
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig(logger)
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var rdb *redis.Client
	if cfg.RedisAddr == "" {
		mr, err := miniredis.Run()
		if err != nil {
			logger.Error("embedded redis", "err", err)
			os.Exit(1)
		}
		defer mr.Close()
		rdb = redis.NewClient(&redis.Options{Addr: mr.Addr(), PoolSize: 256})
		logger.Warn("DEV_MODE: embedded in-process Redis. Fine for functional runs, not for Redis capacity numbers.")
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 256})
	}
	defer rdb.Close()

	units := make([]string, cfg.Units)
	for i := range units {
		units[i] = fmt.Sprintf("u%06d", i)
	}
	st := store.NewMemory(store.Config{PaymentGrace: cfg.Grace, HardDeadline: cfg.Hard, Fungible: true})
	st.SeedSale(cfg.Sale, units)
	gate := inventory.NewGate(rdb, cfg.Buckets)
	if err := gate.Rebuild(ctx, cfg.Sale, units); err != nil {
		logger.Error("seed redis", "err", err)
		os.Exit(1)
	}
	signer, err := token.NewSigner(cfg.TokenKey)
	if err != nil {
		logger.Error("token signer", "err", err)
		os.Exit(1)
	}
	m := metrics.New()
	provider := psp.NewFake() // a real provider integration is design only
	w := &worker.Worker{
		Store: st, Gate: gate, PSP: provider, Sale: cfg.Sale, Now: time.Now,
		ResendWindow: 5 * time.Minute, LookupAfter: 2 * time.Second, SweepMargin: 10 * time.Second,
		OnResult: func(r store.Result) { m.PaymentResults.WithLabelValues(string(r)).Inc() },
	}
	srv := httpapi.New(httpapi.Config{
		Sale: cfg.Sale, HoldTTL: cfg.HoldTTL, HardDeadline: cfg.Hard, WebhookSecret: cfg.WebhookKey,
		AdminToken: cfg.AdminToken, MaxInFlight: cfg.MaxInFlight, MaxWebhooks: 256,
	}, gate, st, signer, w, m, provider.NetCharges, time.Now)

	go w.Run(ctx, 100*time.Millisecond)
	go srv.RunDetectors(ctx, 5*time.Second, cfg.AutoRepair)

	hs := &http.Server{Addr: cfg.Addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx) // drain in-flight requests so a rolling deploy drops nothing
	}()
	logger.Info("listening", "addr", cfg.Addr, "sale", cfg.Sale, "units", cfg.Units, "buckets", cfg.Buckets)
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}

func loadConfig(logger *slog.Logger) (config, error) {
	c := config{
		Addr: env("ADDR", ":8080"), RedisAddr: os.Getenv("REDIS_ADDR"), Sale: env("SALE_ID", "drop-1"),
		DevMode: os.Getenv("DEV_MODE") == "true", AutoRepair: env("AUTO_REPAIR", "true") == "true",
		TokenKey: []byte(os.Getenv("HOLD_TOKEN_SECRET")), WebhookKey: []byte(os.Getenv("WEBHOOK_SECRET")),
		AdminToken: os.Getenv("ADMIN_TOKEN"),
	}
	var err error
	if c.Units, err = envInt("SALE_UNITS", 50000); err != nil {
		return c, err
	}
	if c.Buckets, err = envInt("SALE_BUCKETS", 8); err != nil {
		return c, err
	}
	if c.MaxInFlight, err = envInt("MAX_INFLIGHT", 1024); err != nil {
		return c, err
	}
	if c.HoldTTL, err = envDur("HOLD_TTL", 5*time.Minute); err != nil {
		return c, err
	}
	if c.Grace, err = envDur("PAYMENT_GRACE", 2*time.Minute); err != nil {
		return c, err
	}
	if c.Hard, err = envDur("HARD_DEADLINE", 30*time.Minute); err != nil {
		return c, err
	}
	if c.DevMode {
		if len(c.TokenKey) == 0 {
			c.TokenKey = randomBytes(32)
		}
		if len(c.WebhookKey) == 0 {
			c.WebhookKey = randomBytes(32)
		}
		if c.AdminToken == "" {
			c.AdminToken = hex.EncodeToString(randomBytes(16))
			logger.Warn("DEV_MODE: generated an admin token for this run", "admin_token", c.AdminToken)
		}
	}
	if c.RedisAddr == "" && !c.DevMode {
		return c, errors.New("REDIS_ADDR is required unless DEV_MODE=true")
	}
	if len(c.TokenKey) < 32 || len(c.WebhookKey) < 32 || c.AdminToken == "" {
		return c, errors.New("HOLD_TOKEN_SECRET and WEBHOOK_SECRET need at least 32 bytes and ADMIN_TOKEN must be set, or use DEV_MODE=true")
	}
	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return n, nil
}

func envDur(k string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", k, err)
	}
	return d, nil
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
