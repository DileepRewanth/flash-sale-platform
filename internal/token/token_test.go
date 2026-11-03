package token

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	s, err := NewSigner([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	h := Hold{HoldID: "h1", Sale: "s", Unit: "u1", User: "alice", Bucket: 2, Expires: now.Add(time.Minute).UnixMilli()}
	tok, err := s.Sign(h)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Verify(tok, now)
	if err != nil || got != h {
		t.Fatalf("Verify = %+v, %v", got, err)
	}
	if _, err := s.Verify(tok, now.Add(2*time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired token: got %v", err)
	}
	tampered := "x" + tok[1:]
	if _, err := s.Verify(tampered, now); err == nil {
		t.Fatal("a tampered token must not verify")
	}
	other, _ := NewSigner([]byte(strings.Repeat("z", 32)))
	if _, err := other.Verify(tok, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong key: got %v", err)
	}
}

func TestShortKeyRejected(t *testing.T) {
	if _, err := NewSigner([]byte("short")); err == nil {
		t.Fatal("short keys must be rejected at startup")
	}
}
