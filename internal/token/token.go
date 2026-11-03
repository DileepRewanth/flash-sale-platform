// Package token issues and verifies HMAC-signed hold tokens.
//
// Tokens are stateless: any API node can verify one with the shared key and no
// lookup, which is why a holder can still start payment while Redis is down.
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrMalformed = errors.New("token malformed")
	ErrSignature = errors.New("token signature invalid")
	ErrExpired   = errors.New("token expired")
)

// Hold is the signed claim a client presents when it starts payment.
type Hold struct {
	HoldID  string `json:"h"`
	Sale    string `json:"s"`
	Unit    string `json:"u"`
	User    string `json:"w"`
	Bucket  int    `json:"b"`
	Expires int64  `json:"e"` // unix milliseconds
}

// ExpiresAt returns the hold expiry as a time.
func (h Hold) ExpiresAt() time.Time { return time.UnixMilli(h.Expires) }

// Signer signs and verifies tokens with one secret key.
type Signer struct{ key []byte }

// NewSigner returns a signer. Short keys are rejected so a weak secret fails at startup.
func NewSigner(key []byte) (*Signer, error) {
	if len(key) < 32 {
		return nil, errors.New("token key must be at least 32 bytes")
	}
	return &Signer{key: append([]byte(nil), key...)}, nil
}

var enc = base64.RawURLEncoding

// Sign returns payload.signature, both base64url encoded.
func (s *Signer) Sign(h Hold) (string, error) {
	payload, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	body := enc.EncodeToString(payload)
	return body + "." + enc.EncodeToString(s.mac(body)), nil
}

// Verify checks the signature in constant time, then the expiry.
func (s *Signer) Verify(tok string, now time.Time) (Hold, error) {
	body, sig, ok := strings.Cut(tok, ".")
	if !ok {
		return Hold{}, ErrMalformed
	}
	got, err := enc.DecodeString(sig)
	if err != nil {
		return Hold{}, ErrMalformed
	}
	if !hmac.Equal(got, s.mac(body)) {
		return Hold{}, ErrSignature
	}
	raw, err := enc.DecodeString(body)
	if err != nil {
		return Hold{}, ErrMalformed
	}
	var h Hold
	if err := json.Unmarshal(raw, &h); err != nil {
		return Hold{}, ErrMalformed
	}
	if !now.Before(h.ExpiresAt()) {
		return h, ErrExpired
	}
	return h, nil
}

func (s *Signer) mac(body string) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(body))
	return m.Sum(nil)
}
