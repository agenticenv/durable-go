package durable

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestIssueDecodeStepToken_UnsignedDefault(t *testing.T) {
	e := &Engine{}
	tok := e.issueStepToken("t", "r", "wait", time.Hour)
	if strings.HasPrefix(tok, hmacTokenPrefix) {
		t.Fatalf("unsigned token should not use HMAC prefix: %s", tok)
	}
	taskID, runID, stepID, err := e.decodeStepToken(tok)
	if err != nil || taskID != "t" || runID != "r" || stepID != "wait" {
		t.Fatalf("got %s/%s/%s err=%v", taskID, runID, stepID, err)
	}
}

func TestDecodeStepToken_SecretRejectsUnsigned(t *testing.T) {
	e := &Engine{cfg: engineConfig{tokenSecret: []byte("secret")}}
	unsigned := encodeStepToken("t", "r", "wait")
	if _, _, _, err := e.decodeStepToken(unsigned); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("got %v, want ErrInvalidToken", err)
	}
}

func TestIssueDecodeStepToken_HMACRoundTrip(t *testing.T) {
	e := &Engine{cfg: engineConfig{tokenSecret: []byte("secret")}}
	tok := e.issueStepToken("t", "r", "kind:approve", time.Hour)
	if !strings.HasPrefix(tok, hmacTokenPrefix) {
		t.Fatalf("expected HMAC token, got %s", tok)
	}
	taskID, runID, stepID, err := e.decodeStepToken(tok)
	if err != nil || taskID != "t" || runID != "r" || stepID != "kind:approve" {
		t.Fatalf("got %s/%s/%s err=%v", taskID, runID, stepID, err)
	}
}

func TestDecodeStepToken_WrongMAC(t *testing.T) {
	e := &Engine{cfg: engineConfig{tokenSecret: []byte("secret")}}
	tok := e.issueStepToken("t", "r", "wait", time.Hour)
	bad := tok[:len(tok)-2] + "aa"
	if _, _, _, err := e.decodeStepToken(bad); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("got %v, want ErrInvalidToken", err)
	}
}

func TestDecodeStepToken_Expired(t *testing.T) {
	orig := stepTokenNow
	now := time.UnixMilli(1_700_000_000_000).UTC()
	stepTokenNow = func() time.Time { return now }
	t.Cleanup(func() { stepTokenNow = orig })

	e := &Engine{cfg: engineConfig{tokenSecret: []byte("secret")}}
	tok := e.issueStepToken("t", "r", "wait", time.Hour)
	stepTokenNow = func() time.Time { return now.Add(2 * time.Hour) }
	if _, _, _, err := e.decodeStepToken(tok); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("got %v, want ErrTokenExpired", err)
	}
}

func TestDecodeStepToken_ZeroTTLDoesNotExpire(t *testing.T) {
	orig := stepTokenNow
	now := time.UnixMilli(1_700_000_000_000).UTC()
	stepTokenNow = func() time.Time { return now }
	t.Cleanup(func() { stepTokenNow = orig })

	e := &Engine{cfg: engineConfig{tokenSecret: []byte("secret")}}
	tok := e.issueStepToken("t", "r", "wait", 0)
	stepTokenNow = func() time.Time { return now.Add(365 * 24 * time.Hour) }
	if _, _, _, err := e.decodeStepToken(tok); err != nil {
		t.Fatalf("zero TTL should not expire: %v", err)
	}
}

func TestEffectiveTokenTTL(t *testing.T) {
	e := &Engine{}
	if e.effectiveTokenTTL(nil) != 0 {
		t.Fatal("no secret should ignore TTL")
	}
	e.cfg.tokenSecret = []byte("s")
	if e.effectiveTokenTTL(nil) != defaultTokenTTL {
		t.Fatalf("default TTL %s", e.effectiveTokenTTL(nil))
	}
	hour := time.Hour
	e.cfg.tokenTTL = &hour
	if e.effectiveTokenTTL(nil) != time.Hour {
		t.Fatal("engine TTL override")
	}
	week := 7 * 24 * time.Hour
	if e.effectiveTokenTTL(&week) != week {
		t.Fatal("step TTL override")
	}
}

func TestHMACToken_NoSecretCannotDecode(t *testing.T) {
	signed := (&Engine{cfg: engineConfig{tokenSecret: []byte("secret")}}).issueStepToken("t", "r", "wait", time.Hour)
	e := &Engine{}
	if _, _, _, err := e.decodeStepToken(signed); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("got %v, want ErrInvalidToken", err)
	}
}
