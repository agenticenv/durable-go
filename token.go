package durable

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

const (
	hmacTokenPrefix = "v1."
	defaultTokenTTL = 24 * time.Hour
)

type stepTokenTTLKey struct{}

var stepTokenNow = time.Now

// WithStepTokenKey enables HMAC StepToken values used by CompleteStep.
// Omit it (or pass nil/empty) to keep unsigned tokens with no expiry, the
// default. The key is copied and never written under dataDir.
// Ignored by NewReadOnlyEngine.
func WithStepTokenKey(key []byte) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c == nil {
			return
		}
		if len(key) == 0 {
			c.tokenSecret = nil
			return
		}
		c.tokenSecret = append([]byte(nil), key...)
	}
}

// WithDefaultStepTokenTTL sets how long HMAC StepToken values remain valid.
// Ignored when no step-token key is configured. Zero means no expiry.
// Omit this option to use the default of 24 hours when a key is set.
// Overridden per step by WithStepTokenTTL. Ignored by NewReadOnlyEngine.
func WithDefaultStepTokenTTL(d time.Duration) Option {
	return func(c *engineConfig, _ *readOnlyConfig) {
		if c != nil {
			c.tokenTTL = &d
		}
	}
}

// WithStepTokenTTL overrides the engine StepToken TTL for this step.
// Ignored when no step-token key is configured. Zero means no expiry for
// this step.
func WithStepTokenTTL(d time.Duration) StepOption {
	return func(c *stepConfig) { c.tokenTTL = &d }
}

func withStepTokenTTL(ctx context.Context, ttl time.Duration) context.Context {
	return context.WithValue(ctx, stepTokenTTLKey{}, ttl)
}

func tokenTTLFromCtx(ctx context.Context) time.Duration {
	ttl, _ := ctx.Value(stepTokenTTLKey{}).(time.Duration)
	return ttl
}

func (e *Engine) effectiveTokenTTL(stepOverride *time.Duration) time.Duration {
	if len(e.cfg.tokenSecret) == 0 {
		return 0
	}
	if stepOverride != nil {
		return *stepOverride
	}
	if e.cfg.tokenTTL != nil {
		return *e.cfg.tokenTTL
	}
	return defaultTokenTTL
}

func (e *Engine) issueStepToken(taskID, runID, stepID string, ttl time.Duration) string {
	if len(e.cfg.tokenSecret) == 0 {
		return encodeStepToken(taskID, runID, stepID)
	}
	var exp int64
	if ttl > 0 {
		exp = stepTokenNow().UTC().UnixMilli() + ttl.Milliseconds()
	}
	payload := taskID + ":" + runID + ":" + stepID + ":" + strconv.FormatInt(exp, 10)
	mac := hmacSHA256(e.cfg.tokenSecret, []byte(payload))
	return hmacTokenPrefix +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.RawURLEncoding.EncodeToString(mac)
}

func (e *Engine) decodeStepToken(token string) (taskID, runID, stepID string, err error) {
	if strings.HasPrefix(token, hmacTokenPrefix) {
		if len(e.cfg.tokenSecret) == 0 {
			return "", "", "", ErrInvalidToken
		}
		return decodeHMACStepToken(token, e.cfg.tokenSecret)
	}
	if len(e.cfg.tokenSecret) > 0 {
		return "", "", "", ErrInvalidToken
	}
	return decodeStepToken(token)
}

func decodeHMACStepToken(token string, secret []byte) (taskID, runID, stepID string, err error) {
	rest := strings.TrimPrefix(token, hmacTokenPrefix)
	dot := strings.LastIndexByte(rest, '.')
	if dot <= 0 || dot == len(rest)-1 {
		return "", "", "", ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(rest[:dot])
	if err != nil {
		return "", "", "", ErrInvalidToken
	}
	mac, err := base64.RawURLEncoding.DecodeString(rest[dot+1:])
	if err != nil {
		return "", "", "", ErrInvalidToken
	}
	if !hmac.Equal(mac, hmacSHA256(secret, payload)) {
		return "", "", "", ErrInvalidToken
	}
	taskID, runID, stepID, exp, err := parseHMACTokenPayload(string(payload))
	if err != nil {
		return "", "", "", err
	}
	if exp > 0 && stepTokenNow().UTC().UnixMilli() > exp {
		return "", "", "", ErrTokenExpired
	}
	return taskID, runID, stepID, nil
}

func parseHMACTokenPayload(payload string) (taskID, runID, stepID string, exp int64, err error) {
	parts := strings.Split(payload, ":")
	if len(parts) < 4 {
		return "", "", "", 0, ErrInvalidToken
	}
	exp, err = strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return "", "", "", 0, ErrInvalidToken
	}
	taskID, runID = parts[0], parts[1]
	stepID = strings.Join(parts[2:len(parts)-1], ":")
	if taskID == "" || runID == "" || stepID == "" {
		return "", "", "", 0, ErrInvalidToken
	}
	return taskID, runID, stepID, exp, nil
}

func hmacSHA256(secret, payload []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

// encodeStepToken uses base64.RawURLEncoding of "taskID:runID:stepID".
// runID is a ULID (no colons); taskID is rejected if it contains ':'.
// stepID may contain colons because decode uses SplitN(..., 3).
func encodeStepToken(taskID, runID, stepID string) string {
	raw := taskID + ":" + runID + ":" + stepID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeStepToken(token string) (taskID, runID, stepID string, err error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", "", "", ErrInvalidToken
	}
	parts := strings.SplitN(string(b), ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", ErrInvalidToken
	}
	return parts[0], parts[1], parts[2], nil
}
