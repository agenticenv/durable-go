package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

const (
	maxSteps        = 10_000
	maxPayloadBytes = 8 << 20 // 8 MiB — well under the 32 MiB journal frame cap
)

func parsePayload(s string) (int, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" {
		return 0, fmt.Errorf("payload must not be empty")
	}
	i := 0
	for i < len(s) && (unicode.IsDigit(rune(s[i])) || s[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid payload %q", s)
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid payload %q: %w", s, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("payload must be > 0")
	}
	mult := 1.0
	switch strings.TrimSpace(s[i:]) {
	case "", "b", "byte", "bytes":
		// keep 1.0
	case "k", "kb", "kib":
		mult = 1024
	case "m", "mb", "mib":
		mult = 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown payload unit in %q (use b, kb, mb)", s)
	}
	bytes := int(n * mult)
	if bytes <= 0 {
		return 0, fmt.Errorf("payload must be > 0")
	}
	if bytes > maxPayloadBytes {
		return 0, fmt.Errorf("payload %d B exceeds cap of %d B", bytes, maxPayloadBytes)
	}
	return bytes, nil
}

func formatBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1024*1024 {
		return fmt.Sprintf("%.1f KiB", float64(n)/1024)
	}
	return fmt.Sprintf("%.1f MiB", float64(n)/(1024*1024))
}
