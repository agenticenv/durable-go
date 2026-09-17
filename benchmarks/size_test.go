package main

import "testing"

func TestParsePayload(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{in: "1024", want: 1024},
		{in: "1kb", want: 1024},
		{in: "1KB", want: 1024},
		{in: "1 kib", want: 1024},
		{in: "64kb", want: 64 * 1024},
		{in: "1mb", want: 1024 * 1024},
		{in: "2.5kb", want: 2560},
		{in: "0", wantErr: true},
		{in: "-1", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "1tb", wantErr: true},
	}
	for _, tc := range cases {
		got, err := parsePayload(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parsePayload(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parsePayload(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parsePayload(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	if got := formatBytes(512); got != "512 B" {
		t.Fatalf("got %q", got)
	}
	if got := formatBytes(1024); got != "1.0 KiB" {
		t.Fatalf("got %q", got)
	}
}

func TestParseFlagsStripsGoRunDashDash(t *testing.T) {
	cfg, err := parseFlags([]string{"--", "-steps", "3", "-payload", "512"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.steps != 3 || cfg.payloadBytes != 512 {
		t.Fatalf("got steps=%d payload=%d", cfg.steps, cfg.payloadBytes)
	}
}
