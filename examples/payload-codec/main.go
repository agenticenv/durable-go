// Package main walks durable-go payload privacy options: plaintext default,
// AES-GCM, a reversible custom codec, HMAC step tokens, journal MAC, then
// inspect hints.
//
//	go run .
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/examples/internal/exdir"
)

const (
	demoPayloadKeyHex = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff" // gitleaks:allow demo key, not a secret
	demoPrefix        = "DEMO:"
	echoTaskID        = "echo"
	echoRunID         = "run-1"
	echoInput         = "hello"
)

// demoPrefixCodec is a reversible stub so `go run` needs no cloud KMS.
// Replace Encode/Decode with a real KMS (and pass aad as encryption context).
type demoPrefixCodec struct{}

func (demoPrefixCodec) Encode(plaintext, _ []byte) ([]byte, error) {
	out := make([]byte, 0, len(demoPrefix)+len(plaintext))
	out = append(out, demoPrefix...)
	out = append(out, plaintext...)
	return out, nil
}

func (demoPrefixCodec) Decode(ciphertext, _ []byte) ([]byte, error) {
	if !bytes.HasPrefix(ciphertext, []byte(demoPrefix)) {
		return nil, fmt.Errorf("demo codec: missing %q prefix", demoPrefix)
	}
	out := make([]byte, len(ciphertext)-len(demoPrefix))
	copy(out, ciphertext[len(demoPrefix):])
	return out, nil
}

var echoTask = durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
	return durable.RunStep(ctx, s, "echo", in, func(_ context.Context, in string) (string, error) {
		return in, nil
	}).Get(ctx)
})

var tokenCh = make(chan string, 1)

var approveTask = durable.Func(func(ctx context.Context, s *durable.StepRunner, in string) (string, error) {
	return durable.RunStep(ctx, s, "approve", in, func(ctx context.Context, _ string) (string, error) {
		tokenCh <- s.StepToken(ctx)
		return "", durable.ErrStepPending
	}, durable.WithStepTokenTTL(7*24*time.Hour)).Get(ctx)
})

func main() {
	log.SetFlags(0)
	ctx := context.Background()

	fmt.Println("durable-go payload codec")
	fmt.Println()

	fmt.Println("── 1. Default — plaintext JSON ──")
	plainDir := journalDir("plain")
	runEcho(ctx, plainDir)
	showInput(plainDir, echoTaskID, echoRunID)

	fmt.Println()
	fmt.Println("── 2. AES-GCM — key from DURABLE_PAYLOAD_KEY (or demo hex) ──")
	key := payloadKey()
	codec, err := durable.NewAESGCMCodec(key)
	if err != nil {
		log.Fatal(err)
	}
	aesDir := journalDir("aes")
	runEcho(ctx, aesDir, durable.WithPayloadCodec(codec))
	showInput(aesDir, echoTaskID, echoRunID)
	fmt.Printf("  inspect key (hex): %s\n", hex.EncodeToString(key))

	fmt.Println()
	fmt.Println("── 3. Custom PayloadCodec — demo prefix (not a real KMS) ──")
	kmsDir := journalDir("kms")
	runEcho(ctx, kmsDir, durable.WithPayloadCodec(demoPrefixCodec{}))
	showInput(kmsDir, echoTaskID, echoRunID)

	fmt.Println()
	fmt.Println("── 4. HMAC step tokens — WithStepTokenKey ──")
	tokDir := journalDir("tokens")
	runApprove(ctx, tokDir)

	fmt.Println()
	fmt.Println("── 5. Journal MAC — WithJournalMACKey ──")
	macKey := journalMACKey()
	macDir := journalDir("journal-mac")
	runEcho(ctx, macDir, durable.WithJournalMACKey(macKey))
	showJournalTrailer(macDir, echoTaskID, echoRunID, true)
	fmt.Printf("  export DURABLE_JOURNAL_MAC_KEY=%s\n", hex.EncodeToString(macKey))

	fmt.Println()
	fmt.Println("Inspect (writer is closed; set keys in the env, not flags):")
	fmt.Printf("  go run ./cmd/durable-inspect -d %s task get %s %s\n", plainDir, echoTaskID, echoRunID)
	fmt.Printf("  go run ./cmd/durable-inspect -d %s --redact task get %s %s\n", plainDir, echoTaskID, echoRunID)
	fmt.Printf("  export DURABLE_PAYLOAD_KEY=%s\n", hex.EncodeToString(key))
	fmt.Printf("  go run ./cmd/durable-inspect -d %s task get %s %s\n", aesDir, echoTaskID, echoRunID)
	fmt.Printf("  go run ./cmd/durable-inspect -d %s --redact task get %s %s\n", aesDir, echoTaskID, echoRunID)
	fmt.Printf("  go run ./cmd/durable-inspect -d %s step get %s %s echo\n", macDir, echoTaskID, echoRunID)
}

func journalDir(name string) string {
	return exdir.Data("payload-codec", name)
}

func payloadKey() []byte {
	s := os.Getenv("DURABLE_PAYLOAD_KEY")
	if s == "" {
		s = demoPayloadKeyHex
	}
	if b, err := hex.DecodeString(s); err == nil {
		switch len(b) {
		case 16, 24, 32:
			return b
		}
	}
	raw := []byte(s)
	switch len(raw) {
	case 16, 24, 32:
		return raw
	default:
		log.Fatal("DURABLE_PAYLOAD_KEY must be 16, 24, or 32 bytes, or hex of that length")
		return nil
	}
}

func tokenSecret() []byte {
	if s := os.Getenv("DURABLE_TOKEN_SECRET"); s != "" {
		return []byte(s)
	}
	return []byte("demo-token-secret")
}

func journalMACKey() []byte {
	s := os.Getenv("DURABLE_JOURNAL_MAC_KEY")
	if s == "" {
		return []byte("demo-journal-mac-key")
	}
	if b, err := hex.DecodeString(s); err == nil && len(b) > 0 {
		return b
	}
	return []byte(s)
}

func runEcho(ctx context.Context, dir string, opts ...durable.Option) {
	e, err := durable.NewEngine(ctx, dir, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := durable.RegisterTask(e, echoTaskID, echoTask, durable.WithName("Echo")); err != nil {
		log.Fatal(err)
	}
	out, err := durable.RunTask[string, string](ctx, e, echoTaskID, echoRunID, echoInput).Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  journal: %s\n", dir)
	fmt.Printf("  result:  %q\n", out)
	if err := e.Close(); err != nil {
		log.Fatal(err)
	}
}

func runApprove(ctx context.Context, dir string) {
	secret := tokenSecret()
	e, err := durable.NewEngine(ctx, dir, durable.WithStepTokenKey(secret))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	if err := durable.RegisterTask(e, "approve", approveTask, durable.WithName("Approve")); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("  journal: %s\n", dir)
	run := durable.RunTask[string, string](ctx, e, "approve", echoRunID, "ticket-1")
	done := make(chan struct{})
	var out string
	var runErr error
	go func() {
		out, runErr = run.Get(ctx)
		close(done)
	}()

	select {
	case token := <-tokenCh:
		kind := "unsigned"
		if strings.HasPrefix(token, "v1.") {
			kind = "HMAC v1"
		}
		fmt.Printf("  token:   %s\n", kind)
		if err := durable.CompleteStep(ctx, e, token, "approved"); err != nil {
			log.Fatal(err)
		}
	case <-done:
		fmt.Println("  (run already completed — nothing to approve)")
	}

	<-done
	if runErr != nil {
		log.Fatal(runErr)
	}
	fmt.Printf("  result:  %q\n", out)
	if err := e.Close(); err != nil {
		log.Fatal(err)
	}
}

func showJournalTrailer(dir, taskID, runID string, hmac bool) {
	p := filepath.Join(dir, "tasks", taskID, runID, "journal.log")
	b, err := os.ReadFile(p)
	if err != nil {
		log.Fatal(err)
	}
	trail, kind := 4, "CRC32 (4 B)"
	if hmac {
		trail, kind = 32, "HMAC-SHA256 (32 B)"
	}
	var frames int
	for off := 0; off+4+trail <= len(b); {
		n := int(binary.BigEndian.Uint32(b[off : off+4]))
		next := off + 4 + n + trail
		if next > len(b) {
			break
		}
		frames++
		off = next
	}
	fmt.Printf("  frames:  %d × %s\n", frames, kind)
}

func showInput(dir, taskID, runID string) {
	p := filepath.Join(dir, "tasks", taskID, runID, "input.json")
	b, err := os.ReadFile(p)
	if err != nil {
		log.Fatal(err)
	}
	switch {
	case bytes.Equal(b, []byte(`"`+echoInput+`"`)):
		fmt.Printf("  on disk: %s  (plaintext JSON)\n", b)
	case bytes.HasPrefix(b, []byte(demoPrefix)):
		fmt.Printf("  on disk: %s…  (custom codec)\n", demoPrefix)
	case len(b) > 0 && b[0] == 0x01:
		fmt.Printf("  on disk: 0x01… (%d bytes, AES-GCM envelope)\n", len(b))
	default:
		fmt.Printf("  on disk: %d bytes (not plaintext JSON)\n", len(b))
	}
}
