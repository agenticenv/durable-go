# payload-codec — plaintext, AES-GCM, custom codec, HMAC tokens, journal MAC

One `go run` writes five journals under `.data/` so you can compare on-disk bytes and inspect them afterwards.

Existing examples (`func-task`, `resume`, …) stay default `NewEngine` (plaintext). This one is the privacy walkthrough. Each mode writes its **own** `.data/<name>/` directory — one `dataDir`, one codec, one journal MAC key. To add security and keep an old journal, use a second engine on a new path.

## Run

From the **repo root**:

```bash
go run ./examples/payload-codec/
```

Optional env (otherwise demo values are used — not for production):

| Variable | Role |
|---|---|
| `DURABLE_PAYLOAD_KEY` | AES-GCM key: 16/24/32 raw bytes, or hex of that |
| `DURABLE_TOKEN_SECRET` | HMAC secret for `CompleteStep` tokens |
| `DURABLE_JOURNAL_MAC_KEY` | HMAC key: raw bytes, or hex of the key (inspect hex-decodes) |

## Modes

### 1. Default — plaintext JSON

```go
e, err := durable.NewEngine(ctx, "./data")
```

`input.json` is `"hello"`. Same as every other example.

### 2. Built-in AES-GCM

Key from env / secret manager — never under `dataDir`:

```go
key, err := hex.DecodeString(os.Getenv("DURABLE_PAYLOAD_KEY")) // 16, 24, or 32 bytes as hex
codec, err := durable.NewAESGCMCodec(key)
e, err := durable.NewEngine(ctx, "./data", durable.WithPayloadCodec(codec))
```

On disk: version `0x01` + nonce + ciphertext. Resume/inspect without the same key fails closed (inspect with no key prints quoted ciphertext).

### 3. User-owned codec

Any reversible `Encode`/`Decode`. AAD binds each blob to kind/task/run/step — pass it through to KMS as encryption context:

```go
type kmsCodec struct{ client KMS }

func (c kmsCodec) Encode(plaintext, aad []byte) ([]byte, error) {
    return c.client.Encrypt(plaintext, aad)
}
func (c kmsCodec) Decode(ciphertext, aad []byte) ([]byte, error) {
    return c.client.Decrypt(ciphertext, aad)
}

e, err := durable.NewEngine(ctx, "./data", durable.WithPayloadCodec(kmsCodec{client: kms}))
```

This example uses an in-process `DEMO:` prefix stub so `go run` needs no cloud. Do not use that stub in production.

### 4. HMAC step tokens

Unsigned tokens (no expiry) unless a key is set. With `WithStepTokenKey`, tokens are HMAC `v1.…` and default to 24h TTL:

```go
e, err := durable.NewEngine(ctx, "./data",
    durable.WithStepTokenKey([]byte(os.Getenv("DURABLE_TOKEN_SECRET"))),
    // WithDefaultStepTokenTTL optional; default 24h when key is set
)
// ...
durable.RunStep(ctx, s, "approve", in, fn, durable.WithStepTokenTTL(7*24*time.Hour))
```

The secret is not stored in `dataDir`.

### 5. Journal MAC

Default frames end in CRC32 (torn-write detection only). `WithJournalMACKey` replaces that trailer with HMAC-SHA256 and signs `input.json` / `output.json` / `meta.json` so an editor cannot rewrite cached results or flip run status:

```go
key, err := hex.DecodeString(os.Getenv("DURABLE_JOURNAL_MAC_KEY")) // or []byte(env) if not hex
e, err := durable.NewEngine(ctx, "./data", durable.WithJournalMACKey(key))
```

Payloads stay plaintext JSON in this mode — the MAC authenticates frames, it does not encrypt them. The key is not stored in `dataDir`. A wrong key fails closed. Omit the key and HMAC frames look like a torn CRC tail (inspect: empty / not found).

## Inspect

After the example exits, the writer lock is released:

```bash
# set keys in the environment (do not pass --payload-key / --journal-mac-key)
export DURABLE_PAYLOAD_KEY=...          # hex or 16/24/32 raw bytes
export DURABLE_JOURNAL_MAC_KEY=...      # hex or raw bytes

# plaintext JSON, then hide INPUT/RESULT
go run ./cmd/durable-inspect -d examples/payload-codec/.data/plain task get echo run-1
go run ./cmd/durable-inspect -d examples/payload-codec/.data/plain --redact task get echo run-1

# AES: no env key → ciphertext; DURABLE_PAYLOAD_KEY → JSON; + --redact → [redacted]
go run ./cmd/durable-inspect -d examples/payload-codec/.data/aes task get echo run-1
go run ./cmd/durable-inspect -d examples/payload-codec/.data/aes --redact step get echo run-1 echo

# journal MAC: no env key → empty/not found; matching DURABLE_JOURNAL_MAC_KEY → step result
go run ./cmd/durable-inspect -d examples/payload-codec/.data/journal-mac step get echo run-1 echo
```

The example prints `export DURABLE_*` lines (demo values if the env vars were unset). `--redact` is display-only; it is not a codec.

See [Data privacy](../../README.md#data-privacy--sensitive-payloads) and [`cmd/durable-inspect/README.md`](../../cmd/durable-inspect/README.md).

## Reset

```bash
rm -rf examples/payload-codec/.data/
```
