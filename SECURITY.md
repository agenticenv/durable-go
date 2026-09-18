# Security Policy

## Supported Versions

Security fixes are applied to the **default branch** (`main`) and released as **semver tags** (`vMAJOR.MINOR.PATCH`). Treat the **[latest GitHub release](https://github.com/agenticenv/durable-go/releases/latest)** as the current supported line.

- **Latest release:** full support (including security fixes).
- **Older tags:** we do not guarantee long-term support for every past version; upgrade to the latest release when possible.

Maintainers document breaking changes in release notes. See [RELEASING.md](RELEASING.md) for how versions are cut.

## Reporting a Vulnerability

Please report security vulnerabilities by opening a [GitHub Security Advisory](https://github.com/agenticenv/durable-go/security/advisories/new). Do not open a public issue for security vulnerabilities.

We will acknowledge your report within 48 hours and will send a more detailed response within 7 days. Please do not publicly disclose the vulnerability until we have released a fix.

We appreciate responsible disclosure and will acknowledge security researchers who help us improve the security of this project (with their permission). We do not currently offer a bug bounty or monetary rewards for vulnerability reports.

## Scope

- Security issues in the durable-go engine (`NewEngine`, `RunTask`, `RunStep`, `CompleteStep`, task lifecycle)
- The filesystem journal under `dataDir` (`meta.json`, `input.json`, `journal.log`, `output.json`, OS flock)
- Optional `PayloadCodec` / AES-GCM, HMAC step tokens, journal MAC, and `durable-inspect`
- Sensitive data exposure in persisted task or step records

## Security Considerations

Application-level payload choices are the caller's responsibility. The library persists what you put in task/step I/O. See [README — Data privacy](README.md#data-privacy--sensitive-payloads).

### Task and step payloads

Step and task I/O are JSON-marshalled and stored (`input.json`, `output.json`, journal step Input/Result, `CompleteStep` signal payloads). Do not put secrets, API keys, credentials, or PII there. Persist IDs and load secrets inside `fn` from the environment or a secret manager.

Default persist is **plaintext JSON**. `WithPayloadCodec` (including `NewAESGCMCodec`) is opt-in, reversible, and applied after marshal. It is not redaction. Decode failure is fail-closed. The codec key is never written under `dataDir`. Additional authenticated data binds each blob to kind/task/run/step so ciphertext cannot be copied between fields. Compact copies ciphertext as-is. Do not enable a codec on an existing plaintext tree; open a second `NewEngine` on a new `dataDir` (keep the old journal) or wipe the old one.

`Error`, `PanicTrace`, step IDs, status, timestamps, and `meta.json` are **not** encoded. Logs may echo what the process prints.

### Journal files

The engine writes `meta.json`, `input.json`, `journal.log`, and `output.json` under `dataDir/tasks/`. On Unix, directories are `0700` and files `0600`; `NewEngine` chmods an existing tree and warns if `dataDir` is still group- or world-accessible. Windows chmod is best-effort. Encryption is at rest versus other local users, not versus this process or root. Do not commit `examples/**/.data/` directories.

### Step tokens

`CompleteStep` tokens are unsigned with no expiry unless `WithStepTokenKey` is set. With a key, tokens are HMAC-SHA256 with a default 24h TTL (`WithDefaultStepTokenTTL`, per-step `WithStepTokenTTL`). The key is not stored in `dataDir`. Unsigned tokens are rejected when a key is configured. `CancelRun`'s internal signal is not a user token.

### Journal frames

Default `journal.log` frames use a CRC32 trailer (crash/torn-write detection). CRC is not authentication: anyone who can edit the file can change a cached step result and recompute the CRC. `WithJournalMACKey` replaces the trailer with HMAC-SHA256 and signs `input.json` / `output.json` / `meta.json` (32-byte trailer, bound to task/run; key not stored in `dataDir`). Resume fail-closes on a MAC mismatch. Truncating or deleting files is still possible; missing steps may re-run. Do not enable a journal MAC on an existing unsigned tree; open a second `NewEngine` on a new `dataDir` or wipe the old one.

### Inspect

Set `DURABLE_PAYLOAD_KEY` / `DURABLE_JOURNAL_MAC_KEY` in the environment (inspect flags put secrets on the command line). Hex is tried first for both keys; otherwise the string is raw bytes. A wrong key fails closed; omit the journal MAC key and HMAC frames look empty. One `-d` and one key pair per command — plaintext and AES (or CRC and HMAC) journals belong in separate directories (a second `NewEngine` on a new `dataDir`, not mixed in one tree). `--redact` prints `[redacted]` for non-empty INPUT and RESULT even after decrypt. Status, IDs, ERROR, and PANIC stay visible. `--redact` is display-only; it does not change the journal.

### Third-party dependencies

Monitor via **Dependabot** (`.github/dependabot.yml`) and **`govulncheck`** (`task govuln`, Security workflow, and Release hard-fail).

## Out of Scope

- Callers' application logic inside task or step closures
- External services invoked from user steps
- General usage questions
- Encrypting `Error` / `PanicTrace`, key rotation, and secure wipe on purge
