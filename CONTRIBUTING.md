# Contributing to durable-go

Thank you for your interest in contributing. **durable-go** is a single-node Go library for fault-tolerant task execution and step memoization. This document explains how to set up your environment and what we expect from contributors.

## Contributor License Agreement (CLA)

By contributing to this project, you agree that your contributions will be governed by our [Contributor License Agreement](https://github.com/agenticenv/durable-go/blob/main/CLA.md).

When you submit your first Pull Request, our CLA Assistant bot will automatically prompt you to review and digitally sign the agreement if you haven't already.

## Prerequisites

Before contributing, ensure you have:

| Requirement | Version / Notes |
|-------------|-----------------|
| **Go** | **Minimum `go 1.26.5`** (see the `go` line in `go.mod`; use that version or newer). |
| **Task** | Task runner for all dev commands (`task build`, `task check`, `task lint`, ...). Install: `go install github.com/go-task/task/v3/cmd/task@latest` or see [taskfile.dev/installation](https://taskfile.dev/installation/) |
| **golangci-lint** | Required for `task lint` — install **v2** with Go **≥** the `go` line in `go.mod`: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest` |
| **gofmt** | `task lint` runs `gofmt -s` check first; run `task fmt` to apply `gofmt -s -w` project-wide |
| **misspell** | `task spell` or `task lint` — typos via `misspell` |

## Development Workflow

### 1. Clone and prepare

**Fork** the repo on GitHub (if you don't have push access), then clone your fork:

```bash
git clone https://github.com/<your-username>/durable-go.git
cd durable-go
git remote add upstream https://github.com/agenticenv/durable-go.git
go mod download
```

(If you have push access, you may clone the main repo and create branches there.)

### 2. Create a branch for your changes

Create a branch from `main` for each change. Do not push directly to `main`.

```bash
git checkout main
git pull upstream main    # or origin main if using main repo
git checkout -b <branch-name>
```

**Branch naming** (common open source practice):

| Prefix | Use for |
|--------|---------|
| `feat/` | New features (e.g. `feat/add-retry`, `feat/step-timeout`) |
| `fix/` | Bug fixes (e.g. `fix/nil-pointer`, `fix/replay-cache`) |
| `docs/` | Documentation only (e.g. `docs/readme`, `docs/api-examples`) |
| `test/` | Test additions or fixes (e.g. `test/sqlite-purge`) |
| `refactor/` | Code refactoring, no behavior change |
| `chore/` | Maintenance (deps, tooling, config) |

Keep your branch short and descriptive. Sync with `main` before opening a PR: `git pull upstream main` (or rebase if you prefer). Push your branch to your fork and open a PR against `main`.

### 3. Run checks before a PR

```bash
task check
```

Runs `fmt-check`, spell check, `task lint`, `task test`, `task build`, `task secrets-scan`, and `task govuln` — local parity with CI plus gitleaks/govulncheck from the **Security** workflow (coverage is CI-only; use `task test-coverage` locally if you want a report). `task govuln` sets `GOTOOLCHAIN` from `go.mod` so stdlib findings match CI.

**CI and Security run automatically** on pull requests to `main`. Quality is **CI**; secrets/vulns/CodeQL are **Security**. Pushes or merges to `main` do not trigger CI; use **workflow_dispatch** in GitHub Actions for an on-demand CI or Security run. Run `task check` locally before opening a PR; CI and Security must pass on the PR before merge.

To run only tests (e.g. while iterating):

```bash
task test
```

Or a specific package:

```bash
go test ./store/sqlite/... -count=1 -v
```

### 4. Run linters (included in `task check`)

```bash
task lint
```

This runs `gofmt -s` check, `misspell`, `go vet`, and `golangci-lint`. Use when debugging a lint failure without re-running the full `task check`.

**golangci-lint vs Go version:** If you see `the Go language version used to build golangci-lint is lower than the targeted Go version`, your `golangci-lint` binary is too old for this module (Go 1.26+ requires **golangci-lint v2**). Reinstall: `go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest`, ensure `$(go env GOPATH)/bin` is on `PATH` ahead of any older install, then run `golangci-lint version` — it should report **v2.x** and a Go build **≥ 1.26**.

### 5. Generate coverage

```bash
task test-coverage
# Open coverage.html in a browser
```

### 6. Run examples

From the repo root:

```bash
go run ./examples/resume/
go run ./examples/payment/
go run ./examples/agent/
```

See [examples/README.md](examples/README.md). Example SQLite files are written under `examples/<name>/.data/` (gitignored).

## Ways to Contribute

### Propose a feature

Before implementing a new feature, **open an issue** to propose and discuss it. This helps:

- Align on scope and design before you spend time coding
- Avoid duplicate work if someone else is already working on it
- Get feedback from maintainers early

Use the **Feature** or **Enhancement** label if available, and include: use case, proposed API or behavior, and any alternatives you considered.

### Report bugs

Found a bug? **Open an issue** with:

- Steps to reproduce
- Expected vs actual behavior
- Go version, OS, and (if relevant) store driver
- Minimal code or config that reproduces the problem

### Code contributions

1. **Discuss first** for larger changes — open an issue or discussion before a big PR.
2. **Small fixes** (typos, docs, obvious bugs) can go directly to a PR.
3. **Pull requests** — see [What Contributors Must Follow](#what-contributors-must-follow) below.

## What Contributors Must Follow

1. **Code quality**
   - Run `task check` before submitting a PR (format, spell, lint, test, build, secrets scan). PRs must pass.
   - Run `task tidy` before committing if you add or remove dependencies.

2. **Tests**
   - Add tests for new features and bug fixes.
   - Unit tests go in `*_test.go` files alongside the code.

3. **Commits**
   - Use [conventional commits](https://www.conventionalcommits.org) — these drive the release changelog:
     - `feat: add step timeout` — features
     - `fix: honour UpdatedAt on SaveTask` — bug fixes
     - `docs: update README examples` — documentation
     - `test: add sqlite purge coverage` — tests
     - `ci: update release workflow` — CI/CD
     - `chore: bump dependencies` — maintenance
   - Prefer one logical change per commit.

4. **Pull requests**
   - Open a PR against the default branch.
   - Describe the change and why it's needed.
   - Reference any related issues.

5. **Scope**
   - Keep changes focused. For larger work, consider splitting into multiple PRs.
   - New store drivers belong under `store/<driver>/` and must implement `durable.Store`.

## Releasing (maintainers only)

See **[RELEASING.md](RELEASING.md)** for how to cut releases — tag-triggered workflow, checklist, and version rules.

## Getting Help

| Need | Where |
|------|-------|
| **Feature idea or design discussion** | [Open an issue](https://github.com/agenticenv/durable-go/issues) (use Feature/Enhancement label if available) |
| **Bug report** | [Open an issue](https://github.com/agenticenv/durable-go/issues) with repro steps |
| **Question or general discussion** | [GitHub Discussions](https://github.com/agenticenv/durable-go/discussions) |
| **Security concern** | See [SECURITY.md](SECURITY.md) |

We follow typical open source flow: discuss in issues/discussions first for non-trivial changes, then implement and open a PR when ready.
