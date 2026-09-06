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

- Security issues in the durable-go engine (`Run`, `Step`, task lifecycle)
- Persistence drivers under `store/` (e.g. SQLite)
- Sensitive data exposure in persisted task or step records

## Security Considerations

### Task and step payloads

Step results are JSON-marshalled and stored. Do not put secrets, API keys, or credentials in task inputs or step outputs.

### SQLite files

The SQLite driver writes a local database file. Restrict filesystem permissions on that file. Do not commit `*.db` files or `examples/**/.data/` directories.

### Third-party dependencies

Monitor via **Dependabot** (`.github/dependabot.yml`) and **`govulncheck`** (`task govuln`, Security workflow, and Release hard-fail):

- modernc.org/sqlite

## Out of Scope

- Callers' application logic inside task or step closures
- External services invoked from user steps
- General usage questions
