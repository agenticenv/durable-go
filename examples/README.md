# Examples

| Example | Style | What it shows |
|---|---|---|
| [`resume/`](./resume/) | closure | **Start here** — one `go run`: crash after step 2, resume from cache |
| [`func-task/`](./func-task/) | closure | `durable.Func` inline tasks, `RegisterTask` / `RunTask` / `RunStep` |
| [`struct-task/`](./struct-task/) | struct | Injected dependencies, `WithTaskTimeout`, `WithTaskMaxRetries` |
| [`fanout/`](./fanout/) | closure | Concurrent `RunStep`, first-of-N with `Done()`, `ErrStepPending` + `CompleteStep` |
| [`yaml-task/`](./yaml-task/) | YAML | One YAML file = one task; each `steps[].id` is a `RunStep` |
| [`payload-codec/`](./payload-codec/) | closure | Plaintext vs AES-GCM vs custom codec, HMAC tokens, journal MAC, inspect `--redact` |

This directory is its own module. From here:

```bash
go run ./resume/
go run ./func-task/
go run ./struct-task/
go run ./fanout/
go run ./yaml-task/
go run ./payload-codec/
```

Or `cd` into an example and `go run .`. Journals land under `<name>/.data/` (gitignored). `resume/` uses a temp dir and prints the path.
