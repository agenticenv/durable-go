# Examples

| Example | Style | What it shows |
|---|---|---|
| [`resume/`](./resume/) | closure | **Start here** — one `go run`: crash after step 2, resume from cache |
| [`func-task/`](./func-task/) | closure | `durable.Func` inline tasks, `RegisterTask` / `RunTask` / `RunStep` |
| [`struct-task/`](./struct-task/) | struct | Injected dependencies, `WithTaskTimeout`, `WithTaskMaxRetries` |
| [`fanout/`](./fanout/) | closure | Concurrent `RunStep`, first-of-N with `Done()`, `ErrStepPending` + `CompleteStep` |
| [`yaml-task/`](./yaml-task/) | YAML | One YAML file = one task; each `steps[].id` is a `RunStep` |
| [`payload-codec/`](./payload-codec/) | closure | Plaintext vs AES-GCM vs custom codec, HMAC tokens, journal MAC, inspect `--redact` |

From the repo root:

```bash
go run ./examples/resume/
go run ./examples/func-task/
go run ./examples/struct-task/
go run ./examples/fanout/
go run ./examples/yaml-task/
go run ./examples/payload-codec/
```
