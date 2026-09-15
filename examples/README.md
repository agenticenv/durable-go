# Examples

| Example | Style | What it shows |
|---|---|---|
| [`resume/`](./resume/) | closure | **Start here** — crash recovery and step replay |
| [`func-task/`](./func-task/) | closure | `durable.Func` inline tasks, `RegisterTask` / `RunTask` / `RunStep` |
| [`struct-task/`](./struct-task/) | struct | Injected dependencies, `WithTaskTimeout`, `WithTaskMaxRetries` |
| [`fanout/`](./fanout/) | closure | Concurrent `RunStep`, first-of-N with `Done()`, `ErrStepPending` + `CompleteStep` |
| [`yaml-task/`](./yaml-task/) | YAML | One YAML file = one task; each `steps[].id` is a `RunStep` |

From the repo root:

```bash
go run ./examples/resume/
go run ./examples/func-task/
go run ./examples/struct-task/
go run ./examples/fanout/
go run ./examples/yaml-task/
```
