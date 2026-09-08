# Examples

| Example | Style | What it shows |
|---|---|---|
| [`resume/`](./resume/) | closure | **Start here** — crash recovery and step replay |
| [`func-task/`](./func-task/) | closure | `durable.Func` inline tasks, `RegisterTask` / `RunTask` / `RunStep` |
| [`struct-task/`](./struct-task/) | struct | Injected dependencies, `WithTaskTimeout`, `WithTaskMaxRetries` |

From the repo root:

```bash
go run ./examples/resume/
go run ./examples/func-task/
go run ./examples/struct-task/
```
