# struct-task — Struct / Method Receiver Style

Demonstrates a struct with injected dependencies (`OpenAIClient`, `Database`)
that implements `durable.Task` directly via an `Exec` method.

Idiomatic pattern for production services where dependencies are constructed once
and reused across task runs. Also shows `WithTaskTimeout`, `WithTaskMaxRetries`,
`WithTag`, and `WithAutoPurge`.

## Run

```bash
go run .
```

## Reset

```bash
rm -rf examples/struct-task/.data/
```
