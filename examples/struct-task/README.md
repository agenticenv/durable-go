# Agent Example — Struct / Method Receiver Style

Demonstrates `AgentRunner`, a struct with injected dependencies (`OpenAIClient`, `Database`)
that implements `durable.Task` directly via an `Exec` method.

This is the idiomatic pattern for production services and local AI agents where
dependencies are constructed once and reused across task runs.

## Run

```bash
go run .
```

## Reset

```bash
rm examples/agent/.data/agent.db
```
