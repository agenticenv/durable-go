# durable-go
Durable task execution for Go — memoized steps with pluggable persistence (SQLite and more).

durable-go is a small Go library for durable task execution. Define typed tasks, run memoized steps through a StepRunner, and persist task/step state via a pluggable Store (including SQLite). Aimed at building restart-safe, agentic, and long-running workflows without a heavy orchestration framework.
