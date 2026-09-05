# durable-go
Durable task execution for Go with memoized steps and pluggable persistence.

durable-go is a Go library for durable task execution. Define typed tasks, run memoized steps, and persist progress so work can resume safely after failures or restarts. Useful for any Go app that needs reliable, resumable workflows without a heavy orchestration framework.
