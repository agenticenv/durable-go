module github.com/agenticenv/durable-go

go 1.26

toolchain go1.26.5

// Retract v0.1.0 through v0.1.3: v0.1.0–v0.1.2 rewrote the journal
// (async RunStep + dropped seq). v0.1.3 is the last release before
// typed step input and WithStepVersion. Start from v0.1.4.
retract [v0.1.0, v0.1.3]

require (
	github.com/gofrs/flock v0.13.1
	github.com/oklog/ulid/v2 v2.1.2
	google.golang.org/protobuf v1.36.12
)

require golang.org/x/sys v0.47.0 // indirect
