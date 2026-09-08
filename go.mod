module github.com/agenticenv/durable-go

go 1.26.5

// Retract v0.1.0 and v0.1.1: switched core storage engine to Journal-per-Task with Protobuf framing.
// Upgrade directly to v0.1.2 or later.
retract [v0.1.0, v0.1.1]

require (
	github.com/gofrs/flock v0.13.1
	github.com/oklog/ulid/v2 v2.1.2
	google.golang.org/protobuf v1.36.12
)

require golang.org/x/sys v0.47.0 // indirect
