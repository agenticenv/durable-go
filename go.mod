module github.com/agenticenv/durable-go

go 1.26.5

// Retract v0.1.0 through v0.1.2: v0.1.3 changed the step model to async
// RunStep + Get (matching RunTask) and rewrote the journal wire format
// (StepEntry dropped its seq field). Runs written by these versions are not
// forward-compatible. Upgrade directly to v0.1.3 or later on a clean journal.
retract [v0.1.0, v0.1.2]

require (
	github.com/gofrs/flock v0.13.1
	github.com/oklog/ulid/v2 v2.1.2
	google.golang.org/protobuf v1.36.12
	gopkg.in/yaml.v3 v3.0.1
)

require golang.org/x/sys v0.47.0 // indirect
