// Command durable-inspect is a read-only viewer for a durable-go journal.
//
//	durable-inspect -d ./data task list
//	durable-inspect -d ./data task get <taskID|runID|name> [runID]
//	durable-inspect -d ./data step list <taskID> <runID>
//	durable-inspect -d ./data step get <taskID> <runID> <stepID>
//
// --dir / -d wins over DURABLE_DIR. The journal must not be held by a writer.
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}
