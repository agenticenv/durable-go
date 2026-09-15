package main

import (
	"fmt"
	"strings"

	durable "github.com/agenticenv/durable-go"
)

const usage = `Usage:
  durable-inspect [flags] task list [--status STATUS]
  durable-inspect [flags] task get <taskID|runID|name> [runID]
  durable-inspect [flags] step list <taskID> <runID>
  durable-inspect [flags] step get <taskID> <runID> <stepID>

Flags:
  -d, --dir string   journal data directory (overrides DURABLE_DIR)

DURABLE_DIR is used when --dir / -d is omitted.
STATUS is one of: running, waiting, completed, failed.

The journal is opened read-only. If a writer holds the exclusive lock,
this command fails — stop that process or inspect a copy.
`

type parsedArgs struct {
	dir    string
	status string
	help   bool
	rest   []string
}

func parseArgs(args []string) (parsedArgs, error) {
	var p parsedArgs
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-h" || a == "--help":
			p.help = true
		case a == "-d" || a == "--dir":
			if i+1 >= len(args) {
				return p, fmt.Errorf("%s requires a value", a)
			}
			i++
			p.dir = args[i]
		case strings.HasPrefix(a, "-d="):
			p.dir = strings.TrimPrefix(a, "-d=")
		case strings.HasPrefix(a, "--dir="):
			p.dir = strings.TrimPrefix(a, "--dir=")
		case a == "--status":
			if i+1 >= len(args) {
				return p, fmt.Errorf("--status requires a value")
			}
			i++
			p.status = args[i]
		case strings.HasPrefix(a, "--status="):
			p.status = strings.TrimPrefix(a, "--status=")
		case a == "--":
			p.rest = append(p.rest, args[i+1:]...)
			return p, nil
		case strings.HasPrefix(a, "-"):
			return p, fmt.Errorf("unknown flag %s", a)
		default:
			p.rest = append(p.rest, a)
		}
	}
	return p, nil
}

func resolveDir(flagDir string, getenv func(string) string) (string, error) {
	if flagDir != "" {
		return flagDir, nil
	}
	if getenv != nil {
		if env := getenv("DURABLE_DIR"); env != "" {
			return env, nil
		}
	}
	return "", fmt.Errorf("journal directory required: pass --dir / -d or set DURABLE_DIR")
}

func parseStatus(s string) (durable.TaskStatus, error) {
	st := durable.TaskStatus(s)
	switch st {
	case durable.StatusRunning, durable.StatusWaiting, durable.StatusCompleted, durable.StatusFailed:
		return st, nil
	default:
		return "", fmt.Errorf("unknown status %q (want running, waiting, completed, failed)", s)
	}
}
