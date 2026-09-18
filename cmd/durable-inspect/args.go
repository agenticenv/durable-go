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
  -d, --dir string         journal data directory (overrides DURABLE_DIR)
      --payload-key string AES-GCM key: 16/24/32 raw bytes, or hex of that
                           (overrides DURABLE_PAYLOAD_KEY; prefer the env var)
      --journal-mac-key string HMAC key: raw bytes, or hex of the key
                           (overrides DURABLE_JOURNAL_MAC_KEY; prefer the env var)
      --redact             hide task/step INPUT and RESULT ([redacted])

DURABLE_DIR is used when --dir / -d is omitted.
Set DURABLE_PAYLOAD_KEY and DURABLE_JOURNAL_MAC_KEY in the environment
(flags put secrets on the command line). Hex is tried first for both keys.
STATUS is one of: running, waiting, completed, failed.

The journal is opened read-only. If a writer holds the exclusive lock,
this command fails — stop that process or inspect a copy.
`

type parsedArgs struct {
	dir           string
	status        string
	payloadKey    string
	journalMACKey string
	redact        bool
	help          bool
	rest          []string
}

func parseArgs(args []string) (parsedArgs, error) {
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
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
		case a == "--payload-key":
			if i+1 >= len(args) {
				return p, fmt.Errorf("%s requires a value", a)
			}
			i++
			p.payloadKey = args[i]
		case strings.HasPrefix(a, "--payload-key="):
			p.payloadKey = strings.TrimPrefix(a, "--payload-key=")
		case a == "--journal-mac-key":
			if i+1 >= len(args) {
				return p, fmt.Errorf("%s requires a value", a)
			}
			i++
			p.journalMACKey = args[i]
		case strings.HasPrefix(a, "--journal-mac-key="):
			p.journalMACKey = strings.TrimPrefix(a, "--journal-mac-key=")
		case a == "--redact":
			p.redact = true
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

func resolvePayloadKey(flagKey string, getenv func(string) string) string {
	if flagKey != "" {
		return flagKey
	}
	if getenv != nil {
		return getenv("DURABLE_PAYLOAD_KEY")
	}
	return ""
}

func resolveJournalMACKey(flagKey string, getenv func(string) string) string {
	if flagKey != "" {
		return flagKey
	}
	if getenv != nil {
		return getenv("DURABLE_JOURNAL_MAC_KEY")
	}
	return ""
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
