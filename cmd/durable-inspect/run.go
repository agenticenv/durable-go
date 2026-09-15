package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	durable "github.com/agenticenv/durable-go"
)

func run(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	p, err := parseArgs(args)
	if err != nil {
		fmt.Fprintln(stderr, err)
		fmt.Fprint(stderr, usage)
		return 2
	}
	if p.help || len(p.rest) == 0 {
		fmt.Fprint(stdout, usage)
		if p.help {
			return 0
		}
		return 2
	}

	if err := dispatch(p, stdout, getenv); err != nil {
		fmt.Fprintln(stderr, err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(stderr, usage)
			return 2
		}
		return 1
	}
	return 0
}

var errUsage = errors.New("invalid command")

func dispatch(p parsedArgs, stdout io.Writer, getenv func(string) string) error {
	if len(p.rest) < 2 {
		return fmt.Errorf("%w: want <task|step> <list|get> …", errUsage)
	}
	noun, verb := p.rest[0], p.rest[1]
	ids := p.rest[2:]

	if p.status != "" && !(noun == "task" && verb == "list") {
		return fmt.Errorf("%w: --status is only valid on task list", errUsage)
	}

	dir, err := resolveDir(p.dir, getenv)
	if err != nil {
		return err
	}

	ctx := context.Background()
	r, err := openRO(dir)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	switch noun + " " + verb {
	case "task list":
		if len(ids) != 0 {
			return fmt.Errorf("%w: task list takes no arguments", errUsage)
		}
		return cmdTaskList(ctx, r, stdout, p.status)
	case "task get":
		if len(ids) == 0 || len(ids) > 2 {
			return fmt.Errorf("%w: task get <taskID|runID|name> [runID]", errUsage)
		}
		return cmdTaskGet(ctx, r, stdout, ids)
	case "step list":
		if len(ids) != 2 {
			return fmt.Errorf("%w: step list <taskID> <runID>", errUsage)
		}
		return cmdStepList(ctx, r, stdout, ids[0], ids[1])
	case "step get":
		if len(ids) != 3 {
			return fmt.Errorf("%w: step get <taskID> <runID> <stepID>", errUsage)
		}
		return cmdStepGet(ctx, r, stdout, ids[0], ids[1], ids[2])
	default:
		return fmt.Errorf("%w: unknown command %q %q", errUsage, noun, verb)
	}
}

func openRO(dir string) (*durable.ReadOnlyEngine, error) {
	r, err := durable.NewReadOnlyEngine(dir, durable.WithROLockTimeout(500*time.Millisecond))
	if errors.Is(err, durable.ErrEngineLocked) {
		return nil, fmt.Errorf("journal %q is locked by a writer; stop that process or inspect a copy", dir)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func cmdTaskList(ctx context.Context, r *durable.ReadOnlyEngine, w io.Writer, status string) error {
	var filter []durable.TaskStatus
	if status != "" {
		st, err := parseStatus(status)
		if err != nil {
			return err
		}
		filter = []durable.TaskStatus{st}
	}
	tasks, err := r.ListTasks(ctx, filter...)
	if err != nil {
		return err
	}
	printTaskTable(w, tasks)
	return nil
}

func cmdTaskGet(ctx context.Context, r *durable.ReadOnlyEngine, w io.Writer, ids []string) error {
	all, err := r.ListTasks(ctx)
	if err != nil {
		return err
	}
	var matches []durable.TaskInfo
	if len(ids) == 2 {
		info, ok, err := r.GetTask(ctx, ids[0], ids[1])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("task %s / run %s not found", ids[0], ids[1])
		}
		matches = []durable.TaskInfo{info}
	} else {
		matches = matchRuns(all, ids[0])
		if len(matches) == 0 {
			return fmt.Errorf("no task matching %q (task ID, run ID, or name)", ids[0])
		}
		if len(matches) > 1 {
			fmt.Fprintf(w, "multiple runs match %q; pass task ID and run ID:\n", ids[0])
			printTaskTable(w, matches)
			return fmt.Errorf("multiple runs match %q", ids[0])
		}
	}
	return printTaskDetail(ctx, r, w, matches[0])
}

func matchRuns(all []durable.TaskInfo, q string) []durable.TaskInfo {
	var out []durable.TaskInfo
	for _, t := range all {
		if t.TaskID == q || t.RunID == q || t.Name == q {
			out = append(out, t)
		}
	}
	return out
}

func cmdStepList(ctx context.Context, r *durable.ReadOnlyEngine, w io.Writer, taskID, runID string) error {
	if _, ok, err := r.GetTask(ctx, taskID, runID); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("task %s / run %s not found", taskID, runID)
	}
	steps, err := r.LoadSteps(ctx, taskID, runID)
	if err != nil {
		return err
	}
	printStepTable(w, steps)
	return nil
}

func cmdStepGet(ctx context.Context, r *durable.ReadOnlyEngine, w io.Writer, taskID, runID, stepID string) error {
	rec, ok, err := r.GetStep(ctx, taskID, runID, stepID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("step %q not found on task %s / run %s", stepID, taskID, runID)
	}
	fmt.Fprintf(w, "STEP_ID:\t%s\n", rec.StepID)
	fmt.Fprintf(w, "STATUS:\t%s\n", rec.Status)
	fmt.Fprintf(w, "STARTED:\t%s\n", fmtTime(rec.StartedAt))
	fmt.Fprintf(w, "COMPLETED:\t%s\n", fmtTime(rec.CompletedAt))
	if rec.Error != "" {
		fmt.Fprintf(w, "ERROR:\t%s\n", rec.Error)
	}
	if rec.PanicTrace != "" {
		fmt.Fprintf(w, "PANIC:\t%s\n", rec.PanicTrace)
	}
	if len(rec.Result) > 0 {
		fmt.Fprintf(w, "RESULT:\t%s\n", fmtResult(rec.Result))
	}
	return nil
}

func printTaskDetail(ctx context.Context, r *durable.ReadOnlyEngine, w io.Writer, t durable.TaskInfo) error {
	fmt.Fprintf(w, "TASK_ID:\t%s\n", t.TaskID)
	fmt.Fprintf(w, "RUN_ID:\t%s\n", t.RunID)
	fmt.Fprintf(w, "NAME:\t%s\n", t.Name)
	fmt.Fprintf(w, "STATUS:\t%s\n", t.Status)
	fmt.Fprintf(w, "CREATED:\t%s\n", fmtTime(t.CreatedAt))
	fmt.Fprintf(w, "STARTED:\t%s\n", fmtTime(t.StartedAt))
	fmt.Fprintf(w, "COMPLETED:\t%s\n", fmtTime(t.CompletedAt))
	if t.Error != "" {
		fmt.Fprintf(w, "ERROR:\t%s\n", t.Error)
	}
	if len(t.Tags) > 0 {
		fmt.Fprintf(w, "TAGS:\t%v\n", t.Tags)
	}
	in, ok, err := r.LoadInput(ctx, t.TaskID, t.RunID)
	if err != nil {
		return err
	}
	if ok {
		fmt.Fprintf(w, "INPUT:\t%s\n", fmtResult(in))
	}
	steps, err := r.LoadSteps(ctx, t.TaskID, t.RunID)
	if err != nil {
		return err
	}
	if len(steps) == 0 {
		return nil
	}
	fmt.Fprintln(w)
	printStepTable(w, steps)
	return nil
}

func printTaskTable(w io.Writer, tasks []durable.TaskInfo) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TASK_ID\tRUN_ID\tNAME\tSTATUS\tCREATED")
	for _, t := range tasks {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", t.TaskID, t.RunID, t.Name, t.Status, fmtTime(t.CreatedAt))
	}
	_ = tw.Flush()
}

func printStepTable(w io.Writer, steps []durable.StepRecord) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "STEP_ID\tSTATUS\tSTARTED\tCOMPLETED")
	for _, s := range steps {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.StepID, s.Status, fmtTime(s.StartedAt), fmtTime(s.CompletedAt))
	}
	_ = tw.Flush()
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func fmtResult(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return fmt.Sprintf("%q", b)
}
