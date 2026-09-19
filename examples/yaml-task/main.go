// Package main runs a YAML workflow file as one durable-go task.
// Each steps[].id is a RunStep; the YAML is the task spec (passed as input).
//
//	go run .
//	CRASH_AFTER=2 go run .   # crash after step 2, then re-run to resume
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/examples/internal/exdir"
	"gopkg.in/yaml.v3"
)

type Workflow struct {
	Name  string `yaml:"name" json:"name"`
	Steps []Step `yaml:"steps" json:"steps"`
}

type Step struct {
	ID  string `yaml:"id" json:"id"`
	Run string `yaml:"run" json:"run"`
}

type WorkflowOutput struct {
	Name    string            `json:"name"`
	Results map[string]string `json:"results"`
}

var crashAfter = os.Getenv("CRASH_AFTER")

var runWorkflow = durable.Func(func(
	ctx context.Context,
	s *durable.StepRunner,
	wf Workflow,
) (WorkflowOutput, error) {
	results := make(map[string]string, len(wf.Steps))

	for i, step := range wf.Steps {
		step := step
		out, err := durable.RunStep(ctx, s, step.ID, step, func(ctx context.Context, step Step) (string, error) {
			log.Printf("  → [%s]  %s", step.ID, step.Run)
			cmd := exec.CommandContext(ctx, "sh", "-c", step.Run)
			var buf bytes.Buffer
			cmd.Stdout = &buf
			cmd.Stderr = &buf
			if err := cmd.Run(); err != nil {
				return "", fmt.Errorf("%s: %w\n%s", step.ID, err, buf.String())
			}
			result := string(bytes.TrimSpace(buf.Bytes()))
			log.Printf("  ✓ [%s]  %s", step.ID, result)
			return result, nil
		}).Get(ctx)
		if err != nil {
			return WorkflowOutput{}, err
		}
		results[step.ID] = out

		if crashAfter != "" && crashAfter == fmt.Sprintf("%d", i+1) {
			log.Println()
			log.Printf("💥  SIMULATED CRASH after step %s (%s)", crashAfter, step.ID)
			log.Println("    Re-run without CRASH_AFTER to resume remaining steps.")
			log.Println()
			os.Exit(1)
		}
	}

	return WorkflowOutput{Name: wf.Name, Results: results}, nil
})

func main() {
	ctx := context.Background()

	wf, err := loadWorkflow()
	if err != nil {
		log.Fatal(err)
	}

	e, err := durable.NewEngine(ctx, exdir.Data("yaml-task", "yaml-journal"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	if err := durable.RegisterTask(e, wf.Name, runWorkflow,
		durable.WithName(wf.Name),
	); err != nil {
		log.Fatal(err)
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("  YAML workflow %q — %d steps as one task\n", wf.Name, len(wf.Steps))
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	run := durable.RunTask[Workflow, WorkflowOutput](ctx, e, wf.Name, wf.Name, wf)
	out, err := run.Get(ctx)
	if err != nil {
		log.Fatalf("task failed: %v", err)
	}

	fmt.Printf("\n✅  %s done:", out.Name)
	for _, step := range wf.Steps {
		fmt.Printf(" %s=%s", step.ID, out.Results[step.ID])
	}
	fmt.Println()
}

func loadWorkflow() (Workflow, error) {
	p := filepath.Join(exdir.Dir("yaml-task"), "workflow.yaml")
	b, err := os.ReadFile(p)
	if err != nil {
		return Workflow{}, fmt.Errorf("read workflow.yaml: %w", err)
	}
	var wf Workflow
	if err := yaml.Unmarshal(b, &wf); err != nil {
		return Workflow{}, fmt.Errorf("%s: %w", p, err)
	}
	if err := validateWorkflow(wf); err != nil {
		return Workflow{}, fmt.Errorf("%s: %w", p, err)
	}
	return wf, nil
}

func validateWorkflow(wf Workflow) error {
	if wf.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(wf.Steps) == 0 {
		return fmt.Errorf("steps must not be empty")
	}
	seen := make(map[string]struct{}, len(wf.Steps))
	for i, step := range wf.Steps {
		if step.ID == "" {
			return fmt.Errorf("steps[%d]: id is required", i)
		}
		if step.Run == "" {
			return fmt.Errorf("steps[%d] %q: run is required", i, step.ID)
		}
		if _, dup := seen[step.ID]; dup {
			return fmt.Errorf("duplicate step id %q", step.ID)
		}
		seen[step.ID] = struct{}{}
	}
	return nil
}
