// Package main demonstrates the struct / method-receiver style of durable-go.
// AgentRunner is a production-style struct with injected dependencies.
// It implements durable.Task[AgentInput, AgentOutput] so it can be passed
// directly to RegisterTask.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"time"

	durable "github.com/agenticenv/durable-go"
)

type OpenAIClient struct{ Model string }

func (c *OpenAIClient) Complete(ctx context.Context, prompt string) (string, error) {
	log.Printf("[openai] completing prompt (model=%s) …", c.Model)
	return fmt.Sprintf("LLM response for: %q", prompt), nil
}

type Database struct{ DSN string }

func (db *Database) SaveMemory(ctx context.Context, userID, content string) error {
	log.Printf("[db] persisting memory for user=%s …", userID)
	return nil
}

func (db *Database) FetchContext(ctx context.Context, userID string) (string, error) {
	log.Printf("[db] fetching context for user=%s …", userID)
	return fmt.Sprintf("prior context for user %s", userID), nil
}

type AgentInput struct {
	UserID string
	Query  string
}

type AgentOutput struct {
	Response    string
	MemorySaved bool
}

type UserContext struct {
	UserID  string
	History string
}

type LLMCompletion struct {
	Response string
	Model    string
	Tokens   int
}

// AgentRunner implements durable.Task[AgentInput, AgentOutput].
type AgentRunner struct {
	AI *OpenAIClient
	DB *Database
}

func (r *AgentRunner) Exec(ctx context.Context, s *durable.StepRunner, in AgentInput) (AgentOutput, error) {
	uctx, err := durable.RunStep(ctx, s, "fetch-user-context", func(ctx context.Context) (UserContext, error) {
		history, err := r.DB.FetchContext(ctx, in.UserID)
		if err != nil {
			return UserContext{}, fmt.Errorf("fetch context: %w", err)
		}
		return UserContext{UserID: in.UserID, History: history}, nil
	}).Get(ctx)
	if err != nil {
		return AgentOutput{}, err
	}

	prompt := fmt.Sprintf("Context: %s\nQuery: %s", uctx.History, in.Query)
	completion, err := durable.RunStep(ctx, s, "run-llm-completion", func(ctx context.Context) (LLMCompletion, error) {
		resp, err := r.AI.Complete(ctx, prompt)
		if err != nil {
			return LLMCompletion{}, fmt.Errorf("llm completion: %w", err)
		}
		return LLMCompletion{Response: resp, Model: r.AI.Model, Tokens: len(resp)}, nil
	}).Get(ctx)
	if err != nil {
		return AgentOutput{}, err
	}

	_, err = durable.RunStep(ctx, s, "persist-memory", func(ctx context.Context) (struct{}, error) {
		if err := r.DB.SaveMemory(ctx, in.UserID, completion.Response); err != nil {
			return struct{}{}, fmt.Errorf("persist memory: %w", err)
		}
		return struct{}{}, nil
	}).Get(ctx)
	if err != nil {
		return AgentOutput{}, err
	}

	return AgentOutput{Response: completion.Response, MemorySaved: true}, nil
}

func main() {
	ctx := context.Background()

	e, err := durable.NewEngine(ctx, "examples/struct-task/.data/agent-journal",
		durable.WithLogger(slog.Default()),
		durable.WithAutoPurge(24*time.Hour),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = e.Close() }()

	runner := &AgentRunner{
		AI: &OpenAIClient{Model: "gpt-4o"},
		DB: &Database{DSN: "postgres://localhost/myapp"},
	}

	if err := durable.RegisterTask(e, "agent-run", runner,
		durable.WithName("Agent Run – User 99"),
		durable.WithTag("user_id", "99"),
		durable.WithTag("env", "production"),
		durable.WithTaskTimeout(30*time.Second),
		durable.WithTaskMaxRetries(2),
	); err != nil {
		log.Fatal(err)
	}

	run := durable.RunTask[AgentInput, AgentOutput](ctx, e, "agent-run", "agent-run-user-99-session-1", AgentInput{
		UserID: "99",
		Query:  "Summarise my last three orders.",
	})
	out, err := run.Get(ctx)
	if err != nil {
		log.Fatalf("agent task failed: %v", err)
	}

	log.Printf("agent done: memorySaved=%v response=%q", out.MemorySaved, out.Response)
}
