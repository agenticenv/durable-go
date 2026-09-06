// Package main demonstrates the struct / method-receiver style of durable-go.
// AgentRunner is a production-style struct with injected dependencies.
// It implements durable.Task[AgentInput, AgentOutput] so it can be passed
// directly to durable.Run – no adapter or wrapper needed.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	durable "github.com/agenticenv/durable-go"
	"github.com/agenticenv/durable-go/store/journal"
)

// --- injected dependency stubs (replace with real clients in production) ---

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

// --- domain types ---

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

// --- task implementation as a struct ---

// AgentRunner implements durable.Task[AgentInput, AgentOutput].
// Dependencies are injected at construction time; steps capture them via the receiver.
type AgentRunner struct {
	AI *OpenAIClient
	DB *Database
}

// Exec implements durable.Task. Each logical operation is wrapped in a durable.Step
// so that crashes after any step are recovered transparently on the next run.
func (r *AgentRunner) Exec(ctx context.Context, s *durable.StepRunner, in AgentInput) (AgentOutput, error) {

	// Step 1 – fetch user context from the database.
	uctx, err := durable.Step(ctx, s, "fetch-user-context", func(ctx context.Context) (UserContext, error) {
		history, err := r.DB.FetchContext(ctx, in.UserID)
		if err != nil {
			return UserContext{}, fmt.Errorf("fetch context: %w", err)
		}
		return UserContext{UserID: in.UserID, History: history}, nil
	})
	if err != nil {
		return AgentOutput{}, err
	}

	// Step 2 – run LLM completion.
	// On replay the API is NOT called again; the cached completion is returned as-is.
	prompt := fmt.Sprintf("Context: %s\nQuery: %s", uctx.History, in.Query)
	completion, err := durable.Step(ctx, s, "run-llm-completion", func(ctx context.Context) (LLMCompletion, error) {
		resp, err := r.AI.Complete(ctx, prompt)
		if err != nil {
			return LLMCompletion{}, fmt.Errorf("llm completion: %w", err)
		}
		return LLMCompletion{Response: resp, Model: r.AI.Model, Tokens: len(resp)}, nil
	})
	if err != nil {
		return AgentOutput{}, err
	}

	// Step 3 – persist the response as a memory entry.
	_, err = durable.Step(ctx, s, "persist-memory", func(ctx context.Context) (struct{}, error) {
		if err := r.DB.SaveMemory(ctx, in.UserID, completion.Response); err != nil {
			return struct{}{}, fmt.Errorf("persist memory: %w", err)
		}
		return struct{}{}, nil
	})
	if err != nil {
		return AgentOutput{}, err
	}

	return AgentOutput{Response: completion.Response, MemorySaved: true}, nil
}

func main() {
	ctx := context.Background()

	if err := os.MkdirAll("examples/struct-task/.data", 0o755); err != nil {
		log.Fatal(err)
	}
	store, err := journal.NewJournalStore("examples/struct-task/.data/agent-journal", journal.WithLogger(slog.Default()))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	client, err := durable.NewClient(ctx, store, durable.WithAutoPurge(24*time.Hour))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	runner := &AgentRunner{
		AI: &OpenAIClient{Model: "gpt-4o"},
		DB: &Database{DSN: "postgres://localhost/myapp"},
	}

	handle := client.NewTask(
		"agent-run-user-99-session-1",
		durable.WithName("Agent Run – User 99"),
		durable.WithTag("user_id", "99"),
		durable.WithTag("env", "production"),
		durable.WithTimeout(30*time.Second),
		durable.WithMaxRetries(2),
	)

	out, err := durable.Run(ctx, handle, AgentInput{
		UserID: "99",
		Query:  "Summarise my last three orders.",
	}, runner)
	if err != nil {
		log.Fatalf("agent task failed: %v", err)
	}

	log.Printf("agent done: memorySaved=%v response=%q", out.MemorySaved, out.Response)
}
