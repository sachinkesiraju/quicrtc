package main

import (
	"context"
	"fmt"
)

// llmConfig configures real Anthropic worker brains.
type llmConfig struct {
	Model    string
	MaxTurns int
}

// workerTurn is one LLM decision: reasoning text, a tool call, and
// whether this worker's specialist task is complete.
type workerTurn struct {
	Title     string
	Reasoning string
	Call      toolCall
	Done      bool
}

// WorkerBrain decides the next tool call for one parallel specialist.
// Implementations must not be called concurrently for the same worker.
type WorkerBrain interface {
	NextStep(ctx context.Context, snap sceneSnapshot, steer []string) (workerTurn, error)
	Close() error
}

func workerGoal(id workerID) string {
	switch id {
	case workerTests:
		return `You are the tests specialist on a shared payments-service workspace.
Your job: run the Go test suite, reproduce TestInvoiceTotalWithDiscount, and confirm when tests pass after the code fix.
Use run_command for shell commands. Call finish_task when your part is done.`
	case workerCode:
		return `You are the code specialist on a shared payments-service workspace.
Your job: open billing/invoice.go, fix the per-line discount truncation bug (apply discount at invoice subtotal, round half-up once), and verify with go test ./billing.
Use open_file, edit_file, and run_command. Call finish_task when the fix is in place and billing tests pass.`
	case workerShip:
		return `You are the ship specialist on a shared payments-service workspace.
Your job: commit the fix on a branch, simulate opening a PR, and mark CI green once tests pass.
Use run_command and git_commit. Call finish_task when the PR is ready.`
	default:
		return fmt.Sprintf("Worker %s on the invoice rounding task.", id)
	}
}

func workerTools(id workerID) []string {
	switch id {
	case workerTests:
		return []string{"run_command", "finish_task"}
	case workerCode:
		return []string{"open_file", "edit_file", "run_command", "finish_task"}
	case workerShip:
		return []string{"run_command", "git_commit", "finish_task"}
	default:
		return []string{"run_command", "finish_task"}
	}
}
