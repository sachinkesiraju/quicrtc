package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	anthropicURL     = "https://api.anthropic.com/v1/messages"
	anthropicVersion = "2023-06-01"
	defaultLLMModel  = "claude-sonnet-4-20250514"
)

type anthropicWorkerBrain struct {
	id       workerID
	cfg      llmConfig
	apiKey   string
	http     *http.Client
	system   string
	tools    []codingTool
	history  []apiMessage
	pending  string
	started  bool
	turn     int
	maxTurns int
}

type codingTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"input_schema"`
}

func newAnthropicWorkerBrain(id workerID, cfg llmConfig) (WorkerBrain, error) {
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("ANTHROPIC_API_KEY is not set (required for -llm)")
	}
	if cfg.Model == "" {
		cfg.Model = defaultLLMModel
	}
	maxTurns := cfg.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 8
	}
	return &anthropicWorkerBrain{
		id:       id,
		cfg:      cfg,
		apiKey:   apiKey,
		http:     &http.Client{Timeout: 120 * time.Second},
		system:   workerGoal(id),
		tools:    codingToolsFor(id),
		maxTurns: maxTurns,
	}, nil
}

func (b *anthropicWorkerBrain) Close() error { return nil }

func (b *anthropicWorkerBrain) NextStep(ctx context.Context, snap sceneSnapshot, steer []string) (workerTurn, error) {
	if b.turn >= b.maxTurns {
		return workerTurn{Done: true, Reasoning: "max turns reached"}, nil
	}
	b.turn++

	if !b.started {
		prompt := scenePrompt(snap) + "\n\nBegin your specialist work. Call exactly one tool."
		if len(steer) > 0 {
			prompt += "\n\nHuman steer:\n- " + strings.Join(steer, "\n- ")
		}
		b.history = append(b.history, apiMessage{Role: "user", Content: []apiContent{{Type: "text", Text: prompt}}})
		b.started = true
	} else if b.pending != "" {
		// tool_result appended by RecordToolResult before next NextStep
	}

	resp, err := b.callAPI(ctx)
	if err != nil {
		return workerTurn{}, err
	}
	b.history = append(b.history, apiMessage{Role: "assistant", Content: resp.Content})

	var reasoning strings.Builder
	var call toolCall
	haveTool := false
	for _, c := range resp.Content {
		switch c.Type {
		case "text":
			reasoning.WriteString(c.Text)
		case "tool_use":
			b.pending = c.ID
			if tc, ok := toolUseToCodingCall(c.Name, c.Input); ok {
				call = tc
				haveTool = true
			}
		}
	}

	turn := workerTurn{
		Title:     snap.StepTitle,
		Reasoning: strings.TrimSpace(reasoning.String()),
		Call:      call,
	}
	if call.Tool == "finish_task" || resp.StopReason == "end_turn" && !haveTool {
		turn.Done = true
	}
	if haveTool {
		turn.Title = fmt.Sprintf("[%s] %s", b.id, call.Tool)
	}
	return turn, nil
}

// RecordToolResult appends the tool execution output for the next API turn.
func (b *anthropicWorkerBrain) RecordToolResult(output string) {
	if b.pending == "" {
		return
	}
	b.history = append(b.history, apiMessage{Role: "user", Content: []apiContent{{
		Type:      "tool_result",
		ToolUseID: b.pending,
		Content:   []apiContent{{Type: "text", Text: output}},
	}}})
	b.pending = ""
}

func (b *anthropicWorkerBrain) callAPI(ctx context.Context) (apiResponse, error) {
	reqBody := map[string]any{
		"model":      b.cfg.Model,
		"max_tokens": 1024,
		"system":     b.system,
		"tools":      b.tools,
		"messages":   b.history,
	}
	raw, err := json.Marshal(reqBody)
	if err != nil {
		return apiResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, anthropicURL, bytes.NewReader(raw))
	if err != nil {
		return apiResponse{}, err
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("x-api-key", b.apiKey)
	req.Header.Set("anthropic-version", anthropicVersion)

	httpResp, err := b.http.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer httpResp.Body.Close()
	body, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode != http.StatusOK {
		return apiResponse{}, fmt.Errorf("anthropic API %d: %s", httpResp.StatusCode, truncateAPI(string(body), 400))
	}
	var resp apiResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return apiResponse{}, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

type apiMessage struct {
	Role    string       `json:"role"`
	Content []apiContent `json:"content"`
}

type apiContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
	ToolUseID string       `json:"tool_use_id,omitempty"`
	Content   []apiContent `json:"content,omitempty"`
}

type apiResponse struct {
	Content    []apiContent `json:"content"`
	StopReason string       `json:"stop_reason"`
}

func codingToolsFor(id workerID) []codingTool {
	all := map[string]codingTool{
		"run_command": {
			Name:        "run_command",
			Description: "Run a shell command in the workspace root",
			InputSchema: objSchema(map[string]any{
				"cmd": prop("string", "Shell command to execute"),
			}, "cmd"),
		},
		"open_file": {
			Name:        "open_file",
			Description: "Read a file into the editor",
			InputSchema: objSchema(map[string]any{
				"path": prop("string", "Relative path e.g. billing/invoice.go"),
			}, "path"),
		},
		"edit_file": {
			Name:        "edit_file",
			Description: "Write a file (full content in content field)",
			InputSchema: objSchema(map[string]any{
				"path":    prop("string", "Relative path"),
				"content": prop("string", "Full new file contents"),
			}, "path", "content"),
		},
		"git_commit": {
			Name:        "git_commit",
			Description: "Commit changes and open a PR",
			InputSchema: objSchema(map[string]any{
				"message": prop("string", "Commit message"),
			}, "message"),
		},
		"finish_task": {
			Name:        "finish_task",
			Description: "Call when your specialist task is complete",
			InputSchema: objSchema(map[string]any{
				"summary": prop("string", "One-line summary of what you finished"),
			}, "summary"),
		},
	}
	var out []codingTool
	for _, name := range workerTools(id) {
		if t, ok := all[name]; ok {
			out = append(out, t)
		}
	}
	return out
}

func objSchema(props map[string]any, required ...string) map[string]any {
	return map[string]any{
		"type":       "object",
		"properties": props,
		"required":   required,
	}
}

func prop(typ, desc string) map[string]any {
	return map[string]any{"type": typ, "description": desc}
}

func toolUseToCodingCall(name string, input json.RawMessage) (toolCall, bool) {
	var raw map[string]string
	if err := json.Unmarshal(input, &raw); err != nil {
		return toolCall{}, false
	}
	switch name {
	case "run_command":
		return toolCall{Tool: "run_command", Cmd: raw["cmd"]}, true
	case "open_file":
		return toolCall{Tool: "open_file", Path: raw["path"]}, true
	case "edit_file":
		return toolCall{Tool: "edit_file", Path: raw["path"], Note: raw["content"]}, true
	case "git_commit":
		return toolCall{Tool: "git_commit", Note: raw["message"]}, true
	case "finish_task":
		return toolCall{Tool: "finish_task", Note: raw["summary"]}, true
	default:
		return toolCall{}, false
	}
}

func scenePrompt(snap sceneSnapshot) string {
	return fmt.Sprintf(`Current UI state:
- step: %s
- terminal lines: %d
- editor: %s
- ci visible: %v passing: %v
- pr: %s`,
		snap.StepTitle, snap.TermLines, snap.EditorFile, snap.CIVisible, snap.CIPassing, snap.PRURL)
}

func truncateAPI(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// brainWithResult extends WorkerBrain with tool-result feedback.
type brainWithResult interface {
	WorkerBrain
	RecordToolResult(output string)
}

func newWorkerBrains(cfg llmConfig) (map[workerID]WorkerBrain, error) {
	brains := make(map[workerID]WorkerBrain)
	for _, id := range []workerID{workerTests, workerCode, workerShip} {
		b, err := newAnthropicWorkerBrain(id, cfg)
		if err != nil {
			return nil, err
		}
		brains[id] = b
	}
	return brains, nil
}
