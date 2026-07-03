package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// toolOutcome is the result of executing one tool against the workspace.
type toolOutcome struct {
	output    string
	mutations []mutation
}

func executeTool(ctx context.Context, ws *workspace, call toolCall) (toolOutcome, error) {
	switch call.Tool {
	case "run_command":
		return ws.runCommand(ctx, call.Cmd)
	case "open_file":
		return ws.openFile(call.Path)
	case "edit_file":
		return ws.editFile(call.Path, call.Diff, call.Note)
	case "git_commit":
		return ws.gitCommit(ctx, call.Cmd, call.Note)
	case "wait_for_ci":
		return toolOutcome{
			output: "CI: go-test, go-vet, gosec — waiting",
			mutations: []mutation{
				{0, term("$ waiting for CI on pull request", cPrompt)},
				{2 * time.Second, func(s *scene) {
					s.ciPassing = true
					s.prURL = "PR — all checks passed"
				}},
				{0, term("CI: all checks passed", cGreen)},
			},
		}, nil
	case "finish_task":
		return toolOutcome{output: "task marked complete"}, nil
	default:
		return toolOutcome{}, fmt.Errorf("unknown tool %q", call.Tool)
	}
}

func (w *workspace) runCommand(ctx context.Context, cmd string) (toolOutcome, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if strings.TrimSpace(cmd) == "" {
		return toolOutcome{}, fmt.Errorf("empty command")
	}
	// Map demo paths to workspace-relative commands.
	cmd = strings.ReplaceAll(cmd, "payments-service/", "")
	cmd = strings.TrimPrefix(cmd, "cd payments-service && ")

	c := exec.CommandContext(ctx, "bash", "-lc", cmd)
	c.Dir = w.root
	c.Env = append(os.Environ(), "GOFLAGS=-mod=mod")
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	err := c.Run()
	out := strings.TrimSpace(stdout.String())
	if stderr.Len() > 0 {
		if out != "" {
			out += "\n"
		}
		out += strings.TrimSpace(stderr.String())
	}
	if out == "" && err == nil {
		out = "(no output)"
	}
	color := cText
	if err != nil {
		color = cRed
		if out == "" {
			out = err.Error()
		}
	} else if strings.Contains(out, "PASS") || strings.Contains(out, "ok  \t") {
		color = cGreen
	}

	muts := []mutation{
		{0, term("$ "+cmd, cPrompt)},
		{0, term(out, color)},
	}
	return toolOutcome{output: out, mutations: muts}, nil
}

func (w *workspace) openFile(path string) (toolOutcome, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	path = cleanPath(path)
	full := filepath.Join(w.root, path)
	body, err := os.ReadFile(full)
	if err != nil {
		return toolOutcome{}, err
	}
	lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
	muts := []mutation{{
		0, func(s *scene) {
			s.editorFile = path
			s.editorLines = lines
			s.editorHighlight = nil
			s.editorModified = false
		},
	}}
	return toolOutcome{output: fmt.Sprintf("opened %s (%d lines)", path, len(lines)), mutations: muts}, nil
}

func (w *workspace) editFile(path, diff, note string) (toolOutcome, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	path = cleanPath(path)
	full := filepath.Join(w.root, path)

	var newBody string
	switch {
	case note != "":
		newBody = note
	case strings.Contains(diff, "math.Round"):
		newBody = joinLines(invoiceFixed)
	case diff != "":
		// Minimal diff support: if diff mentions invoice fix, apply known good file.
		newBody = joinLines(invoiceFixed)
	default:
		return toolOutcome{}, fmt.Errorf("edit_file needs content or diff")
	}
	if err := os.WriteFile(full, []byte(newBody), 0o644); err != nil {
		return toolOutcome{}, err
	}
	lines := strings.Split(strings.TrimSuffix(newBody, "\n"), "\n")
	muts := []mutation{{
		0, func(s *scene) {
			s.editorFile = path
			s.editorLines = lines
			s.editorHighlight = map[int]bool{8: true, 9: true, 10: true, 11: true, 12: true, 13: true}
			s.editorModified = true
		},
	}}
	return toolOutcome{output: fmt.Sprintf("wrote %s (%d lines)", path, len(lines)), mutations: muts}, nil
}

func (w *workspace) gitCommit(ctx context.Context, cmd, message string) (toolOutcome, error) {
	if message == "" {
		message = "billing: apply discount at invoice level, round half-up"
	}
	branch := "fix/invoice-rounding"
	commands := []string{
		"git init -q",
		"git add -A",
		fmt.Sprintf("git checkout -B %s", branch),
		fmt.Sprintf("git -c user.email=agent@local -c user.name=agent commit -m %q", message),
	}
	var combined strings.Builder
	for _, c := range commands {
		out, err := w.runCommandLocked(ctx, c)
		combined.WriteString(out)
		if err != nil && !strings.Contains(out, "nothing to commit") {
			return toolOutcome{}, err
		}
	}
	muts := []mutation{
		{0, term("$ git checkout -b "+branch, cPrompt)},
		{0, term("$ git commit -am '"+message+"'", cPrompt)},
		{0, term("https://github.com/acme/payments-service/pull/2481", cText)},
		{0, func(s *scene) {
			s.ciVisible = true
			s.ciPassing = false
			s.prURL = "PR #2481 open — CI running"
		}},
	}
	return toolOutcome{output: combined.String(), mutations: muts}, nil
}

func (w *workspace) runCommandLocked(ctx context.Context, cmd string) (string, error) {
	c := exec.CommandContext(ctx, "bash", "-lc", cmd)
	c.Dir = w.root
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	err := c.Run()
	out := strings.TrimSpace(stdout.String() + "\n" + stderr.String())
	return out, err
}

func cleanPath(path string) string {
	path = strings.TrimPrefix(path, "./")
	path = strings.TrimPrefix(path, "payments-service/")
	return path
}
