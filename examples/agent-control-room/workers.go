package main

// Three parallel sub-agents split the invoice-fixing task the way a
// production orchestrator would fan work across specialists.
type workerID string

const (
	workerTests workerID = "tests"
	workerCode  workerID = "code"
	workerShip  workerID = "ship"
)

// workerTool is the JSON on the toolcalls lane — one bidi stream per AU.
type workerTool struct {
	Worker string `json:"worker"`
	toolCall
	State string `json:"state,omitempty"` // running | done | cancelled
}

// workerPlan is one specialist's slice of the scripted session.
type workerPlan struct {
	id    workerID
	steps []step
}

func workerPlans() []workerPlan {
	all := script()
	return []workerPlan{
		{
			id: workerTests,
			steps: []step{
				all[0], // clone + test
				all[1], // reproduce failure
				all[4], // verify fix
			},
		},
		{
			id: workerCode,
			steps: []step{
				all[2], // read code
				all[3], // patch
			},
		},
		{
			id: workerShip,
			steps: []step{
				all[5], // commit + PR
				all[6], // CI
			},
		},
	}
}
