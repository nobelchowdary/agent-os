package backend

import (
	"context"
	"errors"
	"github.com/nobelchowdary/agent-os/internal/model"
)

var ErrNotFound = errors.New("execution not found")

// ErrPending is normal asynchronous progress, not a failed backend operation.
// Keep the durable reservation and reconcile again until cleanup is observed.
var ErrPending = errors.New("execution operation is pending")

type Observation struct {
	State      model.State
	Reason     string
	ReasonCode model.ReasonCode
}

// ResultCapturer is an optional runtime capability. Output is untrusted data;
// an unavailable/partial result must never be mistaken for an observed exit.
type ResultCapturer interface {
	CaptureResult(context.Context, string) (model.ExecutionResult, error)
}

// Execution identifies a managed-label Job for reconciliation. Inventory is
// not authority to adopt it: callers must match a durable record, then Cleanup
// validates the current Job against that record before requesting deletion.
type Execution struct {
	ID       string
	SpecHash string
	Terminal bool
}

type JobInventory interface {
	ListExecutions(context.Context) ([]Execution, error)
	Cleanup(context.Context, model.Record) error
}

// Backend operations must be idempotent for a deterministic run ID.
type Backend interface {
	Ready(context.Context) error
	Submit(context.Context, model.Record) error
	// Activate never creates compute. Persist activation intent before calling it.
	Activate(context.Context, model.Record) error
	Observe(context.Context, string) (Observation, error)
	Cancel(context.Context, string) error
	Logs(context.Context, string, int64) (string, error)
}
