package model

import "time"

type State string

const (
	Queued     State = "queued"
	Starting   State = "starting"
	Activating State = "activating"
	Running    State = "running"
	Cancelling State = "cancelling"
	Succeeded  State = "succeeded"
	Failed     State = "failed"
	Cancelled  State = "cancelled"
)

func (s State) Terminal() bool { return s == Succeeded || s == Failed || s == Cancelled }

func (s State) Valid() bool {
	switch s {
	case Queued, Starting, Activating, Running, Cancelling, Succeeded, Failed, Cancelled:
		return true
	}
	return false
}

// CanTransition is the persisted lifecycle graph. Known-state self transitions
// are idempotent reads: they must not append an event or change the reason.
//
// Normal dispatch is queued -> starting -> activating -> running. Cancellation
// may enter cancelling from queued, starting, activating or running.
// Running work may succeed/fail; confirmed cleanup ends cancelling as cancelled
// or failed. Cancelling cannot return to execution, and terminal states cannot
// change to another state.
//
// Forward observation/recovery edges from queued, starting and activating to
// running/succeeded/failed deliberately preserve authoritative observations of
// an already-owned execution after downtime. These edges never submit or replay
// work. Activation intent must still be persisted before backend activation.
func CanTransition(from, to State) bool {
	if !from.Valid() || !to.Valid() {
		return false
	}
	if from == to {
		return true
	}
	switch from {
	case Queued:
		return to == Starting || to == Cancelling || to == Running || to == Succeeded || to == Failed
	case Starting:
		return to == Activating || to == Cancelling || to == Running || to == Succeeded || to == Failed
	case Activating:
		return to == Running || to == Cancelling || to == Succeeded || to == Failed
	case Running:
		return to == Cancelling || to == Succeeded || to == Failed
	case Cancelling:
		return to == Cancelled || to == Failed
	}
	return false
}

type Resources struct {
	CPUMilli  int64 `json:"cpu_milli"`
	MemoryMiB int64 `json:"memory_mib"`
	DiskMiB   int64 `json:"disk_mib"`
}

// RuntimeProfile is resolved from administrator configuration, never caller
// input. Its fields are persisted with the run and included in idempotency.
// A nil profile on an older Spec means the original gVisor/runsc contract.
type RuntimeProfile struct {
	Kind            string `json:"kind"`
	RuntimeClass    string `json:"runtime_class"`
	RequiredHandler string `json:"required_handler"`
}

// Spec is resolved from an administrator-owned template, never a caller PodSpec.
type Spec struct {
	Template       string          `json:"template"`
	Image          string          `json:"image"`
	RuntimeClass   string          `json:"runtime_class"`
	RuntimeProfile *RuntimeProfile `json:"runtime_profile,omitempty"`
	Command        []string        `json:"command"`
	Resources      Resources       `json:"resources"`
	TimeoutSeconds int64           `json:"timeout_seconds"`
}

type CreateRequest struct {
	Template       string   `json:"template"`
	Command        []string `json:"command,omitempty"`
	TimeoutSeconds int64    `json:"timeout_seconds,omitempty"`
}

type Event struct {
	At     time.Time `json:"at"`
	State  State     `json:"state"`
	Reason string    `json:"reason,omitempty"`
}

type Record struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Spec      Spec      `json:"spec"`
	State     State     `json:"state"`
	Reason    string    `json:"reason,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Events    []Event   `json:"events"`
}
