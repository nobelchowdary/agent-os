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

// ReasonCode describes an observed outcome or a persisted cleanup intent.
// Policy and budget codes are reserved; this alpha has no such enforcement.
type ReasonCode string

const (
	ReasonCompleted        ReasonCode = "completed"
	ReasonTaskError        ReasonCode = "task_error"
	ReasonDeadlineExceeded ReasonCode = "deadline_exceeded"
	ReasonCancelled        ReasonCode = "cancelled"
	ReasonOOMKilled        ReasonCode = "oom_killed"
	ReasonEvicted          ReasonCode = "evicted"
	ReasonImagePullFailed  ReasonCode = "image_pull_failed"
	ReasonPolicyDenied     ReasonCode = "policy_denied"
	ReasonBudgetExceeded   ReasonCode = "budget_exceeded"
	ReasonInfraFailure     ReasonCode = "infra_failure"
	ReasonUnknown          ReasonCode = "unknown"
)

func (code ReasonCode) Valid() bool {
	switch code {
	case ReasonCompleted, ReasonTaskError, ReasonDeadlineExceeded, ReasonCancelled,
		ReasonOOMKilled, ReasonEvicted, ReasonImagePullFailed, ReasonPolicyDenied,
		ReasonBudgetExceeded, ReasonInfraFailure, ReasonUnknown:
		return true
	}
	return false
}

// DefaultReasonCode supplies only what a terminal state establishes. In
// particular, an unspecified failed outcome is not evidence of a task error.
func DefaultReasonCode(state State) ReasonCode {
	switch state {
	case Succeeded:
		return ReasonCompleted
	case Cancelled:
		return ReasonCancelled
	case Failed:
		return ReasonUnknown
	}
	return ""
}

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
	At         time.Time  `json:"at"`
	State      State      `json:"state"`
	Reason     string     `json:"reason,omitempty"`
	ReasonCode ReasonCode `json:"reason_code,omitempty"`
}

// ExecutionResult is an administrator-captured attachment, never caller input.
// Output combines stdout/stderr. A tail is the last scanned bytes; it is the
// final tail only when OutputComplete is true. Expiry removes output while
// retaining observed exit status and capture metadata.
type ExecutionResult struct {
	ExitCode        *int32    `json:"exit_code,omitempty"`
	OutputHead      string    `json:"output_head"`
	OutputTail      string    `json:"output_tail"`
	OutputTruncated bool      `json:"output_truncated"`
	OutputComplete  bool      `json:"output_complete"`
	Status          string    `json:"status"`
	CapturedAt      time.Time `json:"captured_at"`
	ExpiresAt       time.Time `json:"expires_at"`
}

type Record struct {
	ID         string           `json:"id"`
	TenantID   string           `json:"tenant_id"`
	Spec       Spec             `json:"spec"`
	State      State            `json:"state"`
	Reason     string           `json:"reason,omitempty"`
	ReasonCode ReasonCode       `json:"reason_code,omitempty"`
	CreatedAt  time.Time        `json:"created_at"`
	UpdatedAt  time.Time        `json:"updated_at"`
	Events     []Event          `json:"events"`
	Result     *ExecutionResult `json:"result,omitempty"`
}
