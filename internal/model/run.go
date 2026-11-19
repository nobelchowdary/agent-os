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

type Resources struct {
	CPUMilli  int64 `json:"cpu_milli"`
	MemoryMiB int64 `json:"memory_mib"`
	DiskMiB   int64 `json:"disk_mib"`
}

// Spec is resolved from an administrator-owned template, never a caller PodSpec.
type Spec struct {
	Template       string    `json:"template"`
	Image          string    `json:"image"`
	RuntimeClass   string    `json:"runtime_class"`
	Command        []string  `json:"command"`
	Resources      Resources `json:"resources"`
	TimeoutSeconds int64     `json:"timeout_seconds"`
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
