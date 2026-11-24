package controller

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

// Controller has one owner per database. Kubernetes Jobs use deterministic IDs;
// uncertain creates are retried without creating a second logical execution.
type Controller struct {
	Store             *store.Store
	Backend           backend.Backend
	Logger            *slog.Logger
	StartupTimeout    time.Duration
	ResultRetention   time.Duration
	ResultCaptureWait time.Duration
	SweepInterval     time.Duration

	statusMu    sync.RWMutex
	status      Status
	sweepMu     sync.Mutex
	nextSweep   time.Time
	sweepOffset int
}

// Status describes the controller loop, not node isolation or workload health.
// A successful pass means every observed run reconciled without an operation
// error. Per-run errors are diagnostic; they do not stop reads or cancellation.
type Status struct {
	Running                bool      `json:"running"`
	LastCompletedAt        time.Time `json:"last_completed_at,omitzero"`
	LastProgressAt         time.Time `json:"last_progress_at,omitzero"`
	LastSuccessAt          time.Time `json:"last_success_at,omitzero"`
	ConsecutiveFailures    uint64    `json:"consecutive_failures"`
	RunErrors              int       `json:"run_errors"`
	MaintenanceFailed      bool      `json:"maintenance_failed"`
	OrphanJobs             int       `json:"orphan_jobs"`
	TerminalCleanupPending int       `json:"terminal_cleanup_pending"`
	LastSweepAt            time.Time `json:"last_sweep_at,omitzero"`
}

// Status returns an independent snapshot and is safe during reconciliation.
func (c *Controller) Status() Status {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

func (c *Controller) Run(ctx context.Context) {
	c.run(ctx, time.Second)
}

func (c *Controller) run(ctx context.Context, interval time.Duration) {
	c.statusMu.Lock()
	if c.status.Running || ctx.Err() != nil {
		c.statusMu.Unlock()
		return
	}
	c.status.Running = true
	c.statusMu.Unlock()
	defer func() {
		c.statusMu.Lock()
		c.status.Running = false
		c.statusMu.Unlock()
	}()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		// Capture the current generation before reading runs. A commit during
		// reconciliation closes this channel and schedules another pass.
		changed := c.Store.Changed()
		if err := c.Reconcile(ctx); err != nil && ctx.Err() == nil {
			c.logger().Error("reconciliation failed", "category", "reconciliation_failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		case <-t.C:
		}
	}
}

// operationError deliberately excludes backend and database error text from its
// display form. Callers may still inspect the cause with errors.Is/errors.As.
type operationError struct {
	category string
	runID    string
	cause    error
}

func (e *operationError) Error() string {
	if e.runID == "" {
		return e.category
	}
	return e.category + ": " + e.runID
}

func (e *operationError) Unwrap() error { return e.cause }

func (c *Controller) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *Controller) completed(ctx context.Context, err error) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status.LastCompletedAt = time.Now().UTC()
	c.status.LastProgressAt = c.status.LastCompletedAt
	// Shutting down is neither a failed pass nor evidence of recovery.
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		c.status.ConsecutiveFailures++
		return
	}
	c.status.LastSuccessAt = c.status.LastCompletedAt
	c.status.ConsecutiveFailures = 0
}

func (c *Controller) progress() {
	c.statusMu.Lock()
	c.status.LastProgressAt = time.Now().UTC()
	c.statusMu.Unlock()
}

func (c *Controller) Reconcile(ctx context.Context) (result error) {
	defer func() { c.completed(ctx, result) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	runs, err := c.Store.Active()
	if err != nil {
		return &operationError{category: "active_runs_read_failed", cause: err}
	}
	c.progress()
	// Prioritize cancellation and bound concurrent Kubernetes calls. An unhealthy
	// tenant must not serialize every other run behind its request timeouts.
	sort.SliceStable(runs, func(i, j int) bool { return runs[i].State == model.Cancelling && runs[j].State != model.Cancelling })
	work := make(chan model.Record, len(runs))
	for _, r := range runs {
		work <- r
	}
	close(work)
	var wg sync.WaitGroup
	var failuresMu sync.Mutex
	var failures []error
	for i := 0; i < min(8, len(runs)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				if ctx.Err() != nil {
					return
				}
				err := c.reconcileRun(ctx, r)
				c.progress()
				if err != nil && !errors.Is(err, store.ErrConflict) && !errors.Is(err, backend.ErrPending) {
					failure := &operationError{category: "run_reconciliation_failed", runID: r.ID, cause: err}
					failuresMu.Lock()
					failures = append(failures, failure)
					failuresMu.Unlock()
					if ctx.Err() == nil {
						c.logger().Warn("run reconciliation will retry", "run_id", r.ID, "category", failure.category)
					}
				}
			}
		}()
	}
	wg.Wait()
	c.statusMu.Lock()
	c.status.RunErrors = len(failures)
	c.status.MaintenanceFailed = false
	c.statusMu.Unlock()
	if err := c.maintain(ctx); err != nil && !errors.Is(err, context.Canceled) {
		failures = append(failures, &operationError{category: "maintenance_failed", cause: err})
		c.statusMu.Lock()
		c.status.MaintenanceFailed = true
		c.statusMu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (c *Controller) reconcileRun(ctx context.Context, r model.Record) error {
	opctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// A separate startup wait bounds unavailable infrastructure. Execution time
	// is enforced by the Job's activeDeadlineSeconds, not the admission clock.
	if (r.State == model.Queued || r.State == model.Starting || r.State == model.Activating) && time.Since(r.CreatedAt) > c.startupTimeout() {
		ob, err := c.Backend.Observe(opctx, r.ID)
		if err == nil && !validObservationCode(ob.State, ob.ReasonCode) {
			return errors.New("invalid backend observation reason code")
		}
		if err == nil && (ob.State == model.Running || ob.State == model.Succeeded || ob.State == model.Failed || ob.State == model.Cancelling) {
			return c.observe(opctx, r, ob)
		}
		code := model.ReasonInfraFailure
		if err == nil && ob.ReasonCode == model.ReasonImagePullFailed {
			code = model.ReasonImagePullFailed
		}
		_, err = c.Store.TransitionWithCode(r.ID, []model.State{r.State}, model.Cancelling, "startup wait exceeded", code)
		return err
	}
	switch r.State {
	case model.Queued:
		if err := c.Backend.Ready(opctx); err != nil {
			return err
		}
		_, err := c.Store.Transition(r.ID, []model.State{model.Queued}, model.Starting, "")
		return err
	case model.Starting:
		// Stage only suspended compute. Persist activation intent before issuing
		// any noncreating activation request, including after ambiguous failures.
		if err := c.Backend.Submit(opctx, r); err != nil {
			return err
		}
		prepared, err := c.Store.Transition(r.ID, []model.State{model.Starting}, model.Activating, "")
		if err != nil {
			return err
		}
		return c.activate(opctx, prepared)
	case model.Activating:
		return c.activate(opctx, r)
	case model.Running:
		ob, err := c.Backend.Observe(opctx, r.ID)
		if errors.Is(err, backend.ErrNotFound) {
			_, err = c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "execution disappeared; automatic replay disabled", model.ReasonInfraFailure)
			return err
		}
		if err != nil {
			return err
		}
		if time.Since(r.CreatedAt) > c.startupTimeout() && ob.State == model.Starting {
			if !validObservationCode(ob.State, ob.ReasonCode) {
				return errors.New("invalid backend observation reason code")
			}
			code := model.ReasonInfraFailure
			if ob.ReasonCode == model.ReasonImagePullFailed {
				code = model.ReasonImagePullFailed
			}
			// Running records mean dispatched, not necessarily process-started.
			// Apply the existing admission-age bound only to a positive Starting
			// observation; an old actually-running/terminal task remains valid.
			_, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "startup wait exceeded", code)
			return err
		}
		return c.observe(opctx, r, ob)
	case model.Cancelling:
		if !validObservationCode(model.Cancelling, r.ReasonCode) {
			return errors.New("invalid persisted cleanup reason code")
		}
		// Persist a bounded snapshot before any deletion. A running process
		// may not have a final exit code yet; logging failure must not prevent
		// an authorized cancellation from stopping execution.
		captureErr := c.captureResult(opctx, r, true, time.Time{})
		// Give stopping its own finite budget. A log-read timeout must not
		// leave every cleanup attempt with an already expired context.
		cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 5*time.Second)
		cleanupErr := c.cleanup(cleanupCtx, r)
		cleanupCancel()
		if captureErr != nil {
			// Cancellation was authorized by an already durable intent. A full
			// result store must not keep the worker running. Retain that intent
			// and its reservation until persistence recovers; report the capture
			// failure even when asynchronous cleanup is also still pending.
			if cleanupErr != nil && !errors.Is(cleanupErr, backend.ErrPending) {
				return errors.Join(captureErr, cleanupErr)
			}
			return captureErr
		}
		if cleanupErr != nil {
			return cleanupErr
		}
		code := r.ReasonCode
		if code == "" {
			// Records written before structured codes existed retain the old
			// exact-match cancellation compatibility. New intent uses the code.
			code = model.ReasonUnknown
			if r.Reason == "cancel requested" {
				code = model.ReasonCancelled
			}
		}
		end := model.Failed
		if code == model.ReasonCancelled {
			end = model.Cancelled
		}
		_, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Cancelling}, end, r.Reason, code)
		return err
	}
	return nil
}

func (c *Controller) activate(ctx context.Context, r model.Record) error {
	err := c.Backend.Activate(ctx, r)
	if errors.Is(err, backend.ErrNotFound) {
		_, err = c.Store.TransitionWithCode(r.ID, []model.State{model.Activating}, model.Cancelling, "execution disappeared after activation intent; replay disabled", model.ReasonInfraFailure)
		return err
	}
	if err != nil {
		return err
	}
	_, err = c.Store.Transition(r.ID, []model.State{model.Activating}, model.Running, "execution dispatched")
	return err
}

func (c *Controller) observe(ctx context.Context, r model.Record, ob backend.Observation) error {
	// Validate before the same-state/no-progress paths. Otherwise an invalid
	// observation could either look healthy or become an ignored store CAS
	// conflict, leaving the controller stuck without reporting the failure.
	if !validObservationCode(ob.State, ob.ReasonCode) {
		return errors.New("invalid backend observation reason code")
	}
	if ob.State != model.Running && ob.State != model.Succeeded && ob.State != model.Failed && ob.State != model.Cancelling {
		return nil
	}
	if r.State == ob.State {
		return nil
	}
	code := ob.ReasonCode
	if code == "" {
		code = model.DefaultReasonCode(ob.State)
	}
	if ob.State.Terminal() {
		if err := c.captureResult(ctx, r, false, ob.FinishedAt); err != nil {
			return err
		}
	}
	stored, err := c.Store.TransitionWithCode(r.ID, []model.State{r.State}, ob.State, ob.Reason, code)
	if err != nil {
		return err
	}
	if stored.State.Terminal() {
		return c.cleanup(ctx, stored)
	}
	return nil
}

func (c *Controller) startupTimeout() time.Duration {
	if c.StartupTimeout > 0 {
		return c.StartupTimeout
	}
	return 5 * time.Minute
}

func (c *Controller) captureResult(ctx context.Context, r model.Record, cancelling bool, finishedAt time.Time) error {
	if r.Result != nil {
		return nil
	}
	capturer, ok := c.Backend.(backend.ResultCapturer)
	if !ok {
		return nil
	} // Legacy embedded/test backends may not expose results.
	result, err := capturer.CaptureResult(ctx, r.ID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		// Use the Job's stable terminal timestamp across controller restarts.
		// Older backends/Jobs without it use the last durable lifecycle change,
		// never an in-memory first-observed clock that resets on restart.
		if finishedAt.IsZero() || finishedAt.After(time.Now().UTC()) {
			finishedAt = r.UpdatedAt
		}
		wait := c.ResultCaptureWait
		if wait <= 0 {
			wait = 15 * time.Second
		}
		if !cancelling && !errors.Is(err, backend.ErrNotFound) && time.Since(finishedAt) < wait {
			return err
		}
		// Preserve an already observed exit status, but never expose a failed
		// log fetch as a complete output capture.
		result.Status = "unavailable"
		result.OutputHead, result.OutputTail = "", ""
		result.OutputComplete, result.OutputTruncated = false, false
	}
	if result.CapturedAt.IsZero() {
		result.CapturedAt = time.Now().UTC()
	}
	retention := c.ResultRetention
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	result.ExpiresAt = result.CapturedAt.Add(retention)
	return c.Store.SaveResult(r.ID, result)
}

func (c *Controller) cleanup(ctx context.Context, r model.Record) error {
	if inventory, ok := c.Backend.(backend.JobInventory); ok {
		return inventory.Cleanup(ctx, r)
	}
	return c.Backend.Cancel(ctx, r.ID)
}

// maintain recovers the terminal-commit/delete crash window and late suspended
// creations. Only store-known terminal records authorize cleanup. Unknown Jobs
// are counted for operators and are never adopted or deleted.
func (c *Controller) maintain(ctx context.Context) error {
	if !c.sweepMu.TryLock() {
		return nil
	}
	defer c.sweepMu.Unlock()
	now := time.Now().UTC()
	if now.Before(c.nextSweep) {
		return nil
	}
	interval := c.SweepInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	c.nextSweep = now.Add(interval)
	if _, err := c.Store.ExpireResults(now); err != nil {
		return err
	}
	inventory, ok := c.Backend.(backend.JobInventory)
	if !ok {
		return nil
	}
	opctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	executions, err := inventory.ListExecutions(opctx)
	if err != nil {
		return err
	}
	var terminal []model.Record
	orphans := 0
	for _, execution := range executions {
		r, err := c.Store.Lookup(execution.ID)
		if errors.Is(err, store.ErrNotFound) {
			orphans++
			continue
		}
		if err != nil {
			return err
		}
		if r.State.Terminal() {
			terminal = append(terminal, r)
		}
	}
	c.statusMu.Lock()
	c.status.OrphanJobs, c.status.TerminalCleanupPending, c.status.LastSweepAt = orphans, len(terminal), now
	c.statusMu.Unlock()
	// Bound each sweep and rotate its starting point so one unhealthy Job
	// cannot starve older cleanup. Active cancellations ran before this work.
	if len(terminal) == 0 {
		c.sweepOffset = 0
		return nil
	}
	sort.Slice(terminal, func(i, j int) bool { return terminal[i].ID < terminal[j].ID })
	count := min(16, len(terminal))
	work := make(chan model.Record, count)
	for i := 0; i < count; i++ {
		work <- terminal[(c.sweepOffset+i)%len(terminal)]
	}
	c.sweepOffset = (c.sweepOffset + count) % len(terminal)
	close(work)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []error
	for i := 0; i < min(8, count); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				err := c.captureResult(opctx, r, false, r.UpdatedAt)
				if err == nil {
					err = inventory.Cleanup(opctx, r)
				}
				if err != nil && !errors.Is(err, backend.ErrPending) {
					mu.Lock()
					failures = append(failures, &operationError{category: "terminal_cleanup_failed", runID: r.ID, cause: err})
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(failures...)
}

func validObservationCode(state model.State, code model.ReasonCode) bool {
	if code == "" {
		return true // Legacy observations omit causes; terminal defaults apply later.
	}
	if !code.Valid() {
		return false
	}
	switch state {
	case model.Starting:
		// A transient image-pull hint is not persisted as a failed outcome.
		return code == model.ReasonImagePullFailed
	case model.Succeeded:
		return code == model.ReasonCompleted
	case model.Failed:
		return code != model.ReasonCompleted && code != model.ReasonCancelled
	case model.Cancelling:
		return code != model.ReasonCompleted
	}
	return false
}
