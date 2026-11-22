package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
)

type hookedBackend struct {
	backend.Backend
	ready   func(context.Context) error
	observe func(context.Context, string) (backend.Observation, error)
}

func (b *hookedBackend) Ready(ctx context.Context) error {
	if b.ready != nil {
		return b.ready(ctx)
	}
	return b.Backend.Ready(ctx)
}

func (b *hookedBackend) Observe(ctx context.Context, id string) (backend.Observation, error) {
	if b.observe != nil {
		return b.observe(ctx, id)
	}
	return b.Backend.Observe(ctx, id)
}

func waitFor(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("controller did not reach expected condition")
}

func startLoop(t *testing.T, c *Controller) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	// An hour-long fallback proves that observed changes wake the loop.
	return startLoopInterval(t, c, time.Hour)
}

func startLoopInterval(t *testing.T, c *Controller, interval time.Duration) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.run(ctx, interval)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("controller did not stop")
		}
	})
	return cancel, done
}

func TestCommittedAdmissionAndCancelWakeController(t *testing.T) {
	c, _, initial := setup(t)
	if _, err := c.Store.Transition(initial.ID, []model.State{model.Queued}, model.Succeeded, "fixture completed"); err != nil {
		t.Fatal(err)
	}
	startLoop(t, c)
	waitFor(t, func() bool { return !c.Status().LastSuccessAt.IsZero() })
	r, _, err := c.Store.Admit(model.Record{ID: "admitted-after-start", TenantID: "local", Spec: initial.Spec}, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return state(t, c, r) == model.Running })
	if _, err := c.Store.Transition(r.ID, []model.State{model.Running}, model.Cancelling, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return state(t, c, r) == model.Cancelled })
	if snapshot := c.Status(); !snapshot.Running || snapshot.ConsecutiveFailures != 0 {
		t.Fatalf("unexpected loop status: %+v", snapshot)
	}
}

func TestCommitDuringReconcileIsNotMissedAndConflictIsHealthy(t *testing.T) {
	c, b, r := setup(t)
	entered, release := make(chan struct{}), make(chan struct{})
	c.Backend = &hookedBackend{Backend: b, ready: func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	startLoop(t, c)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("readiness was not checked")
	}
	if _, err := c.Store.Transition(r.ID, []model.State{model.Queued}, model.Cancelling, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitFor(t, func() bool { return state(t, c, r) == model.Cancelled })
	waitFor(t, func() bool { return !c.Status().LastSuccessAt.IsZero() })
	if snapshot := c.Status(); snapshot.ConsecutiveFailures != 0 {
		t.Fatalf("expected CAS conflict counted as a failure: %+v", snapshot)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.creates != 0 {
		t.Fatal("cancelled queued work was submitted")
	}
}

func TestFailuresAggregateWithoutBlockingCancellationAndRecover(t *testing.T) {
	c, b, _ := setup(t)
	for _, id := range []string{"second-failure", "cleanup"} {
		r, _, err := c.Store.Admit(model.Record{ID: id, TenantID: id}, "", 1, 3)
		if err != nil {
			t.Fatal(err)
		}
		if id == "cleanup" {
			if _, err := c.Store.Transition(r.ID, []model.State{model.Queued}, model.Cancelling, "cancel requested"); err != nil {
				t.Fatal(err)
			}
		}
	}
	secret := errors.New("backend failed with credential=never-expose-this")
	c.Backend = &hookedBackend{Backend: b, ready: func(context.Context) error { return secret }}
	var logs bytes.Buffer
	c.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	for failureCount := uint64(1); failureCount <= 2; failureCount++ {
		err := c.Reconcile(context.Background())
		if !errors.Is(err, secret) {
			t.Fatal("aggregate lost the operation error cause")
		}
		joined, ok := err.(interface{ Unwrap() []error })
		if !ok || len(joined.Unwrap()) != 2 {
			t.Fatalf("expected both failing runs: %v", err)
		}
		if strings.Contains(err.Error(), "credential") || strings.Contains(logs.String(), "never-expose-this") {
			t.Fatal("raw backend details escaped sanitization")
		}
		if !strings.Contains(logs.String(), "run_id=test-run") || !strings.Contains(logs.String(), "category=run_reconciliation_failed") {
			t.Fatal("retry log is missing safe run/category context")
		}
		snapshot := c.Status()
		if snapshot.Running || snapshot.LastCompletedAt.IsZero() || !snapshot.LastSuccessAt.IsZero() || snapshot.ConsecutiveFailures != failureCount {
			t.Fatalf("failure status is incorrect: %+v", snapshot)
		}
	}
	if got, err := c.Store.Get("cleanup", "cleanup"); err != nil || got.State != model.Cancelled {
		t.Fatalf("unhealthy runs prevented cleanup: %+v, %v", got, err)
	}
	c.Backend = b
	tick(t, c)
	snapshot := c.Status()
	if snapshot.ConsecutiveFailures != 0 || snapshot.LastSuccessAt.IsZero() || !snapshot.LastSuccessAt.Equal(snapshot.LastCompletedAt) {
		t.Fatalf("successful pass did not recover status: %+v", snapshot)
	}
}

func TestWorkloadFailureIsSuccessfulReconciliation(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	b.observed = model.Failed
	tick(t, c)
	if state(t, c, r) != model.Failed || c.Status().ConsecutiveFailures != 0 {
		t.Fatal("legitimate workload failure marked controller unhealthy")
	}
}

func TestUnchangedObservationDoesNotBusyLoop(t *testing.T) {
	c, b, r := setup(t)
	var calls atomic.Int64
	c.Backend = &hookedBackend{Backend: b, observe: func(ctx context.Context, id string) (backend.Observation, error) {
		calls.Add(1)
		return b.Observe(ctx, id)
	}}
	startLoop(t, c)
	waitFor(t, func() bool { return calls.Load() == 1 })
	if _, err := c.Store.Transition(r.ID, []model.State{model.Running}, model.Running, "ignored no-op"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("unchanged running state caused repeated reconciliation")
	}
}

func TestPeriodicReconcileObservesBackendCompletionWithoutStoreCommit(t *testing.T) {
	c, b, r := setup(t)
	startLoopInterval(t, c, 10*time.Millisecond)
	waitFor(t, func() bool { return state(t, c, r) == model.Running })
	// External execution changes do not invalidate the local Store. They must
	// still be observed through the periodic reconciliation fallback.
	b.mu.Lock()
	b.observed = model.Succeeded
	b.mu.Unlock()
	waitFor(t, func() bool { return state(t, c, r) == model.Succeeded })
}

func TestStopCancelsInflightOperationWithoutFalseFailure(t *testing.T) {
	c, b, _ := setup(t)
	entered := make(chan struct{})
	var once sync.Once
	c.Backend = &hookedBackend{Backend: b, observe: func(ctx context.Context, _ string) (backend.Observation, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return backend.Observation{}, ctx.Err()
	}}
	cancel, done := startLoop(t, c)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("observation was not started")
	}
	before := c.Status()
	if !before.Running || before.LastSuccessAt.IsZero() {
		t.Fatalf("unexpected running status: %+v", before)
	}
	// Reading status concurrently with shutdown exercises its synchronization.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for c.Status().Running {
		}
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not stop in-flight operation")
	}
	<-readerDone
	after := c.Status()
	if after.Running || after.ConsecutiveFailures != 0 || !after.LastSuccessAt.Equal(before.LastSuccessAt) {
		t.Fatalf("shutdown changed health incorrectly: %+v", after)
	}
}
