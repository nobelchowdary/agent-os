package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
)

type cleanupPhaseBackend struct {
	*resultsBackend
	phaseMu          sync.Mutex
	captureDeadlines map[string]time.Time
	cleaning         int
	maxCleaning      int
	cleanupAttempts  int
	cleanupHook      func(context.Context, model.Record) error
	captureHook      func(context.Context, string)
}

func (b *cleanupPhaseBackend) CaptureResult(ctx context.Context, id string) (model.ExecutionResult, error) {
	deadline, _ := ctx.Deadline()
	b.phaseMu.Lock()
	if b.captureDeadlines == nil {
		b.captureDeadlines = map[string]time.Time{}
	}
	b.captureDeadlines[id] = deadline
	b.phaseMu.Unlock()
	if b.captureHook != nil {
		b.captureHook(ctx, id)
	}
	return b.resultsBackend.CaptureResult(ctx, id)
}

func (b *cleanupPhaseBackend) Cleanup(ctx context.Context, r model.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	b.phaseMu.Lock()
	capturedDeadline := b.captureDeadlines[r.ID]
	b.cleaning++
	b.cleanupAttempts++
	b.maxCleaning = max(b.maxCleaning, b.cleaning)
	b.phaseMu.Unlock()
	defer func() {
		b.phaseMu.Lock()
		b.cleaning--
		b.phaseMu.Unlock()
	}()
	if !ok || !deadline.After(capturedDeadline) || time.Until(deadline) > 5*time.Second {
		return errors.New("cleanup did not get a fresh bounded operation context")
	}
	if b.cleanupHook != nil {
		if err := b.cleanupHook(ctx, r); err != nil {
			return err
		}
	}
	return b.resultsBackend.Cleanup(ctx, r)
}

func TestTerminalCohortCommitsAll64ResultsBeforeCleanupAndDrainsThisPass(t *testing.T) {
	c, b, initial := setup(t)
	runs := []model.Record{initial}
	for i := 1; i < 64; i++ {
		r, _, err := c.Store.Admit(model.Record{ID: fmt.Sprintf("cohort-%03d", i), TenantID: "local", Spec: initial.Spec}, "", 64, 64)
		if err != nil {
			t.Fatal(err)
		}
		runs = append(runs, r)
	}
	b.observed = model.Succeeded
	runtime := &cleanupPhaseBackend{resultsBackend: &resultsBackend{fakeBackend: b, db: c.Store}}
	c.Backend = runtime
	tick(t, c)
	tick(t, c)
	entered, release := make(chan struct{}, 64), make(chan struct{})
	var releaseOnce sync.Once
	releaseCleanup := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseCleanup()
	var checkOnce sync.Once
	var orderingErr error
	runtime.cleanupHook = func(ctx context.Context, _ model.Record) error {
		checkOnce.Do(func() {
			for _, r := range runs {
				stored, err := c.Store.Lookup(r.ID)
				if err != nil || stored.State != model.Succeeded || stored.Result == nil || stored.Result.Status != "captured" {
					orderingErr = errors.New("cleanup started before the whole terminal cohort and results were durable")
					return
				}
			}
		})
		if orderingErr != nil {
			return orderingErr
		}
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- c.Reconcile(context.Background()) }()
	for i := 0; i < 8; i++ {
		select {
		case <-entered:
		case err := <-done:
			t.Fatalf("terminal phase did not start eight cleanup workers: %v", err)
		case <-time.After(4 * time.Second):
			t.Fatal("terminal cleanup did not start")
		}
	}
	runtime.phaseMu.Lock()
	cleaning, maximum := runtime.cleaning, runtime.maxCleaning
	runtime.phaseMu.Unlock()
	if cleaning != 8 || maximum != 8 {
		t.Fatal("cleanup concurrency is not bounded at eight", cleaning, maximum)
	}
	active, err := c.Store.Active()
	if err != nil || len(active) != 0 {
		t.Fatal("terminal cohort retained durable admission reservations", len(active), err)
	}
	releaseCleanup()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("terminal cohort cleanup did not drain")
	}
	if len(b.jobs) != 0 || runtime.cleanupAttempts != 64 || runtime.cleanupCalls != 64 || runtime.captureCalls != 64 || b.creates != 64 {
		t.Fatal("fresh cleanup was left for the bounded inventory sweep", len(b.jobs), runtime.cleanupAttempts, runtime.cleanupCalls)
	}
	runtime.cleanupHook = nil
	// Reusing one admission slot for another 200 runs catches a cleanup queue
	// that releases durable quota while accumulating finished Job objects.
	for i := 0; i < 200; i++ {
		r, _, err := c.Store.Admit(model.Record{ID: fmt.Sprintf("after-cohort-%03d", i), TenantID: "local", Spec: initial.Spec}, "", 1, 1)
		if err != nil {
			t.Fatal("terminal cohort did not release admission quota", i, err)
		}
		for pass := 0; pass < 3; pass++ {
			tick(t, c)
		}
		stored, err := c.Store.Lookup(r.ID)
		if err != nil || stored.State != model.Succeeded || stored.Result == nil || len(b.jobs) != 0 {
			t.Fatal("replacement execution was not persisted and cleaned", i, err)
		}
	}
	if runtime.captureCalls != 264 || runtime.cleanupCalls != 264 || b.creates != 264 {
		t.Fatal("cohort/replacement work was replayed or leaked", runtime.captureCalls, runtime.cleanupCalls, b.creates)
	}
}

func TestTerminalCleanupFailureOrPendingRecoversBySweepWithoutReplay(t *testing.T) {
	for _, mode := range []string{"failed request", "pending deletion", "lost acknowledgment"} {
		t.Run(mode, func(t *testing.T) {
			c, b, r := setup(t)
			tick(t, c)
			tick(t, c)
			b.observed = model.Failed
			runtime := &cleanupPhaseBackend{resultsBackend: &resultsBackend{fakeBackend: b, db: c.Store, exitCode: 3}}
			failure := errors.New("cleanup transport failed")
			runtime.cleanupHook = func(ctx context.Context, r model.Record) error {
				if mode == "pending deletion" {
					return backend.ErrPending
				}
				if mode == "lost acknowledgment" {
					if err := b.Cancel(ctx, r.ID); err != nil {
						return err
					}
				}
				return failure
			}
			c.Backend = runtime
			// This pass must attempt the fresh cohort even when inventory
			// maintenance is not due for another hour.
			c.nextSweep = time.Now().Add(time.Hour)
			err := c.Reconcile(context.Background())
			pending := mode == "pending deletion"
			if pending && err != nil || !pending && !errors.Is(err, failure) {
				t.Fatal("cleanup outcome was not classified correctly", err)
			}
			before, err := c.Store.Lookup(r.ID)
			if err != nil || before.State != model.Failed || before.Result == nil || before.Result.ExitCode == nil || *before.Result.ExitCode != 3 || runtime.cleanupAttempts != 1 {
				t.Fatal("cleanup outcome lost committed terminal result", err)
			}
			status := c.Status()
			if pending && (status.RunErrors != 0 || status.ConsecutiveFailures != 0) || !pending && (status.RunErrors != 1 || status.ConsecutiveFailures != 1) {
				t.Fatal("cleanup phase changed controller error accounting", status)
			}
			// A lost acknowledgment can be followed by a late object. Neither
			// that object nor a readable later log may reopen the outcome.
			b.jobs[r.ID] = true
			runtime.cleanupHook = nil
			restarted := &Controller{Store: c.Store, Backend: runtime, Logger: c.Logger}
			tick(t, restarted)
			after, err := c.Store.Lookup(r.ID)
			if err != nil || !reflect.DeepEqual(before, after) || b.jobs[r.ID] || runtime.captureCalls != 1 || b.creates != 1 || b.calls != 1 || runtime.cleanupAttempts != 2 {
				t.Fatal("sweep changed terminal data, replayed work or failed cleanup", err)
			}
		})
	}
}

func TestCancelledPassCommitsCapturedTerminalButDoesNotStartDeletion(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	b.observed = model.Succeeded
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &cleanupPhaseBackend{resultsBackend: &resultsBackend{fakeBackend: b, db: c.Store}}
	runtime.captureHook = func(context.Context, string) { cancel() }
	c.Backend = runtime
	if err := c.Reconcile(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled pass did not return cancellation", err)
	}
	before, err := c.Store.Lookup(r.ID)
	if err != nil || before.State != model.Succeeded || before.Result == nil || !b.jobs[r.ID] || runtime.cleanupAttempts != 0 || runtime.cleanupCalls != 0 || c.Status().ConsecutiveFailures != 0 {
		t.Fatal("cancelled parent context was replaced with a live deletion context", err)
	}
	runtime.captureHook = nil
	restarted := &Controller{Store: c.Store, Backend: runtime, Logger: c.Logger}
	tick(t, restarted)
	after, err := c.Store.Lookup(r.ID)
	if err != nil || !reflect.DeepEqual(before, after) || b.jobs[r.ID] || runtime.captureCalls != 1 || runtime.cleanupCalls != 1 || b.creates != 1 {
		t.Fatal("restart did not recover cancelled cleanup without rewriting/replay", err)
	}
}
