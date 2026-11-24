package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
)

type observedResultsBackend struct {
	*resultsBackend
	receipt       *int
	attachReceipt bool
	observedCalls int
	legacyCalls   int
}

func (b *observedResultsBackend) Observe(ctx context.Context, id string) (backend.Observation, error) {
	ob, err := b.resultsBackend.Observe(ctx, id)
	if b.attachReceipt && ob.State.Terminal() {
		ob.Capture = b.receipt
	}
	return ob, err
}

func (b *observedResultsBackend) CaptureResult(ctx context.Context, id string) (model.ExecutionResult, error) {
	b.legacyCalls++
	return b.resultsBackend.CaptureResult(ctx, id)
}

func (b *observedResultsBackend) CaptureObservedResult(ctx context.Context, id string, ob backend.Observation) (model.ExecutionResult, error) {
	b.observedCalls++
	if ob.Capture != b.receipt || !ob.State.Terminal() || !ob.FinishedAt.Equal(b.finishedAt) {
		return model.ExecutionResult{}, errors.New("controller changed observation evidence")
	}
	return b.resultsBackend.CaptureResult(ctx, id)
}

func TestControllerReusesObservationAndRetainsLegacyAndCancellationCapture(t *testing.T) {
	for _, tc := range []struct {
		name          string
		attachReceipt bool
		cancelling    bool
		wantObserved  int
		wantLegacy    int
	}{
		{"observed terminal", true, false, 1, 0},
		{"legacy terminal", false, false, 0, 1},
		{"durable cancellation", true, true, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, b, r := setup(t)
			tick(t, c)
			tick(t, c)
			b.observed = model.Failed
			runtime := &observedResultsBackend{resultsBackend: &resultsBackend{fakeBackend: b, db: c.Store, exitCode: 3, finishedAt: time.Now().UTC()}, receipt: new(int), attachReceipt: tc.attachReceipt}
			c.Backend = runtime
			wantState := model.Failed
			if tc.cancelling {
				if _, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "cancel requested", model.ReasonCancelled); err != nil {
					t.Fatal(err)
				}
				wantState = model.Cancelled
			}
			tick(t, c)
			got, err := c.Store.Get(r.TenantID, r.ID)
			if err != nil || got.State != wantState || got.Result == nil || got.Result.ExitCode == nil || *got.Result.ExitCode != 3 || runtime.observedCalls != tc.wantObserved || runtime.legacyCalls != tc.wantLegacy || runtime.cleanupCalls != 1 || b.jobs[r.ID] {
				t.Fatal("capture dispatch or durable cleanup ordering changed", got.State, err, runtime.observedCalls, runtime.legacyCalls)
			}
		})
	}
}

func TestObservedCaptureRetryKeepsJobFinishBudgetAcrossControllerRestart(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	b.observed = model.Failed
	runtime := &observedResultsBackend{resultsBackend: &resultsBackend{fakeBackend: b, db: c.Store, exitCode: 7, finishedAt: time.Now().UTC(), captureErr: errors.New("logs unavailable")}, receipt: new(int), attachReceipt: true}
	c.Backend = runtime
	if err := c.Reconcile(context.Background()); err == nil || state(t, c, r) != model.Running || !b.jobs[r.ID] || runtime.cleanupCalls != 0 {
		t.Fatal("recent terminal capture failure did not retain result retry")
	}
	runtime.finishedAt = time.Now().UTC().Add(-20 * time.Second)
	restarted := &Controller{Store: c.Store, Backend: runtime, Logger: c.Logger}
	tick(t, restarted)
	got, err := c.Store.Get(r.TenantID, r.ID)
	if err != nil || got.State != model.Failed || got.Result == nil || got.Result.Status != "unavailable" || got.Result.ExitCode == nil || *got.Result.ExitCode != 7 || got.Result.OutputComplete || b.jobs[r.ID] || runtime.observedCalls != 2 || runtime.legacyCalls != 0 {
		t.Fatal("observed capture reset finish budget or lost exit", got.State, err)
	}
	before := len(got.Events)
	runtime.captureErr = nil
	if err := restarted.observe(context.Background(), got, backend.Observation{State: model.Failed}); err != nil {
		t.Fatal(err)
	}
	again, _ := c.Store.Get(r.TenantID, r.ID)
	if len(again.Events) != before || again.Result.Status != "unavailable" || runtime.observedCalls != 2 || b.creates != 1 {
		t.Fatal("later capture rewrote a terminal outcome/result or replayed work")
	}
}
