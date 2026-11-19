package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

type fakeBackend struct {
	mu                                 sync.Mutex
	jobs                               map[string]bool
	creates, calls                     int
	ambiguous, readyErr, cancelPending bool
	activationAmbiguous                bool
	observed                           model.State
}

func (b *fakeBackend) Ready(context.Context) error {
	if b.readyErr {
		return errors.New("not ready")
	}
	return nil
}
func (b *fakeBackend) Submit(_ context.Context, r model.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if !b.jobs[r.ID] {
		b.jobs[r.ID] = true
		b.creates++
	}
	if b.ambiguous {
		b.ambiguous = false
		return errors.New("lost acknowledgment")
	}
	return nil
}
func (b *fakeBackend) Activate(_ context.Context, r model.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.jobs[r.ID] {
		return backend.ErrNotFound
	}
	if b.activationAmbiguous {
		b.activationAmbiguous = false
		return errors.New("lost activation acknowledgment")
	}
	return nil
}
func (b *fakeBackend) Observe(_ context.Context, id string) (backend.Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.jobs[id] {
		return backend.Observation{}, backend.ErrNotFound
	}
	return backend.Observation{State: b.observed}, nil
}
func (b *fakeBackend) Cancel(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cancelPending {
		return errors.New("deletion pending")
	}
	delete(b.jobs, id)
	return nil
}
func (b *fakeBackend) Logs(context.Context, string, int64) (string, error) { return "", nil }

func setup(t *testing.T) (*Controller, *fakeBackend, model.Record) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r, _, err := db.Admit(model.Record{ID: "test-run", TenantID: "local", Spec: model.Spec{TimeoutSeconds: 60}}, "", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBackend{jobs: map[string]bool{}, observed: model.Running}
	return &Controller{Store: db, Backend: b, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, b, r
}

func tick(t *testing.T, c *Controller) {
	t.Helper()
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func state(t *testing.T, c *Controller, r model.Record) model.State {
	t.Helper()
	v, err := c.Store.Get(r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	return v.State
}

func TestAmbiguousSubmitDoesNotCreateSecondExecution(t *testing.T) {
	c, b, r := setup(t)
	b.ambiguous = true
	tick(t, c)
	tick(t, c)
	if state(t, c, r) != model.Starting {
		t.Fatal("uncertain submission released reservation")
	}
	tick(t, c)
	if state(t, c, r) != model.Running || b.creates != 1 || b.calls != 2 {
		t.Fatal("bad retry", b.creates, b.calls, state(t, c, r))
	}
	b.observed = model.Succeeded
	tick(t, c)
	if state(t, c, r) != model.Succeeded {
		t.Fatal("not completed")
	}
	tick(t, c)
	if b.calls != 2 {
		t.Fatal("terminal run replayed")
	}
}

func TestCancelRetainsQuotaUntilBackendCleanup(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	_, err := c.Store.Transition(r.ID, []model.State{model.Running}, model.Cancelling, "cancel requested")
	if err != nil {
		t.Fatal(err)
	}
	b.cancelPending = true
	tick(t, c)
	if state(t, c, r) != model.Cancelling {
		t.Fatal("cleanup not confirmed")
	}
	_, _, err = c.Store.Admit(model.Record{ID: "another", TenantID: "local"}, "", 1, 1)
	if !errors.Is(err, store.ErrQuota) {
		t.Fatal("capacity released early", err)
	}
	b.cancelPending = false
	tick(t, c)
	if state(t, c, r) != model.Cancelled {
		t.Fatal("not cancelled")
	}
}

func TestLostActivationAndDeletedJobCannotRecreateWork(t *testing.T) {
	c, b, r := setup(t)
	b.activationAmbiguous = true
	tick(t, c)
	tick(t, c)
	if state(t, c, r) != model.Activating {
		t.Fatal("activation intent not preserved")
	}
	delete(b.jobs, r.ID)
	tick(t, c)
	if state(t, c, r) != model.Cancelling {
		t.Fatal("missing activation was not fenced")
	}
	tick(t, c)
	if state(t, c, r) != model.Failed || b.creates != 1 || b.calls != 1 {
		t.Fatal("activation recovery recreated work", b.creates, b.calls)
	}
}

func TestDeadlineAndMissingExecution(t *testing.T) {
	t.Run("completion after API downtime keeps success", func(t *testing.T) {
		c, b, _ := setup(t)
		r, _, err := c.Store.Admit(model.Record{ID: "old-completion", TenantID: "other", CreatedAt: time.Now().Add(-time.Hour), Spec: model.Spec{TimeoutSeconds: 1}}, "", 2, 2)
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Store.Transition(r.ID, []model.State{model.Queued}, model.Running, "")
		if err != nil {
			t.Fatal(err)
		}
		b.jobs[r.ID] = true
		b.observed = model.Succeeded
		tick(t, c)
		if state(t, c, r) != model.Succeeded {
			t.Fatal("completed work incorrectly failed from admission age")
		}
	})
	t.Run("missing running job cleans up without replay", func(t *testing.T) {
		c, b, r := setup(t)
		tick(t, c)
		tick(t, c)
		delete(b.jobs, r.ID)
		tick(t, c)
		if state(t, c, r) != model.Cancelling {
			t.Fatal("missing job released capacity before orphan cleanup")
		}
		tick(t, c)
		if state(t, c, r) != model.Failed || b.creates != 1 {
			t.Fatal("missing task replayed")
		}
	})
	t.Run("unready remains queued", func(t *testing.T) {
		c, b, r := setup(t)
		b.readyErr = true
		tick(t, c)
		if state(t, c, r) != model.Queued || b.creates != 0 {
			t.Fatal("unready execution")
		}
	})
	t.Run("expired queued work never submits", func(t *testing.T) {
		c, b, _ := setup(t)
		r, _, err := c.Store.Admit(model.Record{ID: "expired", TenantID: "other", CreatedAt: time.Now().Add(-10 * time.Minute), Spec: model.Spec{TimeoutSeconds: 1}}, "", 2, 2)
		if err != nil {
			t.Fatal(err)
		}
		tick(t, c)
		tick(t, c)
		if state(t, c, r) != model.Failed || b.jobs[r.ID] {
			t.Fatal("expired work executed")
		}
	})
}
