package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
)

type passTestBackend struct {
	*fakeBackend
	passMu                     sync.Mutex
	prepares, direct, cleanups int
	ids                        []string
	outcome                    model.State
	prepareErr                 error
	started                    chan context.Context
	release                    chan struct{}
	cleaned                    chan struct{}
}

type passTestView struct{ outcome model.State }

func (v passTestView) Observe(ctx context.Context, id string) (backend.Observation, error) {
	if err := ctx.Err(); err != nil {
		return backend.Observation{}, err
	}
	return backend.Observation{State: v.outcome}, nil
}
func (b *passTestBackend) PrepareObservations(ctx context.Context, ids []string) (backend.Observer, error) {
	b.passMu.Lock()
	b.prepares++
	b.ids = append([]string(nil), ids...)
	outcome, err, started, release := b.outcome, b.prepareErr, b.started, b.release
	b.passMu.Unlock()
	if started != nil {
		select {
		case started <- ctx:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return passTestView{outcome}, nil
}
func (b *passTestBackend) Observe(ctx context.Context, id string) (backend.Observation, error) {
	b.passMu.Lock()
	b.direct++
	b.passMu.Unlock()
	return b.fakeBackend.Observe(ctx, id)
}
func (b *passTestBackend) Cancel(ctx context.Context, id string) error {
	b.passMu.Lock()
	b.cleanups++
	cleaned := b.cleaned
	b.passMu.Unlock()
	if cleaned != nil {
		select {
		case cleaned <- struct{}{}:
		default:
		}
	}
	return b.fakeBackend.Cancel(ctx, id)
}

func passTestCohort(t *testing.T, n int) (*Controller, *passTestBackend, []model.Record) {
	t.Helper()
	c, base, first := setup(t)
	runs := []model.Record{first}
	for i := 1; i < n; i++ {
		r, _, err := c.Store.Admit(model.Record{ID: fmt.Sprintf("batch-%d", i), TenantID: first.TenantID, Spec: first.Spec}, "", n, n)
		if err != nil {
			t.Fatal(err)
		}
		runs = append(runs, r)
	}
	for i, r := range runs {
		var err error
		for _, next := range []model.State{model.Starting, model.Activating, model.Running} {
			r, err = c.Store.Transition(r.ID, []model.State{r.State}, next, "")
			if err != nil {
				t.Fatal(err)
			}
		}
		runs[i] = r
		base.jobs[r.ID] = true
	}
	b := &passTestBackend{fakeBackend: base, outcome: model.Running}
	c.Backend = b
	return c, b, runs
}

func TestPassObserverSinglePrepareConcurrentWorkersAndFreshNextPass(t *testing.T) {
	c, b, runs := passTestCohort(t, 64)
	tick(t, c)
	if b.prepares != 1 || b.direct != 0 || len(b.ids) != 64 {
		t.Fatal("batch not shared", b.prepares, b.direct, len(b.ids))
	}
	b.outcome = model.Succeeded
	restarted := &Controller{Store: c.Store, Backend: b, Logger: c.Logger}
	tick(t, restarted)
	if b.prepares != 2 || b.direct != 0 || b.cleanups != 64 || b.creates != 0 {
		t.Fatal("view leaked between passes or cleanup replayed", b.prepares, b.direct, b.cleanups, b.creates)
	}
	for _, run := range runs {
		if state(t, c, run) != model.Succeeded || b.jobs[run.ID] {
			t.Fatal("cohort outcome or fresh cleanup missing")
		}
	}
	tick(t, restarted)
	if b.prepares != 2 || b.cleanups != 64 {
		t.Fatal("terminal cohort reobserved or rewritten")
	}
}

func TestPassObserverPreparationFailureRetainsEveryReservation(t *testing.T) {
	c, b, runs := passTestCohort(t, 16)
	b.prepareErr = errors.New("list expired")
	if err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("missing per-run errors")
	}
	if b.prepares != 1 || b.direct != 0 || b.cleanups != 0 || b.creates != 0 || c.Status().RunErrors != 16 {
		t.Fatal("failed shared list fell back, cleaned or replayed", b.prepares, b.direct, b.cleanups, c.Status())
	}
	active, err := c.Store.Active()
	if err != nil || len(active) != 16 {
		t.Fatal("failed read released reservations", err)
	}
	for _, run := range runs {
		if state(t, c, run) != model.Running || !b.jobs[run.ID] {
			t.Fatal("failed list manufactured terminal evidence")
		}
	}
	b.prepareErr = nil
	b.outcome = model.Succeeded
	tick(t, c)
	if b.prepares != 2 || b.cleanups != 16 {
		t.Fatal("failed pass did not recover with fresh snapshot")
	}
}

func TestPassObserverBlockedListDoesNotBlockDurableCancellation(t *testing.T) {
	c, b, runs := passTestCohort(t, 3)
	if _, err := c.Store.TransitionWithCode(runs[0].ID, []model.State{model.Running}, model.Cancelling, "cancel requested", model.ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	b.started = make(chan context.Context, 1)
	b.release = make(chan struct{})
	b.cleaned = make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- c.Reconcile(context.Background()) }()
	listctx := <-b.started
	<-b.cleaned // This must arrive while PrepareObservations is still blocked.
	if listctx.Err() != nil {
		t.Fatal("cancellation waited for list timeout")
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if state(t, c, runs[0]) != model.Cancelled || b.cleanups != 1 || b.prepares != 1 || b.direct != 0 {
		t.Fatal("cancellation used read view or lost priority")
	}
	if len(b.ids) != 2 {
		t.Fatal("cancelling run entered batch cohort", b.ids)
	}
}

func TestPassObserverSharedListUsesPassContextNotFirstPrivateContext(t *testing.T) {
	c, b, runs := passTestCohort(t, 2)
	b.started = make(chan context.Context, 1)
	b.release = make(chan struct{})
	observe := c.passObserver(context.Background(), runs)
	private, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := observe(private, runs[0].ID); first <- err }()
	listctx := <-b.started
	cancel()
	if listctx.Err() != nil {
		t.Fatal("first worker cancelled shared read")
	}
	second := make(chan error, 1)
	go func() {
		ob, err := observe(context.Background(), runs[1].ID)
		if err == nil && ob.State != model.Running {
			err = errors.New("shared view lost")
		}
		second <- err
	}()
	close(b.release)
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal("private cancellation ignored", err)
	}
	if err := <-second; err != nil {
		t.Fatal("private cancellation poisoned other run", err)
	}
	if b.prepares != 1 {
		t.Fatal("parallel preparation duplicated")
	}
}

func TestPassObserverUsesIndividualReadForOneRunningRun(t *testing.T) {
	c, b, _ := passTestCohort(t, 1)
	tick(t, c)
	if b.prepares != 0 || b.direct != 1 {
		t.Fatal("single run allocated batch reads", b.prepares, b.direct)
	}
}

type emptyPassBackend struct{ *passTestBackend }

func (b emptyPassBackend) PrepareObservations(context.Context, []string) (backend.Observer, error) {
	return nil, nil
}
func TestPassObserverRejectsEmptyPreparedView(t *testing.T) {
	c, b, runs := passTestCohort(t, 2)
	c.Backend = emptyPassBackend{b}
	if err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("empty view was accepted")
	}
	for _, run := range runs {
		if state(t, c, run) != model.Running || !b.jobs[run.ID] {
			t.Fatal("empty view manufactured outcome")
		}
	}
}
