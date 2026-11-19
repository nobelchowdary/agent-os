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
	Store   *store.Store
	Backend backend.Backend
	Logger  *slog.Logger
}

func (c *Controller) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		if err := c.Reconcile(ctx); err != nil && ctx.Err() == nil {
			c.Logger.Error("reconciliation failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (c *Controller) Reconcile(ctx context.Context) error {
	runs, err := c.Store.Active()
	if err != nil {
		return err
	}
	// Prioritize cancellation and bound concurrent Kubernetes calls. An unhealthy
	// tenant must not serialize every other run behind its request timeouts.
	sort.SliceStable(runs, func(i, j int) bool { return runs[i].State == model.Cancelling && runs[j].State != model.Cancelling })
	work := make(chan model.Record, len(runs))
	for _, r := range runs {
		work <- r
	}
	close(work)
	var wg sync.WaitGroup
	for i := 0; i < min(8, len(runs)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range work {
				if ctx.Err() != nil {
					return
				}
				if err := c.reconcileRun(ctx, r); err != nil && !errors.Is(err, store.ErrConflict) {
					c.Logger.Warn("run reconciliation will retry", "run_id", r.ID, "error", err)
				}
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

func (c *Controller) reconcileRun(ctx context.Context, r model.Record) error {
	opctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// A separate startup wait bounds unavailable infrastructure. Execution time
	// is enforced by the Job's activeDeadlineSeconds, not the admission clock.
	if (r.State == model.Queued || r.State == model.Starting || r.State == model.Activating) && time.Since(r.CreatedAt) > 5*time.Minute {
		ob, err := c.Backend.Observe(opctx, r.ID)
		if err == nil && (ob.State == model.Running || ob.State == model.Succeeded || ob.State == model.Failed) {
			return c.observe(r, ob)
		}
		_, err = c.Store.Transition(r.ID, []model.State{r.State}, model.Cancelling, "startup wait exceeded")
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
			_, err = c.Store.Transition(r.ID, []model.State{model.Running}, model.Cancelling, "execution disappeared; automatic replay disabled")
			return err
		}
		if err != nil {
			return err
		}
		return c.observe(r, ob)
	case model.Cancelling:
		if err := c.Backend.Cancel(opctx, r.ID); err != nil {
			return err
		}
		end := model.Cancelled
		if r.Reason != "cancel requested" {
			end = model.Failed
		}
		_, err := c.Store.Transition(r.ID, []model.State{model.Cancelling}, end, r.Reason)
		return err
	}
	return nil
}

func (c *Controller) activate(ctx context.Context, r model.Record) error {
	err := c.Backend.Activate(ctx, r)
	if errors.Is(err, backend.ErrNotFound) {
		_, err = c.Store.Transition(r.ID, []model.State{model.Activating}, model.Cancelling, "execution disappeared after activation intent; replay disabled")
		return err
	}
	if err != nil {
		return err
	}
	_, err = c.Store.Transition(r.ID, []model.State{model.Activating}, model.Running, "execution dispatched")
	return err
}

func (c *Controller) observe(r model.Record, ob backend.Observation) error {
	if ob.State != model.Running && ob.State != model.Succeeded && ob.State != model.Failed {
		return nil
	}
	if r.State == ob.State {
		return nil
	}
	_, err := c.Store.Transition(r.ID, []model.State{r.State}, ob.State, ob.Reason)
	return err
}
