package controller

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
)

// passObserver prepares the optional read view lazily. Cancellation workers
// therefore do not wait behind a list before they can stop existing compute.
// The view is discarded after this pass; mutation paths retain fresh reads.
func (c *Controller) passObserver(ctx context.Context, runs []model.Record) func(context.Context, string) (backend.Observation, error) {
	batcher, ok := c.Backend.(backend.ObservationBatcher)
	if !ok {
		return c.Backend.Observe
	}
	ids := []string{}
	eligible := map[string]bool{}
	for _, run := range runs {
		if run.State == model.Running {
			ids = append(ids, run.ID)
			eligible[run.ID] = true
		}
	}
	if len(ids) < 2 || len(ids) > 64 {
		return c.Backend.Observe
	}
	var once sync.Once
	var observer backend.Observer
	var prepareErr error
	return func(opctx context.Context, id string) (backend.Observation, error) {
		if !eligible[id] {
			return c.Backend.Observe(opctx, id)
		}
		if err := opctx.Err(); err != nil {
			return backend.Observation{}, err
		}
		once.Do(func() {
			// Do not tie the shared list to the first worker's shorter remaining
			// private deadline. The pass itself owns this bounded operation.
			listctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			observer, prepareErr = batcher.PrepareObservations(listctx, ids)
			if prepareErr == nil && observer == nil {
				prepareErr = errors.New("empty execution observation view")
			}
		})
		if err := opctx.Err(); err != nil {
			return backend.Observation{}, err
		}
		if prepareErr != nil {
			return backend.Observation{}, prepareErr
		}
		return observer.Observe(opctx, id)
	}
}
