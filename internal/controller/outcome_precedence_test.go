package controller

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

func TestCommittedCleanupIntentWinsLateOutcomeAcrossCASAndRestart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		intent model.ReasonCode
		late   backend.Observation
		end    model.State
	}{
		{"cancellation versus completion", model.ReasonCancelled, backend.Observation{State: model.Succeeded, ReasonCode: model.ReasonCompleted}, model.Cancelled},
		{"cancellation versus deadline", model.ReasonCancelled, backend.Observation{State: model.Failed, ReasonCode: model.ReasonDeadlineExceeded}, model.Cancelled},
		{"infrastructure cleanup versus completion", model.ReasonInfraFailure, backend.Observation{State: model.Succeeded, ReasonCode: model.ReasonCompleted}, model.Failed},
		{"image cleanup versus OOM", model.ReasonImagePullFailed, backend.Observation{State: model.Failed, ReasonCode: model.ReasonOOMKilled}, model.Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, b, r := setup(t)
			tick(t, c)
			tick(t, c)
			entered, release := make(chan struct{}), make(chan struct{})
			c.Backend = &hookedBackend{Backend: b, observe: func(ctx context.Context, _ string) (backend.Observation, error) {
				close(entered)
				select {
				case <-release:
					return tc.late, nil
				case <-ctx.Done():
					return backend.Observation{}, ctx.Err()
				}
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- c.Reconcile(ctx) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("observation was not in flight")
			}
			intent, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "intent committed first", tc.intent)
			if err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-result; err != nil {
				t.Fatalf("normal CAS collision was not handled: %v", err)
			}
			if !reflect.DeepEqual(currentRecord(t, c, r), intent) {
				t.Fatal("late observation overwrote committed cleanup intent")
			}
			observedAgain := false
			restarted := &Controller{Store: c.Store, Logger: c.Logger, Backend: &hookedBackend{Backend: b,
				observe: func(context.Context, string) (backend.Observation, error) { observedAgain = true; return tc.late, nil },
			}}
			b.cancelPending = true
			tick(t, restarted)
			if observedAgain || !reflect.DeepEqual(currentRecord(t, c, r), intent) {
				t.Fatal("restart revisited execution outcome before confirming cleanup")
			}
			if _, _, err := c.Store.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
				t.Fatal("collision released capacity before cleanup")
			}
			b.cancelPending = false
			tick(t, restarted)
			terminal := currentRecord(t, c, r)
			if terminal.State != tc.end || terminal.ReasonCode != tc.intent {
				t.Fatalf("cleanup did not retain first committed intent: %+v", terminal)
			}
			// A further restart with a different backend outcome cannot rewrite
			// the terminal record or execute the workload again.
			final := &Controller{Store: c.Store, Backend: restarted.Backend, Logger: c.Logger}
			tick(t, final)
			if observedAgain || !reflect.DeepEqual(currentRecord(t, c, r), terminal) || b.creates != 1 || b.calls != 1 {
				t.Fatal("committed terminal outcome was replayed or rewritten")
			}
		})
	}
}

func TestCommittedTerminalOutcomeWinsStaleCancellationAcrossRestart(t *testing.T) {
	for _, ob := range []backend.Observation{
		{State: model.Succeeded, ReasonCode: model.ReasonCompleted},
		{State: model.Failed, ReasonCode: model.ReasonDeadlineExceeded},
		{State: model.Failed, ReasonCode: model.ReasonOOMKilled},
	} {
		t.Run(string(ob.ReasonCode), func(t *testing.T) {
			c, b, r := setup(t)
			tick(t, c)
			tick(t, c)
			c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) { return ob, nil }}
			tick(t, c)
			terminal := currentRecord(t, c, r)
			if _, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "stale cancel request", model.ReasonCancelled); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("stale cancellation replaced terminal outcome: %v", err)
			}
			observedAgain := false
			restarted := &Controller{Store: c.Store, Logger: c.Logger, Backend: &hookedBackend{Backend: b,
				observe: func(context.Context, string) (backend.Observation, error) {
					observedAgain = true
					return backend.Observation{State: model.Failed, ReasonCode: model.ReasonTaskError}, nil
				},
			}}
			tick(t, restarted)
			if terminal.State != ob.State || terminal.ReasonCode != ob.ReasonCode || observedAgain || !reflect.DeepEqual(currentRecord(t, c, r), terminal) {
				t.Fatal("restart changed the first committed terminal outcome")
			}
		})
	}
}

func TestDispatchedStartupWaitUsesAdmissionAgeAndActualObservation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		age   time.Duration
		ob    backend.Observation
		err   error
		state model.State
		code  model.ReasonCode
	}{
		{"transient image pull before bound may recover", 4 * time.Minute, backend.Observation{State: model.Starting, ReasonCode: model.ReasonImagePullFailed}, nil, model.Running, ""},
		{"expired image pull requests cleanup", 6 * time.Minute, backend.Observation{State: model.Starting, ReasonCode: model.ReasonImagePullFailed}, nil, model.Cancelling, model.ReasonImagePullFailed},
		{"expired pending execution is infrastructure failure", 6 * time.Minute, backend.Observation{State: model.Starting}, nil, model.Cancelling, model.ReasonInfraFailure},
		{"actual running execution is not startup-expired", time.Hour, backend.Observation{State: model.Running}, nil, model.Running, ""},
		{"committed Job completion beats startup age", time.Hour, backend.Observation{State: model.Succeeded, ReasonCode: model.ReasonCompleted}, nil, model.Succeeded, model.ReasonCompleted},
		{"actual Job deadline retains its own cause", time.Hour, backend.Observation{State: model.Failed, ReasonCode: model.ReasonDeadlineExceeded}, nil, model.Failed, model.ReasonDeadlineExceeded},
		{"failure target retains its cleanup cause", time.Hour, backend.Observation{State: model.Cancelling, ReasonCode: model.ReasonOOMKilled}, nil, model.Cancelling, model.ReasonOOMKilled},
		{"API outage does not establish unstarted execution", time.Hour, backend.Observation{}, errors.New("temporary API unavailable"), model.Running, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, b, prior := setup(t)
			if _, err := c.Store.Transition(prior.ID, []model.State{model.Queued}, model.Succeeded, "fixture complete"); err != nil {
				t.Fatal(err)
			}
			r, _, err := c.Store.Admit(model.Record{ID: "dispatched-old", TenantID: "local", CreatedAt: time.Now().Add(-tc.age), Spec: prior.Spec}, "", 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, next := range []model.State{model.Starting, model.Activating, model.Running} {
				r, err = c.Store.Transition(r.ID, []model.State{r.State}, next, "recent metadata progress does not reset admission age")
				if err != nil {
					t.Fatal(err)
				}
			}
			b.jobs[r.ID] = true
			c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) { return tc.ob, tc.err }}
			before := r
			err = c.Reconcile(context.Background())
			if (tc.err == nil) != (err == nil) {
				t.Fatalf("unexpected reconciliation result: %v", err)
			}
			got := currentRecord(t, c, r)
			if got.State != tc.state || got.ReasonCode != tc.code {
				t.Fatalf("incorrect startup precedence: %+v", got)
			}
			if tc.state == model.Running && !reflect.DeepEqual(got, before) {
				t.Fatal("non-expired or actually-started work changed its history")
			}
			if tc.state == model.Cancelling {
				if _, _, err := c.Store.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
					t.Fatal("startup failure released capacity before cleanup")
				}
				b.cancelPending = true
				tick(t, c)
				if !reflect.DeepEqual(currentRecord(t, c, r), got) {
					t.Fatal("pending cleanup rewrote startup cause")
				}
				b.cancelPending = false
				tick(t, c)
				if end := currentRecord(t, c, r); end.State != model.Failed || end.ReasonCode != tc.code {
					t.Fatalf("confirmed cleanup lost startup cause: %+v", end)
				}
			}
			if b.creates != 0 || b.calls != 0 {
				t.Fatal("startup handling replayed dispatched work")
			}
		})
	}
}
