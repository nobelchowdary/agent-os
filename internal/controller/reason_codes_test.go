package controller

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
	bolt "go.etcd.io/bbolt"
)

func currentRecord(t *testing.T, c *Controller, r model.Record) model.Record {
	t.Helper()
	got, err := c.Store.Get(r.TenantID, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestFailureCausePersistsThroughPendingCleanupAndControllerRestart(t *testing.T) {
	for _, code := range []model.ReasonCode{model.ReasonOOMKilled, model.ReasonTaskError, model.ReasonEvicted,
		model.ReasonImagePullFailed, model.ReasonDeadlineExceeded, model.ReasonInfraFailure, model.ReasonUnknown} {
		t.Run(string(code), func(t *testing.T) {
			c, b, r := setup(t)
			tick(t, c)
			tick(t, c)
			c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) {
				return backend.Observation{State: model.Cancelling, Reason: "cleanup before failure", ReasonCode: code}, nil
			}}
			b.cancelPending = true
			tick(t, c)
			intent := currentRecord(t, c, r)
			if intent.State != model.Cancelling || intent.ReasonCode != code || intent.Events[len(intent.Events)-1].ReasonCode != code {
				t.Fatalf("cleanup cause was not persisted: %+v", intent)
			}
			// A new controller instance reads the durable cause; it must not
			// reconstruct intent from prose or acknowledge pending cleanup.
			restarted := &Controller{Store: c.Store, Backend: b, Logger: c.Logger}
			tick(t, restarted)
			if got := currentRecord(t, restarted, r); !reflect.DeepEqual(got, intent) {
				t.Fatal("pending cleanup altered the persisted intent/history")
			}
			if _, _, err := c.Store.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
				t.Fatalf("capacity released before cleanup: %v", err)
			}
			b.cancelPending = false
			tick(t, restarted)
			got := currentRecord(t, restarted, r)
			if got.State != model.Failed || got.ReasonCode != code || got.Events[len(got.Events)-1].ReasonCode != code || b.creates != 1 || b.calls != 1 {
				t.Fatalf("cleanup lost cause or replayed execution: %+v", got)
			}
		})
	}
}

func TestCancellationCodeSurvivesLostCleanupAcknowledgment(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	if _, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "human cancellation with different prose", model.ReasonCancelled); err != nil {
		t.Fatal(err)
	}
	lost := errors.New("cleanup acknowledgment lost")
	c.Backend = &hookedBackend{Backend: b, cancel: func(ctx context.Context, id string) error {
		if err := b.Cancel(ctx, id); err != nil {
			return err
		}
		return lost // Deletion committed; the reply did not reach the controller.
	}}
	if err := c.Reconcile(context.Background()); !errors.Is(err, lost) {
		t.Fatalf("uncertain cleanup did not return its retryable error: %v", err)
	}
	got := currentRecord(t, c, r)
	if got.State != model.Cancelling || got.ReasonCode != model.ReasonCancelled {
		t.Fatal("ambiguous cleanup released or reclassified cancellation")
	}
	if _, _, err := c.Store.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
		t.Fatal("ambiguous cleanup released the reservation")
	}
	restarted := &Controller{Store: c.Store, Backend: b, Logger: c.Logger}
	tick(t, restarted)
	got = currentRecord(t, restarted, r)
	if got.State != model.Cancelled || got.ReasonCode != model.ReasonCancelled {
		t.Fatalf("confirmed cleanup did not retain cancellation cause: %+v", got)
	}
	terminal := got
	if _, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Cancelled}, model.Cancelled, "replace outcome", model.ReasonInfraFailure); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(currentRecord(t, c, r), terminal) {
		t.Fatal("terminal no-op rewrote cancellation cause/history")
	}
}

func TestCleanupUsesStructuredIntentAndOnlyLegacyFallbackUsesProse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   model.ReasonCode
		reason string
		state  model.State
		want   model.ReasonCode
	}{
		{"explicit cancel different prose", model.ReasonCancelled, "operator requested stop", model.Cancelled, model.ReasonCancelled},
		{"infra code wins over cancellation prose", model.ReasonInfraFailure, "cancel requested", model.Failed, model.ReasonInfraFailure},
		{"unknown code does not use prose fallback", model.ReasonUnknown, "cancel requested", model.Failed, model.ReasonUnknown},
		{"legacy cancellation compatibility", "", "cancel requested", model.Cancelled, model.ReasonCancelled},
		{"legacy failure remains unknown", "", "OOMKilled guessed from old prose", model.Failed, model.ReasonUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, r := setup(t)
			if _, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Cancelling, tc.reason, tc.code); err != nil {
				t.Fatal(err)
			}
			tick(t, c)
			if got := currentRecord(t, c, r); got.State != tc.state || got.ReasonCode != tc.want {
				t.Fatalf("unexpected cleanup classification: %+v", got)
			}
		})
	}
}

func TestStartupWaitAndAuthoritativeOutcomesKeepDistinctCauses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ob    backend.Observation
		err   error
		state model.State
		code  model.ReasonCode
	}{
		{"unavailable infrastructure", backend.Observation{}, backend.ErrNotFound, model.Cancelling, model.ReasonInfraFailure},
		{"expired transient image pull", backend.Observation{State: model.Starting, ReasonCode: model.ReasonImagePullFailed}, nil, model.Cancelling, model.ReasonImagePullFailed},
		{"completed after downtime", backend.Observation{State: model.Succeeded, ReasonCode: model.ReasonCompleted}, nil, model.Succeeded, model.ReasonCompleted},
		{"actual deadline after downtime", backend.Observation{State: model.Failed, ReasonCode: model.ReasonDeadlineExceeded}, nil, model.Failed, model.ReasonDeadlineExceeded},
		{"owned task failure after downtime", backend.Observation{State: model.Failed, ReasonCode: model.ReasonTaskError}, nil, model.Failed, model.ReasonTaskError},
		{"deadline cleanup target after downtime", backend.Observation{State: model.Cancelling, ReasonCode: model.ReasonDeadlineExceeded}, nil, model.Cancelling, model.ReasonDeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, b, _ := setup(t)
			r, _, err := c.Store.Admit(model.Record{ID: "expired", TenantID: "other", CreatedAt: time.Now().Add(-10 * time.Minute), Spec: model.Spec{TimeoutSeconds: 60}}, "", 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) {
				return tc.ob, tc.err
			}}
			tick(t, c)
			got := currentRecord(t, c, r)
			if got.State != tc.state || got.ReasonCode != tc.code {
				t.Fatalf("startup policy overwrote observed outcome: %+v", got)
			}
			if tc.state == model.Cancelling {
				tick(t, c)
				if got := currentRecord(t, c, r); got.State != model.Failed || got.ReasonCode != tc.code {
					t.Fatalf("startup cleanup lost cause: %+v", got)
				}
			}
		})
	}
}

func TestTransientImagePullCanRecoverWithoutCleanupOrCauseRewrite(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	ob := backend.Observation{State: model.Starting, ReasonCode: model.ReasonImagePullFailed}
	c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) { return ob, nil }}
	before := currentRecord(t, c, r)
	tick(t, c)
	if got := currentRecord(t, c, r); !reflect.DeepEqual(got, before) {
		t.Fatal("transient image wait caused cleanup or persisted a terminal cause")
	}
	ob = backend.Observation{State: model.Succeeded, ReasonCode: model.ReasonCompleted}
	tick(t, c)
	if got := currentRecord(t, c, r); got.State != model.Succeeded || got.ReasonCode != model.ReasonCompleted {
		t.Fatalf("transient image pull could not recover: %+v", got)
	}
}

func TestUnspecifiedFailureCauseDoesNotParseBackendProse(t *testing.T) {
	c, b, r := setup(t)
	tick(t, c)
	tick(t, c)
	c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) {
		return backend.Observation{State: model.Failed, Reason: "OOMKilled deadline_exceeded cancelled"}, nil
	}}
	tick(t, c)
	got := currentRecord(t, c, r)
	if got.State != model.Failed || got.ReasonCode != model.ReasonUnknown || got.Events[len(got.Events)-1].ReasonCode != model.ReasonUnknown {
		t.Fatalf("missing cause was inferred from prose: %+v", got)
	}
}

func TestMissingExecutionUsesInfrastructureCauseWithoutReplay(t *testing.T) {
	for _, activating := range []bool{false, true} {
		c, b, r := setup(t)
		b.activationAmbiguous = activating
		tick(t, c)
		if activating {
			retry(t, c)
		} else {
			tick(t, c)
		}
		delete(b.jobs, r.ID)
		tick(t, c)
		if got := currentRecord(t, c, r); got.State != model.Cancelling || got.ReasonCode != model.ReasonInfraFailure {
			t.Fatalf("missing execution cause not persisted: %+v", got)
		}
		tick(t, c)
		if got := currentRecord(t, c, r); got.State != model.Failed || got.ReasonCode != model.ReasonInfraFailure || b.creates != 1 {
			t.Fatalf("missing execution replayed or lost cause: %+v", got)
		}
	}
}

func TestInvalidObservationCodeFailsHealthWithoutChangingRunOrQuota(t *testing.T) {
	for _, ob := range []backend.Observation{
		{State: model.Failed, ReasonCode: "sensitive-invalid-code"},
		{State: model.Failed, ReasonCode: model.ReasonCompleted},
		{State: model.Failed, ReasonCode: model.ReasonCancelled},
		{State: model.Succeeded, ReasonCode: model.ReasonTaskError},
		{State: model.Cancelling, ReasonCode: model.ReasonCompleted},
		{State: model.Running, ReasonCode: model.ReasonUnknown},
		{State: model.Starting, ReasonCode: model.ReasonTaskError},
	} {
		t.Run(string(ob.State)+"/"+string(ob.ReasonCode), func(t *testing.T) {
			c, b, r := setup(t)
			tick(t, c)
			tick(t, c)
			before := currentRecord(t, c, r)
			lastSuccess := c.Status().LastSuccessAt
			cancelled := false
			c.Backend = &hookedBackend{Backend: b,
				observe: func(context.Context, string) (backend.Observation, error) { return ob, nil },
				cancel:  func(context.Context, string) error { cancelled = true; return nil },
			}
			for count := uint64(1); count <= 2; count++ {
				err := c.Reconcile(context.Background())
				if err == nil || errors.Is(err, store.ErrConflict) || strings.Contains(err.Error(), "sensitive-invalid-code") {
					t.Fatalf("invalid observation was hidden or exposed: %v", err)
				}
				if got := currentRecord(t, c, r); !reflect.DeepEqual(got, before) || cancelled {
					t.Fatal("invalid observation changed history or triggered cleanup")
				}
				if _, _, err := c.Store.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
					t.Fatal("invalid observation released its reservation")
				}
				if status := c.Status(); status.ConsecutiveFailures != count || !status.LastSuccessAt.Equal(lastSuccess) {
					t.Fatalf("invalid observation was reported healthy: %+v", status)
				}
			}
			c.Backend = b
			tick(t, c)
			if c.Status().ConsecutiveFailures != 0 {
				t.Fatal("valid observation did not recover controller health")
			}
		})
	}
}

func TestInvalidStartupObservationDoesNotBecomeCleanupIntent(t *testing.T) {
	c, b, _ := setup(t)
	r, _, err := c.Store.Admit(model.Record{ID: "expired-invalid", TenantID: "other", CreatedAt: time.Now().Add(-10 * time.Minute)}, "", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	c.Backend = &hookedBackend{Backend: b, observe: func(context.Context, string) (backend.Observation, error) {
		return backend.Observation{State: model.Starting, ReasonCode: model.ReasonTaskError}, nil
	}}
	if err := c.Reconcile(context.Background()); err == nil || errors.Is(err, store.ErrConflict) {
		t.Fatalf("invalid startup observation was hidden: %v", err)
	}
	if got := currentRecord(t, c, r); !reflect.DeepEqual(got, r) || c.Status().ConsecutiveFailures != 1 {
		t.Fatal("invalid startup observation changed history or stayed healthy")
	}
}

func TestInvalidPersistedCleanupCodeIsRejectedBeforeDeletion(t *testing.T) {
	for _, code := range []model.ReasonCode{"sensitive-invalid-code", model.ReasonCompleted} {
		t.Run(string(code), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state", "runs.db")
			db, err := store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			r, _, err := db.Admit(model.Record{ID: "corrupt-cleanup", TenantID: "local"}, "", 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			r, err = db.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Cancelling, "cleanup requested", model.ReasonInfraFailure)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// Simulate invalid pre-existing data; the production transition API
			// correctly refuses to create this state/code combination.
			r.ReasonCode = code
			r.Events[len(r.Events)-1].ReasonCode = code
			encoded, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			corrupt, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = corrupt.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("records")).Put([]byte(r.ID), encoded) })
			closeErr := corrupt.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("corruption fixture: %v, %v", err, closeErr)
			}
			db, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			cancelled := false
			b := &hookedBackend{cancel: func(context.Context, string) error { cancelled = true; return nil }}
			c := &Controller{Store: db, Backend: b}
			if err := c.Reconcile(context.Background()); err == nil || errors.Is(err, store.ErrConflict) || strings.Contains(err.Error(), "sensitive-invalid-code") {
				t.Fatalf("invalid persisted cleanup code was hidden or exposed: %v", err)
			}
			if cancelled || !reflect.DeepEqual(currentRecord(t, c, r), r) || c.Status().ConsecutiveFailures != 1 {
				t.Fatal("invalid cleanup intent caused deletion, mutation or healthy status")
			}
			if _, _, err := db.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
				t.Fatal("invalid persisted cleanup intent released quota")
			}
		})
	}
}
