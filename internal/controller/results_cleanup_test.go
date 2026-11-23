package controller

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

type resultsBackend struct {
	*fakeBackend
	db                    *store.Store
	resultMu              sync.Mutex
	captureErr            error
	captureCalls          int
	exitCode              int32
	cleanupErrAfterDelete bool
	cleanupCalls          int
}

func (b *resultsBackend) CaptureResult(context.Context, string) (model.ExecutionResult, error) {
	b.resultMu.Lock()
	defer b.resultMu.Unlock()
	b.captureCalls++
	if b.captureErr != nil {
		return model.ExecutionResult{}, b.captureErr
	}
	exit := b.exitCode
	return model.ExecutionResult{ExitCode: &exit, OutputHead: "hello from task\n", OutputTail: "final traceback\n", OutputTruncated: true, OutputComplete: true, Status: "captured"}, nil
}

func (b *resultsBackend) ListExecutions(context.Context) ([]backend.Execution, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var jobs []backend.Execution
	for id := range b.jobs {
		jobs = append(jobs, backend.Execution{ID: id})
	}
	return jobs, nil
}

func (b *resultsBackend) Cleanup(ctx context.Context, r model.Record) error {
	persisted, err := b.db.Lookup(r.ID)
	if err != nil {
		return err
	}
	if (!persisted.State.Terminal() && persisted.State != model.Cancelling) || persisted.Result == nil {
		return errors.New("deletion before durable state and result")
	}
	b.resultMu.Lock()
	b.cleanupCalls++
	loseAck := b.cleanupErrAfterDelete
	b.cleanupErrAfterDelete = false
	b.resultMu.Unlock()
	if err := b.fakeBackend.Cancel(ctx, r.ID); err != nil {
		return err
	}
	if loseAck {
		return errors.New("lost deletion acknowledgment")
	}
	return nil
}

func TestTwoHundredSequentialRunsSucceedAndPersistResultBeforeCleanup(t *testing.T) {
	c, b, initial := setup(t)
	if _, err := c.Store.Transition(initial.ID, []model.State{model.Queued}, model.Succeeded, ""); err != nil {
		t.Fatal(err)
	}
	runtime := &resultsBackend{fakeBackend: b, db: c.Store}
	c.Backend = runtime
	b.observed = model.Succeeded
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("sequential-%03d", i)
		r, created, err := c.Store.Admit(model.Record{ID: id, TenantID: "local", Spec: initial.Spec}, id, 1, 1)
		if err != nil || !created {
			t.Fatal(i, created, err)
		}
		for n := 0; n < 3; n++ {
			tick(t, c)
		}
		finished, err := c.Store.Get("local", r.ID)
		if err != nil || finished.State != model.Succeeded || finished.Result == nil || finished.Result.ExitCode == nil || *finished.Result.ExitCode != 0 || finished.Result.OutputHead != "hello from task\n" || finished.Result.OutputTail != "final traceback\n" {
			t.Fatal(i, finished.State, err)
		}
		b.mu.Lock()
		left := len(b.jobs)
		b.mu.Unlock()
		if left != 0 {
			t.Fatal("finished execution leaked", i, left)
		}
	}
	if b.creates != 200 || runtime.cleanupCalls != 200 || runtime.captureCalls != 200 {
		t.Fatal("unexpected execution/cleanup counts", b.creates, runtime.cleanupCalls, runtime.captureCalls)
	}
}

func TestTerminalCleanupSweepSurvivesRestartAndNeverAdoptsUnknownJobs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := db.Admit(model.Record{ID: "finished", TenantID: "tenant"}, "key", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Model a crash after terminal commit, before result capture/deletion.
	before, err := db.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Failed, "task failed", model.ReasonTaskError)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	b := &resultsBackend{fakeBackend: &fakeBackend{jobs: map[string]bool{r.ID: true, "never-admitted": true}}, db: db, cleanupErrAfterDelete: true, exitCode: 3}
	c := &Controller{Store: db, Backend: b, SweepInterval: time.Nanosecond}
	if err := c.Reconcile(context.Background()); err == nil {
		t.Fatal("ambiguous deletion must be reported")
	}
	finished, err := db.Get("tenant", r.ID)
	if err != nil || finished.State != before.State || len(finished.Events) != len(before.Events) || finished.ReasonCode != before.ReasonCode || finished.Result == nil {
		t.Fatal("cleanup changed outcome or lost result", err)
	}
	// A delayed suspended creation after that lost response is swept too.
	b.jobs[r.ID] = true
	tick(t, c)
	if b.jobs[r.ID] || !b.jobs["never-admitted"] || b.creates != 0 {
		t.Fatal("sweep replayed/adopted work", b.jobs, b.creates)
	}
	if c.Status().OrphanJobs != 1 {
		t.Fatal("unknown labeled Job was not reported", c.Status())
	}
	copyAfter, err := db.Get("tenant", r.ID)
	if err != nil || len(copyAfter.Events) != len(before.Events) || copyAfter.Result.OutputHead != "hello from task\n" || copyAfter.Result.ExitCode == nil || *copyAfter.Result.ExitCode != 3 {
		t.Fatal("restart/cleanup lost durable result", err)
	}
}

func TestCaptureFailurePreservesFinishedJobButDoesNotBlockCancellation(t *testing.T) {
	for _, cancelling := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelling), func(t *testing.T) {
			c, b, r := setup(t)
			for i := 0; i < 2; i++ {
				tick(t, c)
			}
			runtime := &resultsBackend{fakeBackend: b, db: c.Store, captureErr: errors.New("logs temporarily unavailable")}
			c.Backend = runtime
			if cancelling {
				if _, err := c.Store.TransitionWithCode(r.ID, []model.State{model.Running}, model.Cancelling, "cancel requested", model.ReasonCancelled); err != nil {
					t.Fatal(err)
				}
			} else {
				b.observed = model.Failed
			}
			err := c.Reconcile(context.Background())
			got, _ := c.Store.Get(r.TenantID, r.ID)
			if cancelling {
				if err != nil || got.State != model.Cancelled || got.Result == nil || got.Result.Status != "unavailable" || b.jobs[r.ID] {
					t.Fatal("logs failure blocked cancellation", got.State, err)
				}
			} else {
				if err == nil || got.State != model.Running || !b.jobs[r.ID] || runtime.cleanupCalls != 0 {
					t.Fatal("unpersisted result allowed deletion", got.State, err)
				}
				runtime.captureErr = nil
				tick(t, c)
				got, _ = c.Store.Get(r.TenantID, r.ID)
				if got.State != model.Failed || got.Result == nil || b.jobs[r.ID] {
					t.Fatal("capture recovery did not finish", got.State)
				}
			}
		})
	}
}

func TestConfiguredStartupWaitCanExpireBeforeDispatch(t *testing.T) {
	c, b, r := setup(t)
	c.StartupTimeout = time.Nanosecond
	tick(t, c)
	if state(t, c, r) != model.Cancelling || b.creates != 0 {
		t.Fatal("configured startup bound ignored")
	}
	if _, _, err := c.Store.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
		t.Fatal("startup intent released quota before cleanup", err)
	}
	tick(t, c)
	if state(t, c, r) != model.Failed {
		t.Fatal("expired startup did not finish after cleanup")
	}
}

type cancellationStoreFailureBackend struct {
	*fakeBackend
	waitForCaptureDeadline bool
}

func (b *cancellationStoreFailureBackend) CaptureResult(ctx context.Context, id string) (model.ExecutionResult, error) {
	b.mu.Lock()
	present := b.jobs[id]
	b.mu.Unlock()
	if present {
		if b.waitForCaptureDeadline {
			<-ctx.Done()
			return model.ExecutionResult{Status: "unavailable"}, ctx.Err()
		}
		return model.ExecutionResult{Status: "captured", OutputHead: "before cancellation\n"}, nil
	}
	return model.ExecutionResult{Status: "unavailable"}, nil
}

func (b *cancellationStoreFailureBackend) Cleanup(ctx context.Context, r model.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return b.fakeBackend.Cancel(ctx, r.ID)
}

func (b *cancellationStoreFailureBackend) ListExecutions(context.Context) ([]backend.Execution, error) {
	return nil, nil
}

func TestResultPersistenceFailureDoesNotPreventDurableCancellationCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := db.Admit(model.Record{ID: "cancel-store-failure", TenantID: "local"}, "key", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	r, err = db.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Cancelling, "cancel requested", model.ReasonCancelled)
	if err != nil {
		t.Fatal(err)
	}
	b := &cancellationStoreFailureBackend{fakeBackend: &fakeBackend{jobs: map[string]bool{r.ID: true}}, waitForCaptureDeadline: true}
	c := &Controller{Store: db, Backend: b}
	// Simulate attachment persistence becoming unavailable after the controller
	// read this already durable cancellation intent, before its next write. The
	// log read also consumes its entire budget: cleanup needs a fresh context.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileRun(context.Background(), r); err == nil {
		t.Fatal("result persistence failure was hidden")
	}
	if b.jobs[r.ID] {
		t.Fatal("result persistence failure prevented workload cleanup")
	}
	db, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c.Store = db
	persisted, err := db.Get(r.TenantID, r.ID)
	if err != nil || persisted.State != model.Cancelling || persisted.Result != nil {
		t.Fatal("failed persistence rewrote cancellation state or result", err)
	}
	if _, _, err := db.Admit(model.Record{ID: "quota-probe", TenantID: r.TenantID}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
		t.Fatal("failed persistence released cancellation reservation", err)
	}
	tick(t, c)
	persisted, err = db.Get(r.TenantID, r.ID)
	if err != nil || persisted.State != model.Cancelled || persisted.Result == nil || persisted.Result.Status != "unavailable" || b.creates != 0 {
		t.Fatal("recovery did not finalize without replay", err)
	}
}
