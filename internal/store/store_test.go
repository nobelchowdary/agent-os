package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "private", "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testRecord(tenant, id string) model.Record {
	return model.Record{
		ID: id, TenantID: tenant, State: model.Queued,
		Spec: model.Spec{Template: "test", Image: "example.invalid/agent@sha256:abc", Command: []string{"echo", "hello"}, TimeoutSeconds: 60},
	}
}

func mustAdmit(t *testing.T, s *Store, record model.Record) model.Record {
	t.Helper()
	got, created, err := s.Admit(record, "", 100, 100)
	if err != nil || !created {
		t.Fatalf("Admit: created=%v, err=%v", created, err)
	}
	return got
}

func TestPersistenceAndIdempotencyAfterCompletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	original, created, err := s.Admit(testRecord("tenant", "original"), "request", 1, 1)
	if err != nil || !created {
		t.Fatalf("Admit: created=%v, err=%v", created, err)
	}
	if _, err := s.Transition(original.ID, []model.State{model.Queued}, model.Running, "started"); err != nil {
		t.Fatal(err)
	}
	finished, err := s.Transition(original.ID, []model.State{model.Running}, model.Succeeded, "done")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get("tenant", original.ID)
	if err != nil || !reflect.DeepEqual(got, finished) {
		t.Fatalf("reopened record = %#v, err=%v; want %#v", got, err, finished)
	}
	// Capacity is full again, but a retry must still return its original result.
	mustAdmit(t, s, testRecord("tenant", "occupier"))
	retry, created, err := s.Admit(testRecord("tenant", "new-request-id"), "request", 1, 1)
	if err != nil || created || !reflect.DeepEqual(retry, finished) {
		t.Fatalf("retry = %#v, created=%v, err=%v", retry, created, err)
	}
	if _, created, err := s.Admit(testRecord("tenant", "retry-with-admission-disabled"), "request", 0, 0); err != nil || created {
		t.Fatalf("idempotency must precede current admission limits: created=%v, err=%v", created, err)
	}
	active, err := s.Active()
	if err != nil || len(active) != 1 || active[0].ID != "occupier" {
		t.Fatalf("active = %#v, err=%v", active, err)
	}
	for _, item := range []struct {
		path string
		mode os.FileMode
	}{{filepath.Dir(path), 0700}, {path, 0600}} {
		info, err := os.Stat(item.path)
		if err != nil || info.Mode().Perm() != item.mode {
			t.Fatalf("permissions %s: info=%v, err=%v", item.path, info, err)
		}
	}
}

func TestTenantIsolationAndNewestFirst(t *testing.T) {
	s := openTestStore(t)
	now := time.Now().UTC()
	for i, value := range []struct{ tenant, id string }{{"a/b", "late"}, {"a", "b/late"}, {"a/b", "early"}} {
		record := testRecord(value.tenant, value.id)
		record.CreatedAt = now.Add(time.Duration(3-i) * time.Second)
		mustAdmit(t, s, record)
	}
	if _, err := s.Get("a", "late"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant Get: %v", err)
	}
	if _, err := s.Get("", "late"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty-tenant Get: %v", err)
	}
	for _, tenant := range []string{"a/b", "a", "", "missing"} {
		runs, err := s.List(tenant, 100)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]int{"a/b": 2, "a": 1, "": 0, "missing": 0}[tenant]
		if len(runs) != want {
			t.Fatalf("tenant %q list: got %d, want %d", tenant, len(runs), want)
		}
		for _, record := range runs {
			if record.TenantID != tenant {
				t.Fatalf("tenant leaked in list: %#v", record)
			}
		}
	}
	runs, err := s.List("a/b", 1)
	if err != nil || len(runs) != 1 || runs[0].ID != "late" {
		t.Fatalf("newest first/limit: %#v, err=%v", runs, err)
	}
}

func TestConcurrentQuotaAdmission(t *testing.T) {
	for _, test := range []struct {
		name             string
		tenant, global   int
		differentTenants bool
		want             int64
	}{{"tenant", 5, 100, false, 5}, {"global", 100, 7, true, 7}} {
		t.Run(test.name, func(t *testing.T) {
			s := openTestStore(t)
			var admitted atomic.Int64
			var wg sync.WaitGroup
			for i := range 100 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					tenant := "tenant"
					if test.differentTenants {
						tenant = fmt.Sprintf("tenant-%d", i)
					}
					_, created, err := s.Admit(testRecord(tenant, fmt.Sprintf("run-%d", i)), "", test.tenant, test.global)
					if err == nil && created {
						admitted.Add(1)
					} else if !errors.Is(err, ErrQuota) {
						t.Errorf("unexpected admission result: created=%v, err=%v", created, err)
					}
				}(i)
			}
			wg.Wait()
			active, err := s.Active()
			if admitted.Load() != test.want || err != nil || int64(len(active)) != test.want {
				t.Fatalf("admitted=%d, active=%d, err=%v; want %d", admitted.Load(), len(active), err, test.want)
			}
		})
	}
}

func TestCancellingRetainsQuotaUntilTerminal(t *testing.T) {
	s := openTestStore(t)
	if _, _, err := s.Admit(testRecord("tenant", "first"), "", 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition("first", []model.State{model.Queued}, model.Cancelling, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Admit(testRecord("tenant", "second"), "rejected", 1, 1); !errors.Is(err, ErrQuota) {
		t.Fatalf("cancelling did not hold quota: %v", err)
	}
	if _, err := s.Transition("first", []model.State{model.Cancelling}, model.Cancelled, "backend stopped"); err != nil {
		t.Fatal(err)
	}
	second := testRecord("tenant", "second")
	second.Spec.Command = []string{"changed-after-rejection"}
	if _, created, err := s.Admit(second, "rejected", 1, 1); err != nil || !created {
		t.Fatalf("terminal did not release quota or failed transaction retained key: created=%v, err=%v", created, err)
	}
}

func TestIdempotencyScopeFingerprintAndConcurrency(t *testing.T) {
	s := openTestStore(t)
	var createdCount atomic.Int64
	var wg sync.WaitGroup
	for i := range 30 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, created, err := s.Admit(testRecord("a", fmt.Sprintf("retry-%d", i)), "same-key", 1, 100)
			if err != nil || got.TenantID != "a" {
				t.Errorf("retry admission: %#v, err=%v", got, err)
			}
			if created {
				createdCount.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if createdCount.Load() != 1 {
		t.Fatalf("created %d records for the same key", createdCount.Load())
	}
	if _, created, err := s.Admit(testRecord("b", "other-tenant"), "same-key", 1, 100); err != nil || !created {
		t.Fatalf("cross-tenant key collision: created=%v, err=%v", created, err)
	}
	changed := testRecord("a", "mismatched")
	changed.Spec.Command = []string{"different"}
	if _, _, err := s.Admit(changed, "same-key", 1, 100); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("mismatched spec: %v", err)
	}
	for _, id := range []string{"no-key-one", "no-key-two"} {
		if _, created, err := s.Admit(testRecord("c", id), "", 100, 100); err != nil || !created {
			t.Fatalf("empty idempotency key: created=%v, err=%v", created, err)
		}
	}
}

func TestTransitionCASNoopAndTerminalImmutability(t *testing.T) {
	s := openTestStore(t)
	input := testRecord("tenant", "run")
	original := mustAdmit(t, s, input)
	input.Spec.Command[0] = "mutated-input"
	original.Spec.Command[0] = "mutated-result"
	if _, err := s.Transition("run", []model.State{model.Running}, model.Succeeded, "invalid expected state"); !errors.Is(err, ErrConflict) {
		t.Fatalf("CAS mismatch: %v", err)
	}
	unchanged, err := s.Transition("run", []model.State{model.Queued}, model.Queued, "must not be saved")
	if err != nil || len(unchanged.Events) != 1 || unchanged.Reason != "" || unchanged.Spec.Command[0] != "echo" {
		t.Fatalf("no-op or immutable spec: %#v, err=%v", unchanged, err)
	}
	if _, err := s.Transition("run", []model.State{model.Queued}, model.Cancelling, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int64
	var wg sync.WaitGroup
	for _, terminal := range []model.State{model.Failed, model.Cancelled} {
		wg.Add(1)
		go func(terminal model.State) {
			defer wg.Done()
			_, err := s.Transition("run", []model.State{model.Cancelling}, terminal, strings.Repeat("界", 1000))
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Errorf("terminal race: %v", err)
			}
		}(terminal)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("CAS allowed %d competing completions", wins.Load())
	}
	finished, err := s.Get("tenant", "run")
	if err != nil || len(finished.Events) != 3 || len(finished.Reason) > maxReasonBytes || !utf8.ValidString(finished.Reason) {
		t.Fatalf("completion history: %#v, err=%v", finished, err)
	}
	for _, target := range []model.State{model.Queued, model.Starting, model.Activating, model.Running, model.Cancelling, model.Succeeded, model.Failed, model.Cancelled} {
		got, err := s.Transition("run", []model.State{finished.State}, target, "change terminal")
		if target == finished.State {
			if err != nil || !reflect.DeepEqual(got, finished) {
				t.Fatalf("terminal noop changed history: %#v, err=%v", got, err)
			}
		} else if !errors.Is(err, ErrConflict) {
			t.Fatalf("terminal %s -> %s: %v", finished.State, target, err)
		}
	}
	active, err := s.Active()
	if err != nil || len(active) != 0 {
		t.Fatalf("terminal remained active: %#v, err=%v", active, err)
	}
}

func TestOpenRejectsSharedDirectoryAndSymlink(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(filepath.Join(dir, "runs.db")); err == nil {
		s.Close()
		t.Fatal("accepted shared database directory")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "runs.db")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(link); err == nil {
		s.Close()
		t.Fatal("accepted a symlink database file")
	}
}

func TestOpenLockHasFiniteTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	started := time.Now()
	other, err := Open(path)
	if err == nil {
		other.Close()
		t.Fatal("opened the same database twice")
	}
	if !errors.Is(err, bolt.ErrTimeout) || time.Since(started) > 5*time.Second {
		t.Fatalf("expected bounded lock timeout, elapsed=%s, err=%v", time.Since(started), err)
	}
}
