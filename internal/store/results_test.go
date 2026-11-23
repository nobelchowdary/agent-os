package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func resultFixture() model.ExecutionResult {
	exit := int32(3)
	now := time.Now().UTC()
	return model.ExecutionResult{ExitCode: &exit, OutputHead: "hello\n", OutputTail: "traceback at end\n", OutputTruncated: true,
		OutputComplete: true, Status: "captured", CapturedAt: now, ExpiresAt: now.Add(time.Hour)}
}

func TestResultAttachmentSurvivesRestartWithoutRewritingTerminalHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := s.Admit(testRecord("tenant", "run"), "key", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Failed, "exit", model.ReasonTaskError); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := rawDataValues(t, path)
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first := resultFixture()
	if err := s.SaveResult(r.ID, first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.OutputTail = "must not overwrite"
	if err := s.SaveResult(r.ID, second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("foreign", r.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("result leaked across tenant", err)
	}
	got, err := s.Get("tenant", r.ID)
	if err != nil || !reflect.DeepEqual(got.Result, &first) {
		t.Fatalf("result mismatch: %+v %v", got.Result, err)
	}
	got.Result.OutputHead = "caller mutation"
	page, _, err := s.ListPage("tenant", 1, "")
	if err != nil || len(page) != 1 || page[0].Result != nil {
		t.Fatal("history listing included result payload", err)
	}
	retry, created, err := s.Admit(testRecord("tenant", "retry"), "key", 0, 0)
	if err != nil || created || !reflect.DeepEqual(retry.Result, &first) {
		t.Fatal("idempotent result lost", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rawDataValues(t, path)) {
		t.Fatal("result capture rewrote run/spec/event/index/idempotency bytes")
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reopened, err := s.Lookup(r.ID)
	if err != nil || !reflect.DeepEqual(reopened.Result, &first) {
		t.Fatal("result lost at restart", err)
	}
	if _, err := s.Transition(r.ID, []model.State{model.Failed}, model.Succeeded, "rewrite"); !errors.Is(err, ErrConflict) {
		t.Fatal("capture weakened terminal immutability", err)
	}
}

func TestResultValidationRejectsAdmissionAndInvalidCapture(t *testing.T) {
	s := openTestStore(t)
	r := testRecord("tenant", "run")
	r.Result = new(model.ExecutionResult)
	if _, _, err := s.Admit(r, "", 1, 1); err == nil {
		t.Fatal("caller result admitted")
	}
	r.Result = nil
	mustAdmit(t, s, r)
	base := resultFixture()
	if err := s.SaveResult("missing", base); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	for _, change := range []func(*model.ExecutionResult){
		func(v *model.ExecutionResult) { v.Status = "expired" }, func(v *model.ExecutionResult) { v.Status = "invented" },
		func(v *model.ExecutionResult) { v.OutputHead = string([]byte{255}) },
		func(v *model.ExecutionResult) { v.OutputHead = strings.Repeat("x", MaxResultOutputBytes) },
		func(v *model.ExecutionResult) { v.ExpiresAt = v.CapturedAt }, func(v *model.ExecutionResult) { v.CapturedAt = time.Time{} },
		func(v *model.ExecutionResult) { v.Status = "unavailable" },
	} {
		candidate := base
		change(&candidate)
		if err := s.SaveResult(r.ID, candidate); err == nil {
			t.Fatal("invalid result accepted")
		}
		if got, err := s.Get(r.TenantID, r.ID); err != nil || got.Result != nil {
			t.Fatal("invalid capture left partial result", err)
		}
	}
	unavailable := base
	unavailable.Status = "unavailable"
	unavailable.OutputHead = ""
	unavailable.OutputTail = ""
	unavailable.OutputComplete = false
	unavailable.OutputTruncated = false
	if err := s.SaveResult(r.ID, unavailable); err != nil {
		t.Fatal(err)
	}
	active, err := s.Active()
	if err != nil || len(active) != 1 || active[0].Result == nil || active[0].Result.ExitCode == nil || *active[0].Result.ExitCode != 3 {
		t.Fatal("known exit status lost with unavailable output", err)
	}
}

func TestResultExpirySuppressesOutputBeforePruningAndCannotResurrect(t *testing.T) {
	s := openTestStore(t)
	for _, id := range []string{"first", "second"} {
		r := mustAdmit(t, s, testRecord("tenant", id))
		result := resultFixture()
		result.CapturedAt = time.Now().UTC().Add(-2 * time.Hour)
		result.ExpiresAt = result.CapturedAt.Add(time.Hour)
		if err := s.SaveResult(r.ID, result); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get("tenant", r.ID)
		if err != nil || got.Result.Status != "expired" || got.Result.OutputHead != "" || got.Result.OutputTail != "" || *got.Result.ExitCode != 3 {
			t.Fatal("expired output returned before prune", err)
		}
	}
	if n, err := s.ExpireResults(time.Now()); err != nil || n != 2 {
		t.Fatalf("pruned%d %v", n, err)
	}
	if n, err := s.ExpireResults(time.Now()); err != nil || n != 0 {
		t.Fatal("repeated expiry not idempotent", n, err)
	}
	if err := s.SaveResult("first", resultFixture()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket(resultsBucket).Get([]byte("first"))
		if bytes.Contains(value, []byte("hello")) || bytes.Contains(value, []byte("traceback")) {
			t.Fatal("prune retained output bytes in attachment")
		}
		var got model.ExecutionResult
		if json.Unmarshal(value, &got) != nil || got.Status != "expired" || got.ExitCode == nil || *got.ExitCode != 3 {
			t.Fatal("expired result resurrected or lost exit status")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaTwoUpgradePreservesEpochCursorsAndOldData(t *testing.T) {
	path, records := closedStoreFixture(t, false)
	var epoch string
	mutateFile(t, path, func(tx *bolt.Tx) error {
		epoch = string(tx.Bucket(metadataBucket).Get(epochKey))
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, 2)
		if err := tx.Bucket(metadataBucket).Put(schemaKey, value); err != nil {
			return err
		}
		return tx.DeleteBucket(resultsBucket)
	})
	cursor := encodeCursor(records[0].TenantID, orderKey(records[0]), epoch)
	before := rawDataValues(t, path)
	digest := fileDigest(t, path)
	if info, err := CheckOffline(path); err != nil || info.SchemaVersion != 2 || info.StoreEpoch != epoch || fileDigest(t, path) != digest {
		t.Fatal("schema2 check changed source", err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := s.Epoch(); err != nil || got != epoch {
		t.Fatal("schema-only upgrade changed history epoch", err)
	}
	if _, _, err := s.ListPage(records[0].TenantID, 1, cursor); err != nil {
		t.Fatal("schema-only upgrade invalidated cursor", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rawDataValues(t, path)) {
		t.Fatal("upgrade changed old run data")
	}
}

func TestResultBackupRestoreRetainsPayloadAndRejectsOrphanAttachment(t *testing.T) {
	path, records := closedStoreFixture(t, false)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	result := resultFixture()
	if err := s.SaveResult(records[0].ID, result); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(privateTestDir(t), "backup.db")
	info, err := BackupOffline(path, backup)
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(privateTestDir(t), "restored.db")
	if _, err := RestoreOffline(backup, restored, info.SHA256); err != nil {
		t.Fatal(err)
	}
	s, err = Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(records[0].TenantID, records[0].ID)
	if err != nil || !reflect.DeepEqual(got.Result, &result) {
		t.Fatal("backup restore lost result", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	mutateFile(t, restored, func(tx *bolt.Tx) error {
		encoded, _ := json.Marshal(result)
		return tx.Bucket(resultsBucket).Put([]byte("not-a-run"), encoded)
	})
	if _, err := CheckOffline(restored); !errors.Is(err, ErrIntegrity) {
		t.Fatal("orphan result accepted", err)
	}
}

func TestMissingResultBucketFailsClosedWithoutMigratingCurrentSchema(t *testing.T) {
	path, _ := closedStoreFixture(t, false)
	mutateFile(t, path, func(tx *bolt.Tx) error { return tx.DeleteBucket(resultsBucket) })
	before := fileDigest(t, path)
	if s, err := Open(path); !errors.Is(err, ErrSchema) {
		if s != nil {
			_ = s.Close()
		}
		t.Fatal("missing result bucket was repaired", err)
	}
	if fileDigest(t, path) != before {
		t.Fatal("rejected schema changed bytes")
	}
	if _, err := CheckOffline(path); !errors.Is(err, ErrSchema) {
		t.Fatal("offline check accepted incomplete result layout", err)
	}
}
