package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func TestLegacyDeadlineRetryReturnsUnchangedAdmissionAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	original, created, err := s.Admit(testRecord("tenant", "original"), "key", 1, 1)
	if err != nil || !created {
		t.Fatal(created, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := rawDataValues(t, path)
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	changed := s.Changed()
	for _, scope := range []model.DeadlineScope{model.DeadlinePod, model.DeadlineJob} {
		retry := testRecord("tenant", "retry")
		retry.Spec.DeadlineScope = scope
		got, created, err := s.Admit(retry, "key", 0, 0)
		if err != nil || created || !reflect.DeepEqual(got, original) || got.Spec.EffectiveDeadlineScope() != model.DeadlineJob {
			t.Fatalf("legacy retry changed admission: %#v %v %v", got, created, err)
		}
		retry.Spec.TimeoutSeconds++
		if _, _, err := s.Admit(retry, "key", 1, 1); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatal("deadline alias hid other policy drift", err)
		}
	}
	select {
	case <-changed:
		t.Fatal("legacy retry notified a mutation")
	default:
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rawDataValues(t, path)) {
		t.Fatal("legacy retry rewrote records or idempotency bindings")
	}
}

func TestExplicitDeadlineChangesRemainIdempotencyConflicts(t *testing.T) {
	s := openTestStore(t)
	original := testRecord("tenant", "original")
	original.Spec.DeadlineScope = model.DeadlinePod
	if _, _, err := s.Admit(original, "key", 1, 1); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []model.DeadlineScope{"", model.DeadlineJob} {
		retry := original
		retry.ID, retry.Spec.DeadlineScope = "retry", scope
		if _, _, err := s.Admit(retry, "key", 1, 1); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatal("explicit admitted clock was ignored", scope, err)
		}
	}
}

func TestLegacyDeadlineAliasRejectsCorruptOriginalBinding(t *testing.T) {
	s := openTestStore(t)
	original := testRecord("tenant", "original")
	if _, _, err := s.Admit(original, "key", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		original.Spec.TimeoutSeconds++
		encoded, err := json.Marshal(original)
		if err != nil {
			return err
		}
		return tx.Bucket(recordsBucket).Put([]byte(original.ID), encoded)
	}); err != nil {
		t.Fatal(err)
	}
	retry := testRecord("tenant", "retry")
	retry.Spec.DeadlineScope = model.DeadlinePod
	if _, _, err := s.Admit(retry, "key", 1, 1); !errors.Is(err, ErrIntegrity) {
		t.Fatal("legacy alias accepted a corrupt original binding", err)
	}
}

func TestSchemaThreeDeadlineUpgradeOnlyChangesMetadata(t *testing.T) {
	path, _ := closedStoreFixture(t, false)
	var epoch string
	mutateFile(t, path, func(tx *bolt.Tx) error {
		meta := tx.Bucket(metadataBucket)
		epoch = string(meta.Get(epochKey))
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, 3)
		return meta.Put(schemaKey, value)
	})
	before, sourceHash := rawDataValues(t, path), fileDigest(t, path)
	info, err := CheckOffline(path)
	if err != nil || info.SchemaVersion != 3 || fileDigest(t, path) != sourceHash {
		t.Fatal("legacy inspection changed source", info, err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	version, versionErr := s.SchemaVersion()
	afterEpoch, epochErr := s.Epoch()
	if versionErr != nil || epochErr != nil || version != 4 || afterEpoch != epoch {
		t.Fatal("deadline schema fence changed epoch or version", version, afterEpoch, versionErr, epochErr)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rawDataValues(t, path)) {
		t.Fatal("schema 4 migration changed historical data or keys")
	}
}
