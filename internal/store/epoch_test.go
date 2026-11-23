package store

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func TestEpochPersistsAndGetWithEpochPreservesTenantScope(t *testing.T) {
	path := filepath.Join(privateTestDir(t), "store.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := s.Epoch()
	if err != nil || !validEpoch(epoch) {
		t.Fatalf("new epoch: %q %v", epoch, err)
	}
	r := mustAdmit(t, s, testRecord("tenant", "run"))
	got, observedEpoch, err := s.GetWithEpoch("tenant", r.ID)
	if err != nil || observedEpoch != epoch || !reflect.DeepEqual(got, r) {
		t.Fatalf("record/epoch snapshot: %+v %q %v", got, observedEpoch, err)
	}
	if _, hiddenEpoch, err := s.GetWithEpoch("other", r.ID); !errors.Is(err, ErrNotFound) || hiddenEpoch != "" {
		t.Fatalf("epoch exposed before tenant lookup: %q %v", hiddenEpoch, err)
	}
	if _, hiddenEpoch, err := s.GetWithEpoch("tenant", "absent"); !errors.Is(err, ErrNotFound) || hiddenEpoch != "" {
		t.Fatalf("epoch exposed for missing record: %q %v", hiddenEpoch, err)
	}
	if _, err := s.Transition(r.ID, []model.State{model.Queued}, model.Succeeded, "done"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Epoch(); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("closed epoch: %v", err)
	}
	if _, hiddenEpoch, err := s.GetWithEpoch("tenant", r.ID); !errors.Is(err, bolt.ErrDatabaseNotOpen) || hiddenEpoch != "" {
		t.Fatalf("closed record/epoch: %q %v", hiddenEpoch, err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if reopened, err := s.Epoch(); err != nil || reopened != epoch {
		t.Fatalf("ordinary restart changed epoch: %q %v", reopened, err)
	}
}

func TestSchemaOneMigrationAddsEpochWithoutRewritingData(t *testing.T) {
	path, _ := closedStoreFixture(t, false)
	mutateFile(t, path, func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(resultsBucket); err != nil {
			return err
		}
		meta := tx.Bucket(metadataBucket)
		version := make([]byte, 8)
		binary.BigEndian.PutUint64(version, 1)
		if err := meta.Put(schemaKey, version); err != nil {
			return err
		}
		return meta.Delete(epochKey)
	})
	before := rawDataValues(t, path)
	beforeHash := fileDigest(t, path)
	legacy, err := CheckOffline(path)
	if err != nil || legacy.SchemaVersion != 1 || legacy.StoreEpoch != "" || fileDigest(t, path) != beforeHash {
		t.Fatalf("schema1 inspection mutated source: %+v %v", legacy, err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := s.Epoch()
	if err != nil || !validEpoch(epoch) {
		t.Fatalf("migration epoch: %q %v", epoch, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rawDataValues(t, path)) {
		t.Fatal("schema1 migration rewrote data values")
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if again, err := s.Epoch(); err != nil || again != epoch {
		t.Fatal("migrated epoch was not durable")
	}
}

func TestEpochMetadataMalformedFailsWithoutMutation(t *testing.T) {
	for _, tc := range []struct{ name, epoch string }{{"missing", ""}, {"short", strings.Repeat("a", 31)}, {"uppercase", strings.Repeat("A", 32)}, {"nonhex", strings.Repeat("g", 32)}} {
		t.Run(tc.name, func(t *testing.T) {
			epoch := tc.epoch
			path, _ := closedStoreFixture(t, false)
			mutateFile(t, path, func(tx *bolt.Tx) error {
				if epoch == "" {
					return tx.Bucket(metadataBucket).Delete(epochKey)
				}
				return tx.Bucket(metadataBucket).Put(epochKey, []byte(epoch))
			})
			before := fileDigest(t, path)
			if s, err := Open(path); !errors.Is(err, ErrSchema) {
				if s != nil {
					_ = s.Close()
				}
				t.Fatalf("accepted malformed epoch: %v", err)
			}
			if _, err := CheckOffline(path); !errors.Is(err, ErrSchema) {
				t.Fatalf("check accepted malformed epoch: %v", err)
			}
			if fileDigest(t, path) != before {
				t.Fatal("rejected epoch metadata was changed")
			}
		})
	}
}

func TestRestoredEpochRejectsOldCursorPreservingRecordBytes(t *testing.T) {
	source, expected := closedStoreFixture(t, false)
	s, err := Open(source)
	if err != nil {
		t.Fatal(err)
	}
	_, cursor, err := s.ListPage("tenant/a", 1, "")
	if err != nil || cursor == "" {
		t.Fatalf("source cursor: %q %v", cursor, err)
	}
	oldEpoch, err := s.Epoch()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := rawDataValues(t, source)
	sourceHash := fileDigest(t, source)
	backup := filepath.Join(privateTestDir(t), "backup.db")
	info, err := BackupOffline(source, backup)
	if err != nil || info.StoreEpoch != oldEpoch || info.SHA256 != sourceHash || info.SourceSHA256 != "" {
		t.Fatalf("backup changed source epoch/hash: %+v %v", info, err)
	}
	destination := filepath.Join(privateTestDir(t), "restored.db")
	restored, err := RestoreOffline(backup, destination, info.SHA256)
	if err != nil || restored.StoreEpoch == oldEpoch || !validEpoch(restored.StoreEpoch) || restored.SourceSHA256 != sourceHash || restored.SHA256 == sourceHash {
		t.Fatalf("restore identity/provenance: %+v %v", restored, err)
	}
	if fileDigest(t, backup) != sourceHash || fileDigest(t, source) != sourceHash {
		t.Fatal("restore modified original input")
	}
	if !reflect.DeepEqual(before, rawDataValues(t, destination)) {
		t.Fatal("epoch rotation rewrote record/spec/event/idempotency/index bytes")
	}
	checked, err := CheckOffline(destination)
	checked.SourceSHA256 = sourceHash
	if err != nil || checked != restored {
		t.Fatalf("restored evidence does not name final bytes: %+v %v", checked, err)
	}
	s, err = Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, next, err := s.ListPage("tenant/a", 1, cursor); !errors.Is(err, ErrEpoch) || !errors.Is(err, ErrCursor) || next != "" {
		t.Fatalf("old cursor accepted restored history: %q %v", next, err)
	}
	_, fresh, err := s.ListPage("tenant/a", 1, "")
	if err != nil || fresh == "" || fresh == cursor {
		t.Fatalf("new epoch cursor: %q %v", fresh, err)
	}
	if _, _, err := s.ListPage("tenant/a", 10, fresh); err != nil {
		t.Fatalf("fresh cursor rejected: %v", err)
	}
	for _, want := range expected {
		got, epoch, err := s.GetWithEpoch(want.TenantID, want.ID)
		if err != nil || epoch != restored.StoreEpoch || !reflect.DeepEqual(got, want) {
			t.Fatalf("record snapshot after restore: %+v %q %v", got, epoch, err)
		}
	}
}

func TestListCursorRejectsLegacyAndForeignEpoch(t *testing.T) {
	a := openTestStore(t)
	b := openTestStore(t)
	for _, s := range []*Store{a, b} {
		for _, id := range []string{"a", "b"} {
			mustAdmit(t, s, testRecord("tenant", id))
		}
	}
	_, cursor, err := a.ListPage("tenant", 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.ListPage("tenant", 1, cursor); !errors.Is(err, ErrEpoch) {
		t.Fatalf("foreign store cursor accepted: %v", err)
	}
	if _, _, err := b.ListPage("other-tenant", 1, cursor); !errors.Is(err, ErrCursor) || errors.Is(err, ErrEpoch) {
		t.Fatalf("tenant mismatch exposed epoch classification: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		t.Fatal(err)
	}
	legacy := append([]byte{1}, raw[1+cursorEpochSize:]...)
	if _, _, err := a.ListPage("tenant", 1, base64.RawURLEncoding.EncodeToString(legacy)); !errors.Is(err, ErrCursor) || errors.Is(err, ErrEpoch) {
		t.Fatalf("legacy cursor silently resumed: %v", err)
	}
}

func TestRestoreLegacyVersionsCreatesUniqueEpochsWithoutChangingInput(t *testing.T) {
	for _, version := range []uint64{0, 1} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			source, _ := closedStoreFixture(t, false)
			mutateFile(t, source, func(tx *bolt.Tx) error {
				if err := tx.DeleteBucket(resultsBucket); err != nil {
					return err
				}
				if version == 0 {
					return tx.DeleteBucket(metadataBucket)
				}
				meta := tx.Bucket(metadataBucket)
				value := make([]byte, 8)
				binary.BigEndian.PutUint64(value, version)
				if err := meta.Put(schemaKey, value); err != nil {
					return err
				}
				return meta.Delete(epochKey)
			})
			digest := fileDigest(t, source)
			values := rawDataValues(t, source)
			previous := ""
			for range 2 {
				target := filepath.Join(privateTestDir(t), "restored.db")
				info, err := RestoreOffline(source, target, digest)
				if err != nil || info.SchemaVersion != currentSchemaVersion || !validEpoch(info.StoreEpoch) || info.StoreEpoch == previous || info.SourceSHA256 != digest || info.SHA256 == digest {
					t.Fatalf("legacy restore: %+v %v", info, err)
				}
				previous = info.StoreEpoch
				if !reflect.DeepEqual(values, rawDataValues(t, target)) {
					t.Fatal("legacy restore changed data values")
				}
			}
			if fileDigest(t, source) != digest {
				t.Fatal("legacy source migrated in place")
			}
		})
	}
}

func TestRestoreTransformationFailureCleansPrivateCopy(t *testing.T) {
	source, _ := closedStoreFixture(t, false)
	for _, kind := range []string{"failure", "unchanged", "active", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			target := filepath.Join(privateTestDir(t), "restored.db")
			_, err := publishOfflineWithTransform(target, func(file *os.File) (BackupInfo, error) { return inspectOffline(source, true, file) }, func(path string) error {
				if kind == "failure" {
					return errors.New("injected transformation failure")
				}
				if kind == "unchanged" {
					return nil
				}
				if err := rotateRestoredEpoch(path); err != nil {
					return err
				}
				if kind == "invalid" {
					return os.WriteFile(path, []byte("damaged copy"), 0600)
				}
				s, err := Open(path)
				if err != nil {
					return err
				}
				_, _, admitErr := s.Admit(testRecord("tenant", "active"), "", 100, 100)
				return errors.Join(admitErr, s.Close())
			})
			if err == nil {
				t.Fatal("invalid transformation published")
			}
			if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed transformation left target")
			}
			assertNoCopyTemps(t, filepath.Dir(target))
		})
	}
}
