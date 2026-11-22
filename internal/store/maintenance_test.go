package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func privateTestDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func fileDigest(t *testing.T, path string) string {
	t.Helper()
	value, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:])
}

func mutateFile(t *testing.T, path string, mutate func(*bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(mutate); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func closedStoreFixture(t *testing.T, active bool) (string, []model.Record) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var records []model.Record
	for i, tenant := range []string{"tenant/a", "tenant", "tenant/a"} {
		input := testRecord(tenant, fmt.Sprintf("run-%d", i))
		r, created, err := s.Admit(input, fmt.Sprintf("request-%d", i), 10, 10)
		if err != nil || !created {
			t.Fatalf("admission: %v", err)
		}
		if !active {
			if i == 1 {
				r, err = s.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Cancelling, "cancel requested", model.ReasonCancelled)
				if err == nil {
					r, err = s.TransitionWithCode(r.ID, []model.State{model.Cancelling}, model.Cancelled, "stopped", model.ReasonCancelled)
				}
			} else {
				r, err = s.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Failed, "deadline", model.ReasonDeadlineExceeded)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		records = append(records, r)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return path, records
}

func rawDataValues(t *testing.T, path string) map[string][]byte {
	t.Helper()
	db, _, _, err := openDatabase(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	values := make(map[string][]byte)
	var visit func(*bolt.Bucket, string) error
	visit = func(bucket *bolt.Bucket, prefix string) error {
		return bucket.ForEach(func(key, value []byte) error {
			name := prefix + "/" + hex.EncodeToString(key)
			if value == nil {
				values[name] = nil
				return visit(bucket.Bucket(key), name)
			}
			values[name] = bytes.Clone(value)
			return nil
		})
	}
	if err := db.View(func(tx *bolt.Tx) error {
		for _, name := range dataBuckets {
			if err := visit(tx.Bucket(name), string(name)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return values
}

func TestLegacySchemaMigrationPreservesEveryDataValue(t *testing.T) {
	path, expected := closedStoreFixture(t, false)
	mutateFile(t, path, func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(metadataBucket); err != nil {
			return err
		}
		// Historical terminal omissions are preserved, never inferred/replayed.
		bucket := tx.Bucket(recordsBucket)
		for _, r := range expected {
			r.ReasonCode = ""
			for i := range r.Events {
				r.Events[i].ReasonCode = ""
			}
			value, err := json.Marshal(r)
			if err != nil {
				return err
			}
			if err := bucket.Put([]byte(r.ID), value); err != nil {
				return err
			}
		}
		return nil
	})
	before := rawDataValues(t, path)
	beforeHash := fileDigest(t, path)
	info, err := CheckOffline(path)
	if err != nil || info.SchemaVersion != 0 || info.ActiveRuns != 0 {
		t.Fatalf("legacy check: %+v %v", info, err)
	}
	if fileDigest(t, path) != beforeHash {
		t.Fatal("read-only check migrated the legacy source")
	}
	legacyBackup := filepath.Join(privateTestDir(t), "legacy.db")
	backup, err := BackupOffline(path, legacyBackup)
	if err != nil || backup.SchemaVersion != 0 || fileDigest(t, path) != beforeHash {
		t.Fatalf("legacy backup rewrote source: %+v %v", backup, err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	version, err := s.SchemaVersion()
	if err != nil || version != 1 {
		t.Fatalf("schema version = %d %v", version, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, rawDataValues(t, path)) {
		t.Fatal("migration rewrote run/spec/event/index bytes")
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, r := range expected {
		got, err := s.Get(r.TenantID, r.ID)
		if err != nil || got.ReasonCode != "" {
			t.Fatalf("legacy reason code was inferred: %+v %v", got, err)
		}
	}
}

func TestSchemaRejectsFutureMalformedAndIncompleteWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*bolt.Tx) error
	}{
		{"future", func(tx *bolt.Tx) error {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, 2)
			return tx.Bucket(metadataBucket).Put(schemaKey, value)
		}},
		{"malformed", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, []byte("1")) }},
		{"missing version", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Delete(schemaKey) }},
		{"missing data", func(tx *bolt.Tx) error { return tx.DeleteBucket(activeBucket) }},
		{"legacy incomplete", func(tx *bolt.Tx) error {
			if err := tx.DeleteBucket(metadataBucket); err != nil {
				return err
			}
			return tx.DeleteBucket(recordsBucket)
		}},
		{"unknown metadata", func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put([]byte("unexpected"), []byte{1}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := closedStoreFixture(t, false)
			mutateFile(t, path, tc.mutate)
			digest := fileDigest(t, path)
			if s, err := Open(path); !errors.Is(err, ErrSchema) {
				if s != nil {
					_ = s.Close()
				}
				t.Fatalf("Open accepted unsupported layout: %v", err)
			}
			if _, err := CheckOffline(path); !errors.Is(err, ErrSchema) {
				t.Fatalf("check accepted unsupported layout: %v", err)
			}
			if digest != fileDigest(t, path) {
				t.Fatal("rejected schema changed source bytes")
			}
		})
	}
}

func TestOpenInitializesOnlyANewExclusivelyCreatedFile(t *testing.T) {
	path := filepath.Join(privateTestDir(t), "new.db")
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("fixture path already exists")
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	version, err := s.SchemaVersion()
	if err != nil || version != currentSchemaVersion {
		t.Fatalf("new schema: %d %v", version, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := CheckOffline(path)
	if err != nil || info.SchemaVersion != currentSchemaVersion || info.Runs != 0 {
		t.Fatalf("new store incomplete: %+v %v", info, err)
	}

	for _, tc := range []struct {
		name    string
		prepare func(string) error
		want    error
	}{
		{"zero bytes", func(path string) error { return os.WriteFile(path, nil, 0600) }, ErrIntegrity},
		{"truncated", func(path string) error {
			valid, _ := closedStoreFixture(t, false)
			value, err := os.ReadFile(valid)
			if err != nil {
				return err
			}
			return os.WriteFile(path, value[:1024], 0600)
		}, ErrIntegrity},
		{"initialized empty bbolt", func(path string) error {
			db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
			if err != nil {
				return err
			}
			return db.Close()
		}, ErrSchema},
		{"initialized empty unsynced bbolt", func(path string) error {
			db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
			if err != nil {
				return err
			}
			if err := db.Update(func(tx *bolt.Tx) error {
				if _, err := tx.CreateBucket([]byte("temporary")); err != nil {
					return err
				}
				return tx.DeleteBucket([]byte("temporary"))
			}); err != nil {
				_ = db.Close()
				return err
			}
			return db.Close()
		}, ErrSchema},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(privateTestDir(t), "existing.db")
			if err := tc.prepare(path); err != nil {
				t.Fatal(err)
			}
			before := fileDigest(t, path)
			if opened, err := Open(path); !errors.Is(err, tc.want) {
				if opened != nil {
					_ = opened.Close()
				}
				t.Fatalf("existing incomplete file was initialized: %v", err)
			}
			if fileDigest(t, path) != before {
				t.Fatal("rejected existing file changed during startup")
			}
			if _, err := CheckOffline(path); !errors.Is(err, tc.want) {
				t.Fatalf("inspection accepted existing incomplete file: %v", err)
			}
			if fileDigest(t, path) != before {
				t.Fatal("inspection modified incomplete file")
			}
		})
	}
}

func TestRejectedSchemaWithUnsyncedFreelistNeverMutatesOnOpen(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*bolt.Tx) error
	}{
		{"future", func(tx *bolt.Tx) error {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, currentSchemaVersion+1)
			return tx.Bucket(metadataBucket).Put(schemaKey, value)
		}},
		{"malformed", func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(schemaKey, []byte("invalid"))
		}},
		{"incomplete", func(tx *bolt.Tx) error { return tx.DeleteBucket(activeBucket) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := closedStoreFixture(t, false)
			// This is the bbolt-compatible layout that causes a default
			// writable bbolt.Open to commit a freelist transaction by itself.
			db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(tc.mutate); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			before := fileDigest(t, path)
			if s, err := Open(path); !errors.Is(err, ErrSchema) {
				if s != nil {
					_ = s.Close()
				}
				t.Fatalf("rejected layout opened: %v", err)
			}
			if before != fileDigest(t, path) {
				t.Fatal("bbolt startup mutated rejected schema while synchronizing its freelist")
			}
			if _, err := CheckOffline(path); !errors.Is(err, ErrSchema) {
				t.Fatalf("read-only check accepted layout: %v", err)
			}
			if before != fileDigest(t, path) {
				t.Fatal("read-only inspection mutated rejected schema")
			}
		})
	}
}

func TestAcceptedUnsyncedFreelistRestoresNormalDurableWritePolicy(t *testing.T) {
	path, _ := closedStoreFixture(t, false)
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second, NoFreelistSync: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(schemaKey, make([]byte, 8)) }); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.db.NoSync || s.db.NoFreelistSync {
		t.Fatal("accepted store kept startup-only freelist or disabled commit syncing")
	}
	r := mustAdmit(t, s, testRecord("tenant", "after-freelist-recovery"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get(r.TenantID, r.ID)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("normal durable write policy did not survive reopen: %+v %v", got, err)
	}
}

func TestOfflineBackupRestorePreservesTenantHistoryAndIdempotency(t *testing.T) {
	source, expected := closedStoreFixture(t, false)
	sourceHash := fileDigest(t, source)
	backup := filepath.Join(privateTestDir(t), "backup.db")
	info, err := BackupOffline(source, backup)
	if err != nil {
		t.Fatal(err)
	}
	if info.SchemaVersion != 1 || info.Runs != 3 || info.ActiveRuns != 0 || info.Bytes <= 0 || info.SHA256 != sourceHash || fileDigest(t, source) != sourceHash {
		t.Fatalf("backup evidence wrong or source changed: %+v", info)
	}
	checked, err := CheckOffline(backup)
	if err != nil || checked != info {
		t.Fatalf("published backup mismatch: %+v %v", checked, err)
	}
	destination := filepath.Join(privateTestDir(t), "restored.db")
	restored, err := RestoreOffline(backup, destination, strings.ToUpper(info.SHA256))
	if err != nil || restored != info || fileDigest(t, destination) != info.SHA256 {
		t.Fatalf("restore: %+v %v", restored, err)
	}
	for _, path := range []string{backup, destination} {
		stat, err := os.Stat(path)
		if err != nil || stat.Mode().Perm() != 0600 {
			t.Fatalf("unsafe published permissions: %v %v", stat, err)
		}
	}
	s, err := Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, want := range expected {
		got, err := s.Get(want.TenantID, want.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("restored history/spec mismatch: %+v %v", got, err)
		}
		if _, err := s.Get("another-tenant", want.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal("restored record leaked tenant")
		}
		retry, created, err := s.Admit(testRecord(want.TenantID, "retry-id"), fmt.Sprintf("request-%d", i), 0, 0)
		if err != nil || created || !reflect.DeepEqual(retry, want) {
			t.Fatalf("idempotency did not survive restore: %+v created=%v err=%v", retry, created, err)
		}
	}
	a, err := s.List("tenant/a", 100)
	if err != nil || len(a) != 2 {
		t.Fatalf("restored tenant list: %v %v", a, err)
	}
}

func assertNoCopyTemps(t *testing.T, dir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, ".agentos-copy-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary copies leaked: %v %v", matches, err)
	}
}

func TestOfflineInspectionAllowsActiveButCopyAndRestoreRefuse(t *testing.T) {
	source, _ := closedStoreFixture(t, true)
	info, err := CheckOffline(source)
	if err != nil || info.ActiveRuns != 3 {
		t.Fatalf("active inspection: %+v %v", info, err)
	}
	for _, restore := range []bool{false, true} {
		destination := filepath.Join(privateTestDir(t), "copy.db")
		if restore {
			_, err = RestoreOffline(source, destination, info.SHA256)
		} else {
			_, err = BackupOffline(source, destination)
		}
		if !errors.Is(err, ErrActive) {
			t.Fatalf("restore=%v copied active metadata: %v", restore, err)
		}
		if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("rejected active copy published destination")
		}
		assertNoCopyTemps(t, filepath.Dir(destination))
	}
}

func TestOfflineOperationsRefuseLiveOwnerWithFiniteLock(t *testing.T) {
	source, _ := closedStoreFixture(t, false)
	s, err := Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	start := time.Now()
	if _, err := CheckOffline(source); !errors.Is(err, bolt.ErrTimeout) {
		t.Fatalf("check ignored live lock: %v", err)
	}
	destination := filepath.Join(privateTestDir(t), "backup.db")
	if _, err := BackupOffline(source, destination); !errors.Is(err, bolt.ErrTimeout) {
		t.Fatalf("backup ignored live lock: %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("offline lock wait was unbounded")
	}
	assertNoCopyTemps(t, filepath.Dir(destination))
}

func TestRestoreRejectsChecksumAndCorruptionWithoutPublication(t *testing.T) {
	source, _ := closedStoreFixture(t, false)
	for _, digest := range []string{"", strings.Repeat("0", 63), strings.Repeat("z", 64), strings.Repeat("0", 64)} {
		destination := filepath.Join(privateTestDir(t), "restore.db")
		if _, err := RestoreOffline(source, destination, digest); !errors.Is(err, ErrChecksum) {
			t.Fatalf("digest %q: %v", digest, err)
		}
		if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("checksum failure published a database")
		}
		if _, err := os.Stat(filepath.Dir(destination)); err == nil {
			assertNoCopyTemps(t, filepath.Dir(destination))
		}
	}
	corrupt := filepath.Join(t.TempDir(), "private", "bad.db")
	if err := os.MkdirAll(filepath.Dir(corrupt), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(corrupt, bytes.Repeat([]byte("corrupt"), 4096), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckOffline(corrupt); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("corrupt check: %v", err)
	}
	destination := filepath.Join(privateTestDir(t), "restore.db")
	if _, err := RestoreOffline(corrupt, destination, fileDigest(t, corrupt)); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("checksum alone authorized corrupted restore: %v", err)
	}
	assertNoCopyTemps(t, filepath.Dir(destination))
}

func TestOfflinePathsRejectAliasesAndNeverOverwrite(t *testing.T) {
	source, _ := closedStoreFixture(t, false)
	dir := filepath.Dir(source)
	symlink := filepath.Join(dir, "symlink.db")
	if err := os.Symlink(source, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckOffline(symlink); err == nil {
		t.Fatal("followed source symlink")
	}
	hardlink := filepath.Join(dir, "hardlink.db")
	if err := os.Link(source, hardlink); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{source, hardlink} {
		if _, err := CheckOffline(path); err == nil {
			t.Fatal("accepted aliased source inode")
		}
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "missing.db")
	if _, err := CheckOffline(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing source: %v", err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("check created missing source")
	}
	for _, path := range []string{source, symlink} {
		digest := fileDigest(t, source)
		if _, err := BackupOffline(source, path); !errors.Is(err, ErrExists) {
			t.Fatalf("overwrote destination %q: %v", path, err)
		}
		if digest != fileDigest(t, source) {
			t.Fatal("existing destination/source was changed")
		}
	}
	parentLink := filepath.Join(t.TempDir(), "parent")
	if err := os.Symlink(dir, parentLink); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckOffline(filepath.Join(parentLink, filepath.Base(source))); err == nil {
		t.Fatal("followed immediate parent symlink")
	}
	if _, err := BackupOffline(source, filepath.Join(parentLink, "copy.db")); err == nil {
		t.Fatal("published via immediate parent symlink")
	}
	missingParent := filepath.Join(t.TempDir(), "missing", "copy.db")
	if _, err := BackupOffline(source, missingParent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("accepted absent destination parent: %v", err)
	}
	if _, err := os.Lstat(filepath.Dir(missingParent)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("offline copy created an unintended destination directory")
	}
}

func TestOfflinePublicationCleansFailedAndRacingCopies(t *testing.T) {
	source, _ := closedStoreFixture(t, false)
	dir := privateTestDir(t)
	destination := filepath.Join(dir, "copy.db")
	injected := errors.New("injected writer failure")
	if _, err := publishOffline(destination, func(file *os.File) (BackupInfo, error) {
		_, _ = file.Write([]byte("partial"))
		return BackupInfo{}, injected
	}); !errors.Is(err, injected) {
		t.Fatalf("writer failure lost: %v", err)
	}
	assertNoCopyTemps(t, dir)
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial copy published")
	}
	_, err := publishOffline(destination, func(file *os.File) (BackupInfo, error) {
		info, err := inspectOffline(source, true, file)
		if err != nil {
			return info, err
		}
		return info, os.WriteFile(destination, []byte("other writer"), 0600)
	})
	if !errors.Is(err, ErrExists) {
		t.Fatalf("publication overwrote concurrent destination: %v", err)
	}
	contents, err := os.ReadFile(destination)
	if err != nil || string(contents) != "other writer" {
		t.Fatal("concurrent destination changed")
	}
	assertNoCopyTemps(t, dir)
}
