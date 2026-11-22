package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func rewriteFixtureRecord(tx *bolt.Tx, mutate func(*model.Record)) error {
	bucket := tx.Bucket(recordsBucket)
	r, err := readRecord(bucket, "run-0")
	if err != nil {
		return err
	}
	mutate(&r)
	value, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(r.ID), value)
}

func TestOfflineValidationRejectsLogicalCorruptionWithoutRepair(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active bool
		mutate func(*bolt.Tx) error
	}{
		{"spec fingerprint", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) { r.Spec.Command = []string{"changed"} })
		}},
		{"missing tenant index", false, func(tx *bolt.Tx) error { return tx.Bucket(tenantIndexBucket).DeleteBucket([]byte("tenant/a")) }},
		{"cross tenant index", false, func(tx *bolt.Tx) error {
			r, err := readRecord(tx.Bucket(recordsBucket), "run-0")
			if err != nil {
				return err
			}
			return tx.Bucket(tenantIndexBucket).Bucket([]byte("tenant")).Put(orderKey(r), []byte(r.ID))
		}},
		{"extra ordering index", false, func(tx *bolt.Tx) error {
			return tx.Bucket(tenantIndexBucket).Bucket([]byte("tenant/a")).Put([]byte("extra"), []byte("run-0"))
		}},
		{"cross tenant idempotency", false, func(tx *bolt.Tx) error {
			r, err := readRecord(tx.Bucket(recordsBucket), "run-1")
			if err != nil {
				return err
			}
			fp, err := specFingerprint(r.Spec)
			if err != nil {
				return err
			}
			value, err := json.Marshal(idempotencyEntry{ID: r.ID, Fingerprint: fp})
			if err != nil {
				return err
			}
			return tx.Bucket(idempotencyBucket).Bucket([]byte("tenant/a")).Put([]byte("request-0"), value)
		}},
		{"mismatched idempotency fingerprint", false, func(tx *bolt.Tx) error {
			value, _ := json.Marshal(idempotencyEntry{ID: "run-0", Fingerprint: "incorrect"})
			return tx.Bucket(idempotencyBucket).Bucket([]byte("tenant/a")).Put([]byte("request-0"), value)
		}},
		{"missing active reservation", true, func(tx *bolt.Tx) error { return tx.Bucket(activeBucket).Delete([]byte("run-0")) }},
		{"wrong tenant reservation", true, func(tx *bolt.Tx) error { return tx.Bucket(activeBucket).Put([]byte("run-0"), []byte("tenant")) }},
		{"missing tenant reservation", true, func(tx *bolt.Tx) error {
			return tx.Bucket(tenantActiveBucket).Bucket([]byte("tenant/a")).Delete([]byte("run-0"))
		}},
		{"cross tenant reservation", true, func(tx *bolt.Tx) error {
			return tx.Bucket(tenantActiveBucket).Bucket([]byte("tenant")).Put([]byte("run-0"), []byte{1})
		}},
		{"terminal remains reserved", false, func(tx *bolt.Tx) error { return tx.Bucket(activeBucket).Put([]byte("run-0"), []byte("tenant/a")) }},
		{"record current state mismatch", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) { r.State = model.Succeeded })
		}},
		{"record reason mismatch", false, func(tx *bolt.Tx) error { return rewriteFixtureRecord(tx, func(r *model.Record) { r.Reason = "other" }) }},
		{"record code mismatch", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) { r.ReasonCode = model.ReasonUnknown })
		}},
		{"event unknown code", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) { r.Events[1].ReasonCode = "unsupported"; r.ReasonCode = "unsupported" })
		}},
		{"event invalid success code", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) {
				r.Events[1].ReasonCode = model.ReasonCompleted
				r.ReasonCode = model.ReasonCompleted
			})
		}},
		{"event time reversal", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) { r.Events[1].At = r.CreatedAt.Add(-time.Second); r.UpdatedAt = r.Events[1].At })
		}},
		{"event after terminal", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) {
				r.Events = append(r.Events, model.Event{At: r.UpdatedAt, State: model.Succeeded, ReasonCode: model.ReasonCompleted})
				r.State = model.Succeeded
				r.Reason = ""
				r.ReasonCode = model.ReasonCompleted
			})
		}},
		{"history missing admission", false, func(tx *bolt.Tx) error {
			return rewriteFixtureRecord(tx, func(r *model.Record) { r.Events = r.Events[1:] })
		}},
		{"nested record bucket", false, func(tx *bolt.Tx) error {
			_, err := tx.Bucket(recordsBucket).CreateBucket([]byte("unexpected"))
			return err
		}},
		{"nested active bucket", false, func(tx *bolt.Tx) error {
			_, err := tx.Bucket(activeBucket).CreateBucket([]byte("unexpected"))
			return err
		}},
		{"unknown record field", false, func(tx *bolt.Tx) error {
			bucket := tx.Bucket(recordsBucket)
			value := append([]byte{}, bucket.Get([]byte("run-0"))...)
			value = append(value[:len(value)-1], []byte(",\"future_execution\":true}")...)
			return bucket.Put([]byte("run-0"), value)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, _ := closedStoreFixture(t, tc.active)
			mutateFile(t, source, tc.mutate)
			before := fileDigest(t, source)
			if _, err := CheckOffline(source); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("corrupt logical state was accepted: %v", err)
			}
			destination := filepath.Join(privateTestDir(t), "restore.db")
			if _, err := RestoreOffline(source, destination, before); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("trusted checksum bypassed logical validation: %v", err)
			}
			if before != fileDigest(t, source) {
				t.Fatal("validation repaired or modified source")
			}
			assertNoCopyTemps(t, filepath.Dir(destination))
		})
	}
}

func TestOfflineValidationPreservesHistoricalNonterminalEdges(t *testing.T) {
	path, _ := closedStoreFixture(t, false)
	mutateFile(t, path, func(tx *bolt.Tx) error {
		return rewriteFixtureRecord(tx, func(r *model.Record) {
			r.Events = []model.Event{
				{At: r.CreatedAt, State: model.Queued},
				{At: r.CreatedAt.Add(time.Second), State: model.Running},
				{At: r.CreatedAt.Add(2 * time.Second), State: model.Starting},
				{At: r.CreatedAt.Add(3 * time.Second), State: model.Queued},
				{At: r.CreatedAt.Add(4 * time.Second), State: model.Cancelled, Reason: "historical cleanup"},
			}
			r.State = model.Cancelled
			r.Reason = "historical cleanup"
			r.ReasonCode = ""
			r.UpdatedAt = r.Events[len(r.Events)-1].At
		})
	})
	before := fileDigest(t, path)
	if _, err := CheckOffline(path); err != nil {
		t.Fatalf("today's graph was retroactively applied to legacy history: %v", err)
	}
	backup := filepath.Join(privateTestDir(t), "backup.db")
	if _, err := BackupOffline(path, backup); err != nil {
		t.Fatalf("legacy nonterminal recovery edges made backup unusable: %v", err)
	}
	if fileDigest(t, path) != before {
		t.Fatal("legacy history was rewritten")
	}
}
