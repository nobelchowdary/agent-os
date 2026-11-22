package store

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func decodeSnapshotJSON(value []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrIntegrity
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ErrIntegrity
	}
	return nil
}

func validIdentity(value string) bool {
	return len(value) > 0 && len(value) <= maxIdentityBytes && utf8.ValidString(value)
}

func validateHistory(record model.Record) error {
	if !validIdentity(record.ID) || !validIdentity(record.TenantID) || !record.State.Valid() ||
		record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) ||
		len(record.Events) == 0 {
		return ErrIntegrity
	}
	for i, event := range record.Events {
		if !event.State.Valid() || event.At.IsZero() || !utf8.ValidString(event.Reason) ||
			len(event.Reason) > maxReasonBytes || !validReasonCode(event.State, event.ReasonCode, true) {
			return ErrIntegrity
		}
		if i == 0 {
			if event.State != model.Queued || !event.At.Equal(record.CreatedAt) || event.ReasonCode != "" {
				return ErrIntegrity
			}
		} else {
			previous := record.Events[i-1]
			// Pre-graph stores permitted more nonterminal recovery edges. A
			// migration must preserve that history, not impose today's graph
			// retrospectively. Terminal immutability has always been required.
			if previous.State == event.State || previous.State.Terminal() || event.At.Before(previous.At) {
				return ErrIntegrity
			}
		}
	}
	last := record.Events[len(record.Events)-1]
	if record.State != last.State || !record.UpdatedAt.Equal(last.At) || record.Reason != last.Reason || record.ReasonCode != last.ReasonCode {
		return ErrIntegrity
	}
	return nil
}

func snapshotRecord(records *bolt.Bucket, id []byte) (model.Record, error) {
	var record model.Record
	value := records.Get(id)
	if value == nil || decodeSnapshotJSON(value, &record) != nil || record.ID != string(id) {
		return record, ErrIntegrity
	}
	return record, nil
}

// validateSnapshot checks both directions of run/reservation indexes and every
// present idempotency binding. Records do not retain absent keys, so their loss
// cannot be inferred here. It never repairs history or normalizes a Spec.
// Legacy reason omissions remain missing; B-trees bound auxiliary memory.
func validateSnapshot(tx *bolt.Tx) (BackupInfo, error) {
	var info BackupInfo
	version, err := schemaVersion(tx)
	if err != nil {
		return info, err
	}
	info.SchemaVersion = version
	if version == currentSchemaVersion {
		info.StoreEpoch = string(tx.Bucket(metadataBucket).Get(epochKey))
	}
	records := tx.Bucket(recordsBucket)
	index := tx.Bucket(tenantIndexBucket)
	active := tx.Bucket(activeBucket)
	tenantActive := tx.Bucket(tenantActiveBucket)
	if err := records.ForEach(func(id, value []byte) error {
		if value == nil {
			return ErrIntegrity
		}
		record, err := snapshotRecord(records, id)
		if err != nil || validateHistory(record) != nil {
			return ErrIntegrity
		}
		tenantIndex := index.Bucket([]byte(record.TenantID))
		if tenantIndex == nil || !bytes.Equal(tenantIndex.Get(orderKey(record)), id) {
			return ErrIntegrity
		}
		reservation := active.Get(id)
		tenantReservations := tenantActive.Bucket([]byte(record.TenantID))
		var tenantReservation []byte
		if tenantReservations != nil {
			tenantReservation = tenantReservations.Get(id)
		}
		if record.State.Terminal() {
			if reservation != nil || tenantReservation != nil {
				return ErrIntegrity
			}
		} else {
			if !bytes.Equal(reservation, []byte(record.TenantID)) || !bytes.Equal(tenantReservation, []byte{1}) {
				return ErrIntegrity
			}
			info.ActiveRuns++
		}
		info.Runs++
		return nil
	}); err != nil {
		return info, err
	}
	if err := eachTenant(index, func(tenant []byte, bucket *bolt.Bucket) error {
		return bucket.ForEach(func(key, id []byte) error {
			if id == nil {
				return ErrIntegrity
			}
			record, err := snapshotRecord(records, id)
			if err != nil || record.TenantID != string(tenant) || !bytes.Equal(key, orderKey(record)) {
				return ErrIntegrity
			}
			return nil
		})
	}); err != nil {
		return info, err
	}
	if err := active.ForEach(func(id, tenant []byte) error {
		record, err := snapshotRecord(records, id)
		if tenant == nil || err != nil || record.State.Terminal() || record.TenantID != string(tenant) {
			return ErrIntegrity
		}
		return nil
	}); err != nil {
		return info, err
	}
	if err := eachTenant(tenantActive, func(tenant []byte, bucket *bolt.Bucket) error {
		return bucket.ForEach(func(id, reserved []byte) error {
			record, err := snapshotRecord(records, id)
			if !bytes.Equal(reserved, []byte{1}) || err != nil || record.State.Terminal() || record.TenantID != string(tenant) {
				return ErrIntegrity
			}
			return nil
		})
	}); err != nil {
		return info, err
	}
	if err := eachTenant(tx.Bucket(idempotencyBucket), func(tenant []byte, bucket *bolt.Bucket) error {
		return bucket.ForEach(func(key, value []byte) error {
			var entry idempotencyEntry
			if len(key) == 0 || len(key) > maxKeyBytes || value == nil || decodeSnapshotJSON(value, &entry) != nil {
				return ErrIntegrity
			}
			record, err := snapshotRecord(records, []byte(entry.ID))
			if err != nil || record.TenantID != string(tenant) {
				return ErrIntegrity
			}
			fingerprint, err := specFingerprint(record.Spec)
			if err != nil || entry.Fingerprint != fingerprint {
				return ErrIntegrity
			}
			return nil
		})
	}); err != nil {
		return info, err
	}
	return info, nil
}

func eachTenant(parent *bolt.Bucket, visit func([]byte, *bolt.Bucket) error) error {
	return parent.ForEach(func(tenant, value []byte) error {
		if value != nil || !validIdentity(string(tenant)) {
			return ErrIntegrity
		}
		bucket := parent.Bucket(tenant)
		if bucket == nil {
			return ErrIntegrity
		}
		return visit(tenant, bucket)
	})
}
