// Package store persists runs and atomically enforces admission limits.
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
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrNotFound            = errors.New("run not found")
	ErrConflict            = errors.New("run state conflict")
	ErrQuota               = errors.New("active run quota exceeded")
	ErrIdempotencyConflict = errors.New("idempotency key already used for another spec")
)

var (
	recordsBucket      = []byte("records")
	tenantIndexBucket  = []byte("tenant-index")
	activeBucket       = []byte("active")
	tenantActiveBucket = []byte("tenant-active")
	idempotencyBucket  = []byte("idempotency")
)

const (
	maxIdentityBytes = 1024
	maxKeyBytes      = 4096
	maxReasonBytes   = 1024
	defaultListLimit = 100
	maxListLimit     = 1000
)

type Store struct{ db *bolt.DB }

type idempotencyEntry struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
}

// Open obtains an exclusive process lock with a finite timeout. The database
// must live in a private directory; Open creates a missing directory with 0700.
// bbolt's synchronous commits remain enabled so mutations are durable on return.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("database path is required")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("stat database directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("database directory must be private (0700)")
	}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("database path must be a regular file")
		}
		if err := os.Chmod(path, 0600); err != nil {
			return nil, fmt.Errorf("secure database file: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat database file: %w", err)
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{recordsBucket, tenantIndexBucket, activeBucket, tenantActiveBucket, idempotencyBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize database: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Admit stores a queued run and reserves global and tenant capacity in the same
// transaction. Every nonterminal run occupies capacity, including cancellations.
// A matching idempotent retry returns its original record before checking quota.
func (s *Store) Admit(record model.Record, idempotencyKey string, maxTenantActive, maxGlobalActive int) (model.Record, bool, error) {
	if record.ID == "" || len(record.ID) > maxIdentityBytes || record.TenantID == "" || len(record.TenantID) > maxIdentityBytes {
		return model.Record{}, false, errors.New("run and tenant IDs must contain 1 to 1024 bytes")
	}
	if len(idempotencyKey) > maxKeyBytes {
		return model.Record{}, false, errors.New("idempotency key exceeds 4096 bytes")
	}
	if record.State != "" && record.State != model.Queued {
		return model.Record{}, false, fmt.Errorf("%w: new runs must be queued", ErrConflict)
	}
	fingerprint, err := specFingerprint(record.Spec)
	if err != nil {
		return model.Record{}, false, err
	}
	var result model.Record
	created := false
	err = s.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		keys := tx.Bucket(idempotencyBucket).Bucket([]byte(record.TenantID))
		if idempotencyKey != "" && keys != nil {
			if value := keys.Get([]byte(idempotencyKey)); value != nil {
				var entry idempotencyEntry
				if err := json.Unmarshal(value, &entry); err != nil {
					return fmt.Errorf("decode idempotency entry: %w", err)
				}
				if entry.Fingerprint != fingerprint {
					return ErrIdempotencyConflict
				}
				original, err := readRecord(records, entry.ID)
				if err != nil {
					return err
				}
				if original.TenantID != record.TenantID {
					return errors.New("corrupt idempotency tenant index")
				}
				result = original
				return nil
			}
		}
		if records.Get([]byte(record.ID)) != nil {
			return fmt.Errorf("%w: run ID already exists", ErrConflict)
		}
		if maxTenantActive <= 0 || maxGlobalActive <= 0 {
			return fmt.Errorf("%w: limits must be positive", ErrQuota)
		}
		active := tx.Bucket(activeBucket)
		if active.Stats().KeyN >= maxGlobalActive {
			return fmt.Errorf("%w: global limit", ErrQuota)
		}
		tenantActive, err := tx.Bucket(tenantActiveBucket).CreateBucketIfNotExists([]byte(record.TenantID))
		if err != nil {
			return err
		}
		if tenantActive.Stats().KeyN >= maxTenantActive {
			return fmt.Errorf("%w: tenant limit", ErrQuota)
		}
		record.State = model.Queued
		if record.CreatedAt.IsZero() {
			record.CreatedAt = time.Now().UTC()
		} else {
			record.CreatedAt = record.CreatedAt.UTC()
		}
		record.UpdatedAt = record.CreatedAt
		record.Reason = boundedReason(record.Reason)
		record.Events = []model.Event{{At: record.CreatedAt, State: model.Queued, Reason: record.Reason}}
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := records.Put([]byte(record.ID), encoded); err != nil {
			return err
		}
		index, err := tx.Bucket(tenantIndexBucket).CreateBucketIfNotExists([]byte(record.TenantID))
		if err != nil {
			return err
		}
		if err := index.Put(orderKey(record), []byte(record.ID)); err != nil {
			return err
		}
		if err := active.Put([]byte(record.ID), []byte(record.TenantID)); err != nil {
			return err
		}
		if err := tenantActive.Put([]byte(record.ID), []byte{1}); err != nil {
			return err
		}
		if idempotencyKey != "" {
			keys, err = tx.Bucket(idempotencyBucket).CreateBucketIfNotExists([]byte(record.TenantID))
			if err != nil {
				return err
			}
			entry, err := json.Marshal(idempotencyEntry{ID: record.ID, Fingerprint: fingerprint})
			if err != nil {
				return err
			}
			if err := keys.Put([]byte(idempotencyKey), entry); err != nil {
				return err
			}
		}
		// Decode an owned copy: neither the caller's command slice nor Events may
		// alias the returned record while a caller modifies its request.
		if err := json.Unmarshal(encoded, &result); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return model.Record{}, false, err
	}
	return result, created, nil
}

func (s *Store) Get(tenant, id string) (model.Record, error) {
	var result model.Record
	err := s.db.View(func(tx *bolt.Tx) error {
		record, err := readRecord(tx.Bucket(recordsBucket), id)
		if err != nil {
			return err
		}
		if record.TenantID != tenant {
			return ErrNotFound
		}
		result = record
		return nil
	})
	return result, err
}

// List returns at most limit runs in descending creation order. Nonpositive
// limits default to 100; limits greater than 1000 are capped at 1000.
func (s *Store) List(tenant string, limit int) ([]model.Record, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	result := make([]model.Record, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		index := tx.Bucket(tenantIndexBucket).Bucket([]byte(tenant))
		if index == nil {
			return nil
		}
		cursor := index.Cursor()
		for key, id := cursor.Last(); key != nil && len(result) < limit; key, id = cursor.Prev() {
			record, err := readRecord(tx.Bucket(recordsBucket), string(id))
			if err != nil {
				return err
			}
			if record.TenantID != tenant {
				return errors.New("corrupt tenant index")
			}
			result = append(result, record)
		}
		return nil
	})
	return result, err
}

func (s *Store) Active() ([]model.Record, error) {
	result := make([]model.Record, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(activeBucket).ForEach(func(id, tenant []byte) error {
			record, err := readRecord(tx.Bucket(recordsBucket), string(id))
			if err != nil {
				return err
			}
			if record.State.Terminal() || !bytes.Equal(tenant, []byte(record.TenantID)) {
				return errors.New("corrupt active run index")
			}
			result = append(result, record)
			return nil
		})
	})
	return result, err
}

// Transition performs a compare-and-set on the current state. Terminal states
// are immutable. An allowed same-state transition returns without another event.
func (s *Store) Transition(id string, allowed []model.State, to model.State, reason string) (model.Record, error) {
	if !validState(to) {
		return model.Record{}, fmt.Errorf("%w: unknown target state", ErrConflict)
	}
	var result model.Record
	err := s.db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		record, err := readRecord(records, id)
		if err != nil {
			return err
		}
		matched := false
		for _, state := range allowed {
			if state == record.State {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: current state %s", ErrConflict, record.State)
		}
		if record.State == to {
			result = record
			return nil
		}
		if record.State.Terminal() {
			return fmt.Errorf("%w: terminal state %s", ErrConflict, record.State)
		}
		now := time.Now().UTC()
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		record.State = to
		record.Reason = boundedReason(reason)
		record.UpdatedAt = now
		record.Events = append(record.Events, model.Event{At: now, State: to, Reason: record.Reason})
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := records.Put([]byte(id), encoded); err != nil {
			return err
		}
		if to.Terminal() {
			if err := tx.Bucket(activeBucket).Delete([]byte(id)); err != nil {
				return err
			}
			index := tx.Bucket(tenantActiveBucket).Bucket([]byte(record.TenantID))
			if index == nil {
				return errors.New("missing tenant active index")
			}
			if err := index.Delete([]byte(id)); err != nil {
				return err
			}
		}
		result = record
		return nil
	})
	return result, err
}

func readRecord(records *bolt.Bucket, id string) (model.Record, error) {
	var record model.Record
	value := records.Get([]byte(id))
	if value == nil {
		return record, ErrNotFound
	}
	if err := json.Unmarshal(value, &record); err != nil {
		return record, fmt.Errorf("decode run: %w", err)
	}
	if record.ID != id {
		return model.Record{}, errors.New("corrupt run ID index")
	}
	return record, nil
}

func specFingerprint(spec model.Spec) (string, error) {
	encoded, err := json.Marshal(spec)
	if err != nil {
		return "", fmt.Errorf("encode run spec: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// The timestamp has fixed-width fields, so arbitrary IDs cannot collide through
// delimiter ambiguity. Tenant separation is provided by nested bbolt buckets.
func orderKey(record model.Record) []byte {
	key := make([]byte, 12+len(record.ID))
	binary.BigEndian.PutUint64(key[:8], uint64(record.CreatedAt.Unix())^(uint64(1)<<63))
	binary.BigEndian.PutUint32(key[8:12], uint32(record.CreatedAt.Nanosecond()))
	copy(key[12:], record.ID)
	return key
}

func validState(state model.State) bool {
	switch state {
	case model.Queued, model.Starting, model.Activating, model.Running, model.Cancelling, model.Succeeded, model.Failed, model.Cancelled:
		return true
	default:
		return false
	}
}

func boundedReason(reason string) string {
	reason = strings.ToValidUTF8(reason, "�")
	if len(reason) <= maxReasonBytes {
		return reason
	}
	reason = reason[:maxReasonBytes]
	for !utf8.ValidString(reason) && len(reason) > 0 {
		reason = reason[:len(reason)-1]
	}
	return reason
}
