// Package store persists runs and atomically enforces admission limits.
package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	ErrCursor              = errors.New("invalid list cursor")
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
	// Version, store epoch, tenant digest, fixed-width time and bounded run ID.
	cursorVersion    = 2
	cursorEpochSize  = 16
	cursorHeaderSize = 1 + cursorEpochSize + sha256.Size
	maxRawCursorSize = cursorHeaderSize + 12 + maxIdentityBytes
	maxCursorBytes   = (maxRawCursorSize*8 + 5) / 6
)

type Store struct {
	db *bolt.DB
	// Serialize commit and publication so concurrent writes cannot publish an
	// older invalidation after a subscriber has observed a later generation.
	mu      sync.Mutex
	changed chan struct{}
}

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
	path, err := privatePath(path, true)
	if err != nil {
		return nil, err
	}
	db, _, fresh, err := openDatabase(path, false)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := initializeSchema(db, fresh); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize database: %w", err)
	}
	return &Store{db: db, changed: make(chan struct{})}, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// Changed returns the current invalidation channel. Subscribe before reading
// records, then read again when the channel closes. Notifications may coalesce;
// persisted records and event history are authoritative. Only successful new
// admissions and actual state changes close a generation, after durable commit.
// Closing the Store does not notify; callers must also watch their own context.
func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// publishChanged runs under mu only after the write transaction has committed.
func (s *Store) publishChanged() {
	previous := s.changed
	s.changed = make(chan struct{})
	close(previous)
}

// Admit stores a queued run and reserves global and tenant capacity in the same
// transaction. Every nonterminal run occupies capacity, including cancellations.
// A matching idempotent retry returns its original record before checking quota.
func (s *Store) Admit(record model.Record, idempotencyKey string, maxTenantActive, maxGlobalActive int) (model.Record, bool, error) {
	if !validIdentity(record.ID) || !validIdentity(record.TenantID) {
		return model.Record{}, false, errors.New("run and tenant IDs must contain 1 to 1024 UTF-8 bytes")
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
		record.ReasonCode = ""
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
	if created {
		s.publishChanged()
	}
	return result, created, nil
}

func (s *Store) Get(tenant, id string) (model.Record, error) {
	record, _, err := s.GetWithEpoch(tenant, id)
	return record, err
}

// GetWithEpoch reads the tenant-owned record and store epoch from one database
// snapshot, so an event-stream cursor can name the exact durable history read.
func (s *Store) GetWithEpoch(tenant, id string) (model.Record, string, error) {
	var result model.Record
	var epoch string
	err := s.db.View(func(tx *bolt.Tx) error {
		record, err := readRecord(tx.Bucket(recordsBucket), id)
		if err != nil {
			return err
		}
		if record.TenantID != tenant {
			return ErrNotFound
		}
		epoch, err = readEpoch(tx)
		if err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return model.Record{}, "", err
	}
	return result, epoch, nil
}

// List returns at most limit runs in descending creation order. Nonpositive
// limits default to 100; limits greater than 1000 are capped at 1000.
func (s *Store) List(tenant string, limit int) ([]model.Record, error) {
	records, _, err := s.ListPage(tenant, limit, "")
	return records, err
}

// ListPage returns runs newest first and a cursor for the next older page, or an
// empty cursor at the end. Equal creation times are ordered by descending ID.
// The opaque cursor is scoped to a tenant and an exclusive order-key boundary.
// Each page reads at most limit records and one additional index key, using a
// keyset seek rather than scanning earlier pages. Inserts newer than the cursor
// do not shift later pages; pagination is not a cross-request snapshot.
func (s *Store) ListPage(tenant string, limit int, before string) ([]model.Record, string, error) {
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	result := make([]model.Record, 0)
	next := ""
	err := s.db.View(func(tx *bolt.Tx) error {
		epoch, err := readEpoch(tx)
		if err != nil {
			return err
		}
		boundary, err := decodeCursor(tenant, before, epoch)
		if err != nil {
			return err
		}
		index := tx.Bucket(tenantIndexBucket).Bucket([]byte(tenant))
		if index == nil {
			return nil
		}
		cursor := index.Cursor()
		key, id := cursor.Last()
		if boundary != nil {
			key, id = cursor.Seek(boundary)
			if key == nil {
				key, id = cursor.Last()
			} else {
				// Seek is inclusive and may land beyond a removed boundary.
				// Moving back always selects the greatest key strictly below it.
				key, id = cursor.Prev()
			}
		}
		var lastKey []byte
		for key != nil && len(result) < limit {
			record, err := readRecord(tx.Bucket(recordsBucket), string(id))
			if err != nil {
				return err
			}
			if record.TenantID != tenant {
				return errors.New("corrupt tenant index")
			}
			result = append(result, record)
			lastKey = key
			key, id = cursor.Prev()
		}
		if key != nil {
			next = encodeCursor(tenant, lastKey, epoch)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return result, next, nil
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

// Transition performs a compare-and-set against the persisted state and enforces
// model.CanTransition in the same transaction. The caller's expected-state list
// cannot authorize an illegal edge. An allowed same-state transition returns
// without changing history or notifying subscribers.
func (s *Store) Transition(id string, allowed []model.State, to model.State, reason string) (model.Record, error) {
	return s.TransitionWithCode(id, allowed, to, reason, model.DefaultReasonCode(to))
}

// TransitionWithCode records a stable observed outcome or cleanup intent in the
// same transaction as the state change. Same-state reads preserve legacy and
// already-coded outcomes, regardless of the newly proposed reason/code.
func (s *Store) TransitionWithCode(id string, allowed []model.State, to model.State, reason string, code model.ReasonCode) (model.Record, error) {
	if !to.Valid() {
		return model.Record{}, fmt.Errorf("%w: unknown target state", ErrConflict)
	}
	var result model.Record
	changed := false
	s.mu.Lock()
	defer s.mu.Unlock()
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
		if !model.CanTransition(record.State, to) {
			return fmt.Errorf("%w: transition %s to %s is not allowed", ErrConflict, record.State, to)
		}
		if record.State == to {
			result = record
			return nil
		}
		if !validReasonCode(to, code, false) {
			return fmt.Errorf("%w: reason code does not match target state", ErrConflict)
		}
		now := time.Now().UTC()
		if now.Before(record.UpdatedAt) {
			now = record.UpdatedAt
		}
		record.State = to
		record.Reason = boundedReason(reason)
		record.ReasonCode = code
		record.UpdatedAt = now
		record.Events = append(record.Events, model.Event{At: now, State: to, Reason: record.Reason, ReasonCode: code})
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
		changed = true
		return nil
	})
	if err == nil && changed {
		s.publishChanged()
	}
	return result, err
}

// Empty codes remain valid in legacy persisted records. New terminal changes
// must carry a code; only Cancelling can persist nonterminal cleanup intent.
func validReasonCode(state model.State, code model.ReasonCode, legacy bool) bool {
	if code == "" {
		return legacy || !state.Terminal()
	}
	if !code.Valid() {
		return false
	}
	switch state {
	case model.Succeeded:
		return code == model.ReasonCompleted
	case model.Cancelled:
		return code == model.ReasonCancelled
	case model.Failed:
		return code != model.ReasonCompleted && code != model.ReasonCancelled
	case model.Cancelling:
		return code != model.ReasonCompleted
	}
	return false
}

func encodeCursor(tenant string, key []byte, epoch string) string {
	raw := make([]byte, cursorHeaderSize+len(key))
	raw[0] = cursorVersion
	epochBytes, _ := hex.DecodeString(epoch) // readEpoch already validated it.
	copy(raw[1:1+cursorEpochSize], epochBytes)
	scope := sha256.Sum256([]byte(tenant))
	copy(raw[1+cursorEpochSize:cursorHeaderSize], scope[:])
	copy(raw[cursorHeaderSize:], key)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(tenant, before, epoch string) ([]byte, error) {
	if before == "" {
		return nil, nil
	}
	if len(before) > maxCursorBytes {
		return nil, ErrCursor
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(before)
	if err != nil || len(raw) < cursorHeaderSize+13 || len(raw) > maxRawCursorSize || raw[0] != cursorVersion {
		return nil, ErrCursor
	}
	// DecodeString permits newlines even in strict mode. Require the canonical
	// unpadded URL form so malformed alternate encodings are rejected as well.
	if base64.RawURLEncoding.EncodeToString(raw) != before {
		return nil, ErrCursor
	}
	scope := sha256.Sum256([]byte(tenant))
	if !bytes.Equal(raw[1+cursorEpochSize:cursorHeaderSize], scope[:]) {
		return nil, ErrCursor
	}
	key := raw[cursorHeaderSize:]
	if binary.BigEndian.Uint32(key[8:12]) >= 1_000_000_000 {
		return nil, ErrCursor
	}
	if hex.EncodeToString(raw[1:1+cursorEpochSize]) != epoch {
		return nil, errors.Join(ErrCursor, ErrEpoch)
	}
	return key, nil
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
