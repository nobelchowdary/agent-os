package store

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrSchema    = errors.New("unsupported or incomplete store schema")
	ErrIntegrity = errors.New("store integrity check failed")
	ErrActive    = errors.New("offline copy requires a drained store")
	ErrChecksum  = errors.New("backup checksum is invalid or mismatched")
	ErrExists    = errors.New("destination already exists")
	ErrEpoch     = errors.New("cursor belongs to another store epoch")

	metadataBucket = []byte("metadata")
	schemaKey      = []byte("schema_version")
	epochKey       = []byte("store_epoch")
)

// Version 4 fences older binaries that cannot retain a persisted deadline scope.
// Its upgrade changes metadata only; omitted scopes keep the legacy Job clock.
const currentSchemaVersion uint64 = 4

var dataBuckets = [][]byte{recordsBucket, tenantIndexBucket, activeBucket, tenantActiveBucket, idempotencyBucket}

// schemaVersion checks the bucket contract without modifying any existing data.
// An absent metadata bucket is the original version-zero layout.
func schemaVersion(tx *bolt.Tx) (uint64, error) {
	for _, name := range dataBuckets {
		if tx.Bucket(name) == nil {
			return 0, fmt.Errorf("%w: missing data bucket", ErrSchema)
		}
	}
	var version uint64
	meta := tx.Bucket(metadataBucket)
	if meta != nil {
		value := meta.Get(schemaKey)
		if len(value) != 8 {
			return 0, fmt.Errorf("%w: malformed version", ErrSchema)
		}
		version = binary.BigEndian.Uint64(value)
		if version > currentSchemaVersion {
			return 0, fmt.Errorf("%w: newer version", ErrSchema)
		}
		if version >= 2 && !validEpoch(string(meta.Get(epochKey))) {
			return 0, fmt.Errorf("%w: malformed store epoch", ErrSchema)
		}
		if err := meta.ForEach(func(key, value []byte) error {
			known := bytes.Equal(key, schemaKey) || version >= 2 && bytes.Equal(key, epochKey)
			if !known || value == nil {
				return fmt.Errorf("%w: unknown metadata", ErrSchema)
			}
			return nil
		}); err != nil {
			return 0, err
		}
	}
	if version >= 3 && tx.Bucket(resultsBucket) == nil {
		return 0, fmt.Errorf("%w: missing results bucket", ErrSchema)
	}
	if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		if bytes.Equal(name, metadataBucket) || version >= 3 && bytes.Equal(name, resultsBucket) {
			return nil
		}
		for _, known := range dataBuckets {
			if bytes.Equal(name, known) {
				return nil
			}
		}
		return fmt.Errorf("%w: unknown bucket", ErrSchema)
	}); err != nil {
		return 0, err
	}
	return version, nil
}

func initializeSchema(db *bolt.DB, fresh bool) error {
	empty := false
	var version uint64
	if err := db.View(func(tx *bolt.Tx) error {
		count := 0
		if err := tx.ForEach(func(_ []byte, _ *bolt.Bucket) error { count++; return nil }); err != nil {
			return err
		}
		empty = count == 0
		if empty {
			if !fresh {
				return fmt.Errorf("%w: existing database has no application buckets", ErrSchema)
			}
			return nil
		}
		var err error
		version, err = schemaVersion(tx)
		return err
	}); err != nil {
		return err
	}
	// Acquisition suppresses bbolt's automatic freelist-writing transaction
	// until the application schema is accepted. All authorized writes retain
	// ordinary synced freelist/commit behavior from this point onward.
	db.NoFreelistSync = false
	if !empty && version == currentSchemaVersion {
		return nil
	}
	var epoch string
	if version < 2 {
		var err error
		epoch, err = randomEpoch("")
		if err != nil {
			return err
		}
	}
	// One transaction initializes a fresh database or adds version metadata to
	// an intact legacy store. No run/spec/event/index value is rewritten.
	return db.Update(func(tx *bolt.Tx) error {
		if empty {
			for _, name := range dataBuckets {
				if _, err := tx.CreateBucket(name); err != nil {
					return err
				}
			}
		} else if _, err := schemaVersion(tx); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(resultsBucket); err != nil {
			return err
		}
		meta, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, currentSchemaVersion)
		if err := meta.Put(schemaKey, value); err != nil {
			return err
		}
		// Preserve existing history and cursors on a schema-only upgrade.
		if version >= 2 {
			return nil
		}
		return meta.Put(epochKey, []byte(epoch))
	})
}

func validEpoch(epoch string) bool {
	if len(epoch) != 32 {
		return false
	}
	for _, ch := range epoch {
		if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}

func randomEpoch(previous string) (string, error) {
	for {
		var value [16]byte
		if _, err := rand.Read(value[:]); err != nil {
			return "", fmt.Errorf("create store epoch: %w", err)
		}
		epoch := hex.EncodeToString(value[:])
		if epoch != previous {
			return epoch, nil
		}
	}
}

func readEpoch(tx *bolt.Tx) (string, error) {
	version, err := schemaVersion(tx)
	if err != nil {
		return "", err
	}
	if version < 2 {
		return "", ErrSchema
	}
	return string(tx.Bucket(metadataBucket).Get(epochKey)), nil
}

// Epoch identifies this durable store history. It remains stable on ordinary
// restart and changes on restore before the restored database is published.
func (s *Store) Epoch() (string, error) {
	var epoch string
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		epoch, err = readEpoch(tx)
		return err
	})
	return epoch, err
}

func (s *Store) SchemaVersion() (uint64, error) {
	var version uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		version, err = schemaVersion(tx)
		return err
	})
	return version, err
}
