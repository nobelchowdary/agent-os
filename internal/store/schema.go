package store

import (
	"bytes"
	"encoding/binary"
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

	metadataBucket = []byte("metadata")
	schemaKey      = []byte("schema_version")
)

const currentSchemaVersion uint64 = 1

var dataBuckets = [][]byte{recordsBucket, tenantIndexBucket, activeBucket, tenantActiveBucket, idempotencyBucket}

// schemaVersion checks the bucket contract without modifying any existing data.
// An absent metadata bucket is the original version-zero layout.
func schemaVersion(tx *bolt.Tx) (uint64, error) {
	for _, name := range dataBuckets {
		if tx.Bucket(name) == nil {
			return 0, fmt.Errorf("%w: missing data bucket", ErrSchema)
		}
	}
	if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		if bytes.Equal(name, metadataBucket) {
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
	meta := tx.Bucket(metadataBucket)
	if meta == nil {
		return 0, nil
	}
	value := meta.Get(schemaKey)
	if len(value) != 8 {
		return 0, fmt.Errorf("%w: malformed version", ErrSchema)
	}
	version := binary.BigEndian.Uint64(value)
	if version > currentSchemaVersion {
		return 0, fmt.Errorf("%w: newer version", ErrSchema)
	}
	if err := meta.ForEach(func(key, value []byte) error {
		if !bytes.Equal(key, schemaKey) || value == nil {
			return fmt.Errorf("%w: unknown metadata", ErrSchema)
		}
		return nil
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
		meta, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, currentSchemaVersion)
		return meta.Put(schemaKey, value)
	})
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
