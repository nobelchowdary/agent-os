package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

var resultsBucket = []byte("results")

const MaxResultOutputBytes = 64 * 1024

func validResult(result model.ExecutionResult, allowExpired bool) bool {
	if result.CapturedAt.IsZero() || !result.ExpiresAt.After(result.CapturedAt) ||
		!utf8.ValidString(result.OutputHead) || !utf8.ValidString(result.OutputTail) ||
		len(result.OutputHead)+len(result.OutputTail) > MaxResultOutputBytes {
		return false
	}
	switch result.Status {
	case "captured":
		return true
	case "unavailable":
		return result.OutputHead == "" && result.OutputTail == "" && !result.OutputComplete && !result.OutputTruncated
	case "expired":
		return allowExpired && result.OutputHead == "" && result.OutputTail == ""
	}
	return false
}

func projectResult(tx *bolt.Tx, record model.Record) (model.Record, error) {
	bucket := tx.Bucket(resultsBucket)
	if bucket == nil {
		return record, ErrSchema
	}
	value := bucket.Get([]byte(record.ID))
	if value == nil {
		return record, nil
	}
	var result model.ExecutionResult
	if decodeSnapshotJSON(value, &result) != nil || !validResult(result, true) {
		return record, ErrIntegrity
	}
	if !time.Now().Before(result.ExpiresAt) {
		expireResult(&result)
	}
	record.Result = &result
	return record, nil
}

func expireResult(result *model.ExecutionResult) {
	result.Status = "expired"
	result.OutputHead, result.OutputTail = "", ""
}

// SaveResult persists the first bounded result attachment before execution
// cleanup. It never rewrites immutable run/history/spec/idempotency bytes. A
// repeated capture, including after expiry, cannot replace the first result.
func (s *Store) SaveResult(id string, result model.ExecutionResult) error {
	if !validResult(result, false) {
		return errors.New("invalid execution result")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(tx *bolt.Tx) error {
		if _, err := readRecord(tx.Bucket(recordsBucket), id); err != nil {
			return err
		}
		bucket := tx.Bucket(resultsBucket)
		if bucket.Get([]byte(id)) != nil {
			return nil
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(id), encoded)
	})
}

// ExpireResults removes expired payloads while retaining result metadata and
// exit status. bbolt reuses freed pages; expiry does not compact the file or cap
// total metadata/idempotency growth. GET suppresses expired output immediately,
// even before this periodic physical prune has run.
func (s *Store) ExpireResults(now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(resultsBucket)
		var expired [][]byte
		if err := bucket.ForEach(func(id, value []byte) error {
			var result model.ExecutionResult
			if value == nil || decodeSnapshotJSON(value, &result) != nil || !validResult(result, true) {
				return ErrIntegrity
			}
			if result.Status != "expired" && !now.Before(result.ExpiresAt) {
				expired = append(expired, bytes.Clone(id))
			}
			return nil
		}); err != nil {
			return err
		}
		// Do not mutate a bucket while ForEach is traversing it.
		for _, id := range expired {
			var result model.ExecutionResult
			if err := json.Unmarshal(bucket.Get(id), &result); err != nil {
				return ErrIntegrity
			}
			expireResult(&result)
			encoded, err := json.Marshal(result)
			if err != nil {
				return err
			}
			if err := bucket.Put(id, encoded); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Lookup is controller-only inventory reconciliation. HTTP paths must use Get
// with the authenticated tenant; this method grants no caller authorization.
func (s *Store) Lookup(id string) (model.Record, error) {
	var result model.Record
	err := s.db.View(func(tx *bolt.Tx) error {
		record, err := readRecord(tx.Bucket(recordsBucket), id)
		if err != nil {
			return err
		}
		result, err = projectResult(tx, record)
		return err
	})
	return result, err
}
