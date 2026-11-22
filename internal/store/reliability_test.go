package store

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

func assertNotification(t *testing.T, s *Store, previous <-chan struct{}) <-chan struct{} {
	t.Helper()
	select {
	case <-previous:
	default:
		t.Fatal("successful mutation did not invalidate its previous generation")
	}
	current := s.Changed()
	if current == previous {
		t.Fatal("notification did not replace the invalidation channel")
	}
	assertNoNotification(t, s, current)
	return current
}

func assertNoNotification(t *testing.T, s *Store, previous <-chan struct{}) {
	t.Helper()
	select {
	case <-previous:
		t.Fatal("invalidation channel closed without a successful state change")
	default:
	}
	if s.Changed() != previous {
		t.Fatal("invalidation generation changed without a successful state change")
	}
}

func TestChangedFollowsCommittedMutations(t *testing.T) {
	s := openTestStore(t)
	previous := s.Changed()
	if previous == nil {
		t.Fatal("initial invalidation channel is nil")
	}
	// Block the writer before commit. The existing generation must remain open
	// until the transaction can actually persist the new record.
	held, err := s.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, _, err := s.Admit(testRecord("tenant", "run"), "key", 10, 10)
		done <- err
	}()
	<-started
	select {
	case <-previous:
		t.Fatal("notified before the write transaction could commit")
	default:
	}
	if err := held.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	previous = assertNotification(t, s, previous)
	got, err := s.Get("tenant", "run")
	if err != nil || got.State != model.Queued || len(got.Events) != 1 {
		t.Fatalf("notification preceded committed admission: %#v, err=%v", got, err)
	}
	changed, err := s.Transition("run", []model.State{model.Queued}, model.Starting, "starting")
	if err != nil {
		t.Fatal(err)
	}
	assertNotification(t, s, previous)
	got, err = s.Get("tenant", "run")
	if err != nil || !reflect.DeepEqual(got, changed) || len(got.Events) != 2 {
		t.Fatalf("notification preceded committed transition: %#v, err=%v", got, err)
	}
}

func TestChangedIgnoresIdempotencyNoopsAndErrors(t *testing.T) {
	s := openTestStore(t)
	if _, _, err := s.Admit(testRecord("tenant", "original"), "key", 1, 1); err != nil {
		t.Fatal(err)
	}
	previous := s.Changed()
	if _, created, err := s.Admit(testRecord("tenant", "retry"), "key", 1, 1); err != nil || created {
		t.Fatalf("retry: created=%v, err=%v", created, err)
	}
	assertNoNotification(t, s, previous)
	if _, err := s.Transition("original", []model.State{model.Queued}, model.Queued, "not saved"); err != nil {
		t.Fatal(err)
	}
	assertNoNotification(t, s, previous)
	modified := testRecord("tenant", "modified")
	modified.Spec.Command = []string{"different"}
	for name, operation := range map[string]func() error{
		"quota": func() error {
			_, _, err := s.Admit(testRecord("tenant", "over-quota"), "", 1, 1)
			return err
		},
		"duplicate ID": func() error {
			_, _, err := s.Admit(testRecord("tenant", "original"), "", 1, 1)
			return err
		},
		"different idempotency spec": func() error {
			_, _, err := s.Admit(modified, "key", 1, 1)
			return err
		},
		"CAS": func() error {
			_, err := s.Transition("original", []model.State{model.Running}, model.Failed, "bad CAS")
			return err
		},
		"invalid state": func() error {
			_, err := s.Transition("original", []model.State{model.Queued}, "unknown", "bad state")
			return err
		},
		"missing record": func() error {
			_, err := s.Transition("missing", []model.State{model.Queued}, model.Running, "missing")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil {
				t.Fatal("expected mutation to fail")
			}
			assertNoNotification(t, s, previous)
		})
	}
	if _, err := s.Transition("original", []model.State{model.Queued}, model.Succeeded, "done"); err != nil {
		t.Fatal(err)
	}
	previous = assertNotification(t, s, previous)
	if _, err := s.Transition("original", []model.State{model.Succeeded}, model.Succeeded, "terminal noop"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Admit(testRecord("tenant", "completed-retry"), "key", 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Transition("original", []model.State{model.Succeeded}, model.Queued, "resurrect"); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal resurrection: %v", err)
	}
	assertNoNotification(t, s, previous)
}

func TestChangedIgnoresRolledBackAndClosedDatabaseWrites(t *testing.T) {
	s := openTestStore(t)
	original := mustAdmit(t, s, testRecord("tenant", "run"))
	// Force a transactional error after Transition has written the record, so
	// notification cannot be based on merely reaching records.Put.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(tenantActiveBucket).DeleteBucket([]byte("tenant"))
	}); err != nil {
		t.Fatal(err)
	}
	previous := s.Changed()
	if _, err := s.Transition("run", []model.State{model.Queued}, model.Succeeded, "rollback"); err == nil {
		t.Fatal("expected missing tenant index to roll back transition")
	}
	assertNoNotification(t, s, previous)
	got, err := s.Get("tenant", "run")
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatalf("failed transition changed durable data: %#v, err=%v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	assertNoNotification(t, s, previous)
	if _, _, err := s.Admit(testRecord("tenant", "after-close"), "", 10, 10); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("admit after close: %v", err)
	}
	if _, err := s.Transition("run", []model.State{model.Queued}, model.Running, "closed"); !errors.Is(err, bolt.ErrDatabaseNotOpen) {
		t.Fatalf("transition after close: %v", err)
	}
	assertNoNotification(t, s, previous)
}

func TestConcurrentSubscribeBeforeGetSeesCommittedHistory(t *testing.T) {
	s := openTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const count = 24
	var watchers, writers sync.WaitGroup
	errors := make(chan error, 2*count)
	ready := make(chan struct{}, count)
	for i := range count {
		id := fmt.Sprintf("run-%02d", i)
		watchers.Add(1)
		go func() {
			defer watchers.Done()
			first := true
			for {
				changed := s.Changed()
				if first {
					ready <- struct{}{}
					first = false
				}
				record, err := s.Get("tenant", id)
				if err == nil && record.State == model.Succeeded {
					if len(record.Events) != 4 {
						errors <- fmt.Errorf("%s has %d events", id, len(record.Events))
					}
					return
				}
				if err != nil && err != ErrNotFound {
					errors <- err
					return
				}
				select {
				case <-changed:
				case <-ctx.Done():
					errors <- fmt.Errorf("%s: missed final invalidation", id)
					return
				}
			}
		}()
	}
	for range count {
		<-ready
	}
	for i := range count {
		id := fmt.Sprintf("run-%02d", i)
		writers.Add(1)
		go func() {
			defer writers.Done()
			if _, _, err := s.Admit(testRecord("tenant", id), "", count, count); err != nil {
				errors <- err
				return
			}
			from := model.Queued
			for _, to := range []model.State{model.Starting, model.Running, model.Succeeded} {
				if _, err := s.Transition(id, []model.State{from}, to, "observed"); err != nil {
					errors <- err
					return
				}
				from = to
			}
		}()
	}
	writers.Wait()
	watchers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func recordIDs(records []model.Record) []string {
	ids := make([]string, len(records))
	for i := range records {
		ids[i] = records[i].ID
	}
	return ids
}

func TestListPageEqualTimestampsAndConcurrentNewerInsert(t *testing.T) {
	s := openTestStore(t)
	stamp := time.Date(2026, 11, 21, 8, 0, 0, 1234, time.FixedZone("test", 3600))
	want := []string{"a", "aa", "a/child", "a:child", "z", "é", "a\x00"}
	for _, id := range want {
		record := testRecord("tenant", id)
		record.CreatedAt = stamp
		mustAdmit(t, s, record)
	}
	mustAdmit(t, s, testRecord("other-tenant", "not-visible"))
	sort.Sort(sort.Reverse(sort.StringSlice(want)))
	var got []string
	before := ""
	for page := 0; ; page++ {
		records, next, err := s.ListPage("tenant", 2, before)
		if err != nil || len(records) > 2 {
			t.Fatalf("page %d: %#v, next=%q, err=%v", page, records, next, err)
		}
		got = append(got, recordIDs(records)...)
		if page == 0 {
			first, err := s.List("tenant", 2)
			if err != nil || !reflect.DeepEqual(first, records) {
				t.Fatalf("List compatibility: %#v, err=%v", first, err)
			}
			newer := testRecord("tenant", "newer-during-pagination")
			newer.CreatedAt = stamp.Add(time.Second)
			mustAdmit(t, s, newer)
		}
		if next == "" {
			break
		}
		if next == before || page >= len(want) {
			t.Fatal("pagination did not advance")
		}
		before = next
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pagination skipped/duplicated records: got %#v, want %#v", got, want)
	}
	newest, _, err := s.ListPage("tenant", 1, "")
	if err != nil || len(newest) != 1 || newest[0].ID != "newer-during-pagination" {
		t.Fatalf("fresh list did not include later insert: %#v, err=%v", newest, err)
	}
	empty, next, err := s.ListPage("missing-tenant", 10, "")
	if err != nil || empty == nil || len(empty) != 0 || next != "" {
		t.Fatalf("empty tenant page: %#v, next=%q, err=%v", empty, next, err)
	}
}

func TestListPageCursorAndEventsSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	stamp := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b", "c"} {
		record := testRecord("tenant", id)
		record.CreatedAt = stamp
		previous := s.Changed()
		mustAdmit(t, s, record)
		assertNotification(t, s, previous)
	}
	previous := s.Changed()
	finished, err := s.Transition("a", []model.State{model.Queued}, model.Succeeded, "durable result")
	if err != nil {
		t.Fatal(err)
	}
	assertNotification(t, s, previous)
	first, before, err := s.ListPage("tenant", 1, "")
	if err != nil || !reflect.DeepEqual(recordIDs(first), []string{"c"}) || before == "" {
		t.Fatalf("first page: %#v, next=%q, err=%v", first, before, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	assertNoNotification(t, s, s.Changed())
	records, next, err := s.ListPage("tenant", 10, before)
	if err != nil || next != "" || !reflect.DeepEqual(recordIDs(records), []string{"b", "a"}) || !reflect.DeepEqual(records[1], finished) {
		t.Fatalf("persisted page/history: %#v, next=%q, err=%v", records, next, err)
	}
}

func TestListPageCursorScopeAndMalformedBounds(t *testing.T) {
	s := openTestStore(t)
	for _, id := range []string{"a", "b"} {
		mustAdmit(t, s, testRecord("a/b", id))
	}
	_, before, err := s.ListPage("a/b", 1, "")
	if err != nil || before == "" {
		t.Fatalf("cursor: %q, err=%v", before, err)
	}
	for _, tenant := range []string{"a", "", "missing"} {
		if _, next, err := s.ListPage(tenant, 1, before); !errors.Is(err, ErrCursor) || next != "" {
			t.Fatalf("tenant %q accepted another tenant's cursor: next=%q, err=%v", tenant, next, err)
		}
	}
	raw, err := base64.RawURLEncoding.DecodeString(before)
	if err != nil {
		t.Fatal(err)
	}
	wrongVersion := append([]byte(nil), raw...)
	wrongVersion[0]++
	wrongTime := append([]byte(nil), raw...)
	binary.BigEndian.PutUint32(wrongTime[cursorHeaderSize+8:cursorHeaderSize+12], 1_000_000_000)
	for _, cursor := range []string{"!", "a", before + "=", before[:4] + "\n" + before[4:], strings.Repeat("a", maxCursorBytes+1),
		base64.RawURLEncoding.EncodeToString(wrongVersion), base64.RawURLEncoding.EncodeToString(wrongTime),
		base64.RawURLEncoding.EncodeToString(raw[:cursorHeaderSize+12])} {
		if _, next, err := s.ListPage("a/b", 1, cursor); !errors.Is(err, ErrCursor) || next != "" {
			t.Fatalf("accepted malformed cursor with %d bytes: next=%q, err=%v", len(cursor), next, err)
		}
	}
}

func TestListPageMaximumIDAndBoundedRecordReads(t *testing.T) {
	s := openTestStore(t)
	stamp := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	for _, id := range []string{"a", "b", strings.Repeat("z", maxIdentityBytes)} {
		record := testRecord("tenant", id)
		record.CreatedAt = stamp
		mustAdmit(t, s, record)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(recordsBucket).Put([]byte("a"), []byte("not JSON"))
	}); err != nil {
		t.Fatal(err)
	}
	first, before, err := s.ListPage("tenant", 1, "")
	if err != nil || len(first) != 1 || len(before) != maxCursorBytes {
		t.Fatalf("maximum ID cursor: len=%d, records=%d, err=%v", len(before), len(first), err)
	}
	second, next, err := s.ListPage("tenant", 1, before)
	if err != nil || next == "" || !reflect.DeepEqual(recordIDs(second), []string{"b"}) {
		t.Fatalf("page decoded history beyond its limit: %#v, next=%q, err=%v", second, next, err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(recordsBucket).Put([]byte(first[0].ID), []byte("not JSON"))
	}); err != nil {
		t.Fatal(err)
	}
	// A keyset page must not decode the newer corrupt record to reach its key.
	if records, _, err := s.ListPage("tenant", 1, before); err != nil || !reflect.DeepEqual(recordIDs(records), []string{"b"}) {
		t.Fatalf("page scanned newer history: %#v, err=%v", records, err)
	}
	if _, _, err := s.ListPage("tenant", 1, next); err == nil {
		t.Fatal("did not surface corruption when its record actually entered the page")
	}
}
