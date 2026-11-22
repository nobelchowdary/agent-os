package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	bolt "go.etcd.io/bbolt"
)

// This independent contract table keeps the store tests from using the
// implementation under test as their expected-transition oracle.
var lifecycleEdges = map[model.State][]model.State{
	model.Queued:     {model.Queued, model.Starting, model.Running, model.Cancelling, model.Succeeded, model.Failed},
	model.Starting:   {model.Starting, model.Activating, model.Running, model.Cancelling, model.Succeeded, model.Failed},
	model.Activating: {model.Activating, model.Running, model.Cancelling, model.Succeeded, model.Failed},
	model.Running:    {model.Running, model.Cancelling, model.Succeeded, model.Failed},
	model.Cancelling: {model.Cancelling, model.Failed, model.Cancelled},
	model.Succeeded:  {model.Succeeded},
	model.Failed:     {model.Failed},
	model.Cancelled:  {model.Cancelled},
}

var lifecycleStates = []model.State{model.Queued, model.Starting, model.Activating, model.Running, model.Cancelling, model.Succeeded, model.Failed, model.Cancelled}

func permittedEdge(from, to model.State) bool {
	for _, candidate := range lifecycleEdges[from] {
		if candidate == to {
			return true
		}
	}
	return false
}

func prepareState(t *testing.T, s *Store, id string, target model.State) model.Record {
	t.Helper()
	r := mustAdmit(t, s, testRecord("tenant", id))
	paths := map[model.State][]model.State{
		model.Starting:   {model.Starting},
		model.Activating: {model.Starting, model.Activating},
		model.Running:    {model.Starting, model.Activating, model.Running},
		model.Cancelling: {model.Cancelling},
		model.Succeeded:  {model.Starting, model.Activating, model.Running, model.Succeeded},
		model.Failed:     {model.Starting, model.Activating, model.Running, model.Failed},
		model.Cancelled:  {model.Cancelling, model.Cancelled},
	}
	for _, next := range paths[target] {
		var err error
		r, err = s.Transition(id, []model.State{r.State}, next, "fixture forward progress")
		if err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func assertPersistedInvariants(t *testing.T, s *Store, r model.Record) {
	t.Helper()
	terminals := 0
	for i, event := range r.Events {
		if i == 0 && event.State != model.Queued {
			t.Fatal("history did not start at durable admission")
		}
		if i > 0 {
			previous := r.Events[i-1]
			if previous.State == event.State || !permittedEdge(previous.State, event.State) || event.At.Before(previous.At) {
				t.Fatalf("invalid committed event edge: %+v -> %+v", previous, event)
			}
		}
		if event.State.Terminal() {
			terminals++
			if i != len(r.Events)-1 {
				t.Fatal("committed an event after a terminal outcome")
			}
		}
	}
	if terminals > 1 || r.State.Terminal() != (terminals == 1) || r.Events[len(r.Events)-1].State != r.State {
		t.Fatalf("state and terminal history disagree: %+v", r)
	}
	active, err := s.Active()
	if err != nil {
		t.Fatal(err)
	}
	reserved := false
	for _, record := range active {
		reserved = reserved || record.ID == r.ID
	}
	if reserved == r.State.Terminal() {
		t.Fatalf("reservation does not match terminal state %s", r.State)
	}
}

func TestStoreEnforcesEveryStatePairAtomically(t *testing.T) {
	s := openTestStore(t)
	targets := append(append([]model.State{}, lifecycleStates...), "unknown", "")
	for i, from := range lifecycleStates {
		for j, to := range targets {
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				before := prepareState(t, s, fmt.Sprintf("pair-%d-%d", i, j), from)
				changed := s.Changed()
				// Even the complete expected-state list cannot bypass the graph.
				_, err := s.Transition(before.ID, lifecycleStates, to, "attempted update")
				permitted := permittedEdge(from, to)
				if permitted && err != nil || !permitted && !errors.Is(err, ErrConflict) {
					t.Fatalf("%s -> %s returned %v; permitted=%v", from, to, err, permitted)
				}
				after, err := s.Get(before.TenantID, before.ID)
				if err != nil {
					t.Fatal(err)
				}
				progress := permitted && from != to
				if !progress && !reflect.DeepEqual(after, before) {
					t.Fatal("rejection or no-op changed persisted state, reason or history")
				}
				if progress && (after.State != to || len(after.Events) != len(before.Events)+1) {
					t.Fatal("accepted edge did not commit exactly one event")
				}
				select {
				case <-changed:
					if !progress {
						t.Fatal("rejection or no-op published a mutation")
					}
				default:
					if progress {
						t.Fatal("committed progress did not publish its mutation")
					}
				}
				assertPersistedInvariants(t, s, after)
			})
		}
	}
}

func TestUnknownPersistedStateCannotAuthorizeTransition(t *testing.T) {
	s := openTestStore(t)
	r := mustAdmit(t, s, testRecord("tenant", "corrupt-state"))
	r.State = "unknown"
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an unsupported/corrupt persisted state, not an authorized write.
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(recordsBucket).Put([]byte(r.ID), encoded) }); err != nil {
		t.Fatal(err)
	}
	for _, to := range append(append([]model.State{}, lifecycleStates...), "unknown") {
		if _, err := s.Transition(r.ID, []model.State{"unknown"}, to, "must not repair silently"); !errors.Is(err, ErrConflict) {
			t.Fatalf("unknown -> %s = %v", to, err)
		}
	}
	got, err := s.Get(r.TenantID, r.ID)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("unsupported persisted state was silently rewritten")
	}
}

func exerciseTransitionSequence(t *testing.T, data []byte) {
	t.Helper()
	if len(data) > 128 {
		data = data[:128]
	}
	s := openTestStore(t)
	r := mustAdmit(t, s, testRecord("tenant", "sequence"))
	targets := append(append([]model.State{}, lifecycleStates...), "unknown", "")
	for _, action := range data {
		before := r
		to := targets[int(action)%len(targets)]
		allowed := []model.State{before.State}
		matches := true
		switch int(action) / len(targets) % 3 {
		case 1:
			allowed = lifecycleStates // Broad caller expectations still obey the graph.
		case 2:
			allowed, matches = []model.State{"unknown"}, false
		}
		_, err := s.Transition(r.ID, allowed, to, "sequence progress")
		permitted := matches && permittedEdge(before.State, to)
		if permitted && err != nil || !permitted && !errors.Is(err, ErrConflict) {
			t.Fatalf("%s -> %s: %v, permitted=%v", before.State, to, err, permitted)
		}
		r, err = s.Get(r.TenantID, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if (!permitted || before.State == to) && !reflect.DeepEqual(before, r) {
			t.Fatal("failed CAS, illegal transition or no-op changed a committed record")
		}
		assertPersistedInvariants(t, s, r)
	}
	// Reachability is conditional on confirmed cleanup, not a claim that an
	// unavailable cluster or permanently partitioned worker must terminate.
	if !r.State.Terminal() {
		if r.State != model.Cancelling {
			var err error
			r, err = s.Transition(r.ID, []model.State{r.State}, model.Cancelling, "cleanup requested")
			if err != nil {
				t.Fatal(err)
			}
		}
		var err error
		r, err = s.Transition(r.ID, []model.State{model.Cancelling}, model.Cancelled, "cleanup confirmed")
		if err != nil {
			t.Fatal(err)
		}
	}
	assertPersistedInvariants(t, s, r)
	if _, created, err := s.Admit(testRecord("tenant", "after-terminal"), "", 1, 1); err != nil || !created {
		t.Fatalf("terminal result did not release its reservation: %v", err)
	}
}

func TestRandomTransitionSequencesPreserveHistoryAndReservations(t *testing.T) {
	random := rand.New(rand.NewPCG(0x9d09, 0x4147))
	for i := range 24 {
		t.Run(fmt.Sprintf("sequence-%d", i), func(t *testing.T) {
			data := make([]byte, 96)
			for j := range data {
				data[j] = byte(random.Uint32())
			}
			exerciseTransitionSequence(t, data)
		})
	}
}

func FuzzStoreTransitionInvariants(f *testing.F) {
	for _, seed := range [][]byte{{}, {1, 2, 3, 5}, {4, 7}, {3, 1, 0, 2, 4, 5, 7}, {255, 6, 7, 1, 0}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) { exerciseTransitionSequence(t, data) })
}
