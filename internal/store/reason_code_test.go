package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
)

func TestReasonCodeValidationIsAtomicAndStateSpecific(t *testing.T) {
	for _, tc := range []struct {
		name string
		to   model.State
		code model.ReasonCode
		ok   bool
	}{
		{"success", model.Succeeded, model.ReasonCompleted, true},
		{"success missing", model.Succeeded, "", false},
		{"success task error", model.Succeeded, model.ReasonTaskError, false},
		{"failure deadline", model.Failed, model.ReasonDeadlineExceeded, true},
		{"failure unknown", model.Failed, model.ReasonUnknown, true},
		{"failure missing", model.Failed, "", false},
		{"failure success", model.Failed, model.ReasonCompleted, false},
		{"failure cancelled", model.Failed, model.ReasonCancelled, false},
		{"failure unsupported", model.Failed, "made_up", false},
		{"cleanup cancellation", model.Cancelling, model.ReasonCancelled, true},
		{"cleanup infrastructure", model.Cancelling, model.ReasonInfraFailure, true},
		{"cleanup missing legacy", model.Cancelling, "", true},
		{"cleanup success", model.Cancelling, model.ReasonCompleted, false},
		{"starting missing", model.Starting, "", true},
		{"starting failure", model.Starting, model.ReasonInfraFailure, false},
		{"running missing", model.Running, "", true},
		{"running failure", model.Running, model.ReasonTaskError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTestStore(t)
			before := mustAdmit(t, s, testRecord("tenant", "run"))
			changed := s.Changed()
			got, err := s.TransitionWithCode(before.ID, []model.State{model.Queued}, tc.to, "observed outcome", tc.code)
			if tc.ok {
				if err != nil || got.ReasonCode != tc.code || got.Events[len(got.Events)-1].ReasonCode != tc.code {
					t.Fatalf("reason code not committed with event: %+v, %v", got, err)
				}
				select {
				case <-changed:
				default:
					t.Fatal("committed update did not notify")
				}
			} else {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("expected conflict, got %v", err)
				}
				after, _ := s.Get("tenant", before.ID)
				if !reflect.DeepEqual(before, after) {
					t.Fatal("invalid code changed record/history")
				}
				select {
				case <-changed:
					t.Fatal("invalid code notified")
				default:
				}
			}
			active, err := s.Active()
			wantActive := 1
			if tc.ok && tc.to.Terminal() {
				wantActive = 0
			}
			if err != nil || len(active) != wantActive {
				t.Fatalf("reservation changed incorrectly: %v %v", active, err)
			}
		})
	}
}

func TestReasonCodeCleanupIntentPersistsAndNoopCannotRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "runs.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	input := testRecord("tenant", "run")
	input.ReasonCode = model.ReasonCompleted
	input.Events = []model.Event{{State: model.Succeeded, ReasonCode: model.ReasonCompleted}}
	original := mustAdmit(t, s, input)
	if original.ReasonCode != "" || len(original.Events) != 1 || original.Events[0].ReasonCode != "" {
		t.Fatal("caller supplied outcome survived queued admission")
	}
	pending, err := s.TransitionWithCode("run", []model.State{model.Queued}, model.Cancelling, "deadline cleanup", model.ReasonDeadlineExceeded)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get("tenant", "run")
	if err != nil || !reflect.DeepEqual(got, pending) {
		t.Fatalf("cleanup intent lost on restart: %+v %v", got, err)
	}
	for _, code := range []model.ReasonCode{"", "invalid", model.ReasonCompleted, model.ReasonCancelled} {
		changed := s.Changed()
		noop, err := s.TransitionWithCode("run", []model.State{model.Cancelling}, model.Cancelling, "replace intent", code)
		if err != nil || !reflect.DeepEqual(noop, pending) {
			t.Fatalf("same-state call changed cleanup intent: %+v %v", noop, err)
		}
		select {
		case <-changed:
			t.Fatal("no-op notified")
		default:
		}
	}
	finished, err := s.TransitionWithCode("run", []model.State{model.Cancelling}, model.Failed, "cleanup confirmed", pending.ReasonCode)
	if err != nil {
		t.Fatal(err)
	}
	changed := s.Changed()
	legacyNoop, err := s.Transition("run", []model.State{model.Failed}, model.Failed, "legacy default unknown")
	if err != nil || !reflect.DeepEqual(legacyNoop, finished) {
		t.Fatal("legacy no-op replaced specific terminal code")
	}
	select {
	case <-changed:
		t.Fatal("terminal no-op notified")
	default:
	}
}

func TestCancellationRequiresCancellationCodeAndLegacyDefaults(t *testing.T) {
	s := openTestStore(t)
	mustAdmit(t, s, testRecord("tenant", "cancel"))
	if _, err := s.Transition("cancel", []model.State{model.Queued}, model.Cancelling, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	for _, code := range []model.ReasonCode{"", model.ReasonUnknown, model.ReasonCompleted} {
		if _, err := s.TransitionWithCode("cancel", []model.State{model.Cancelling}, model.Cancelled, "done", code); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid cancellation code %q: %v", code, err)
		}
	}
	r, err := s.Transition("cancel", []model.State{model.Cancelling}, model.Cancelled, "done")
	if err != nil || r.ReasonCode != model.ReasonCancelled {
		t.Fatalf("legacy cancellation default: %+v %v", r, err)
	}
	for _, state := range []model.State{model.Succeeded, model.Failed} {
		id := string(state)
		mustAdmit(t, s, testRecord("tenant", id))
		r, err := s.Transition(id, []model.State{model.Queued}, state, "done")
		if err != nil || r.ReasonCode != model.DefaultReasonCode(state) {
			t.Fatalf("legacy %s default: %+v %v", state, r, err)
		}
	}
}
