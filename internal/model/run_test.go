package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReasonCodeVocabularyAndDefaults(t *testing.T) {
	codes := []ReasonCode{ReasonCompleted, ReasonTaskError, ReasonDeadlineExceeded, ReasonCancelled,
		ReasonOOMKilled, ReasonEvicted, ReasonImagePullFailed, ReasonPolicyDenied,
		ReasonBudgetExceeded, ReasonInfraFailure, ReasonUnknown}
	for _, code := range codes {
		if !code.Valid() {
			t.Errorf("declared code %q is invalid", code)
		}
	}
	for _, code := range []ReasonCode{"", "OOMKilled", "custom", " task_error", "TASK_ERROR"} {
		if code.Valid() {
			t.Errorf("accepted undeclared code %q", code)
		}
	}
	for _, state := range []State{Queued, Starting, Activating, Running, Cancelling, Succeeded, Failed, Cancelled, "unknown"} {
		want := map[State]ReasonCode{Succeeded: ReasonCompleted, Failed: ReasonUnknown, Cancelled: ReasonCancelled}[state]
		if got := DefaultReasonCode(state); got != want {
			t.Errorf("default for %s = %q, want %q", state, got, want)
		}
	}
}

func TestReasonCodesAreOptionalForLegacyRecordsAndEvents(t *testing.T) {
	var legacy Record
	if err := json.Unmarshal([]byte(`{"state":"failed","events":[{"state":"failed","reason":"legacy prose"}]}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.ReasonCode != "" || legacy.Events[0].ReasonCode != "" {
		t.Fatal("reading legacy JSON invented a historical cause")
	}
	encoded, err := json.Marshal(legacy)
	if err != nil || strings.Contains(string(encoded), "reason_code") {
		t.Fatalf("legacy omissions did not remain optional: %s, %v", encoded, err)
	}
	legacy.ReasonCode, legacy.Events[0].ReasonCode = ReasonTaskError, ReasonTaskError
	encoded, err = json.Marshal(legacy)
	if err != nil || strings.Count(string(encoded), `"reason_code":"task_error"`) != 2 {
		t.Fatalf("structured record/event causes missing: %s, %v", encoded, err)
	}
}

func TestCanTransitionAllStatePairs(t *testing.T) {
	allowed := map[State][]State{
		Queued:     {Queued, Starting, Running, Cancelling, Succeeded, Failed},
		Starting:   {Starting, Activating, Running, Cancelling, Succeeded, Failed},
		Activating: {Activating, Running, Cancelling, Succeeded, Failed},
		Running:    {Running, Cancelling, Succeeded, Failed},
		Cancelling: {Cancelling, Failed, Cancelled},
		Succeeded:  {Succeeded},
		Failed:     {Failed},
		Cancelled:  {Cancelled},
	}
	states := []State{Queued, Starting, Activating, Running, Cancelling, Succeeded, Failed, Cancelled, "", "unknown", "RUNNING"}
	for _, from := range states {
		for _, to := range states {
			want := false
			for _, candidate := range allowed[from] {
				want = want || candidate == to
			}
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%q, %q) = %v; want %v", from, to, got, want)
			}
		}
	}
}

func TestLifecycleHasNoCyclesExceptIdempotentSelfTransitions(t *testing.T) {
	states := []State{Queued, Starting, Activating, Running, Cancelling, Succeeded, Failed, Cancelled}
	var visit func(State, map[State]bool)
	visit = func(from State, path map[State]bool) {
		if path[from] {
			t.Fatalf("lifecycle contains a non-idempotent cycle at %s", from)
		}
		path[from] = true
		defer delete(path, from)
		for _, to := range states {
			if from != to && CanTransition(from, to) {
				visit(to, path)
			}
		}
	}
	for _, from := range states {
		visit(from, make(map[State]bool))
	}
}
