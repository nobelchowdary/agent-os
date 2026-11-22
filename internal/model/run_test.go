package model

import "testing"

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
