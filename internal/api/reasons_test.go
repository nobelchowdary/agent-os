package api

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
)

func TestReasonCodesAreDocumentedAndStreamed(t *testing.T) {
	schemas := contract(t)["components"].(map[string]any)["schemas"].(map[string]any)
	values := schemas["ReasonCode"].(map[string]any)["enum"].([]any)
	known := []model.ReasonCode{model.ReasonCompleted, model.ReasonTaskError, model.ReasonDeadlineExceeded, model.ReasonCancelled, model.ReasonOOMKilled, model.ReasonEvicted, model.ReasonImagePullFailed, model.ReasonPolicyDenied, model.ReasonBudgetExceeded, model.ReasonInfraFailure, model.ReasonUnknown}
	if len(values) != len(known) {
		t.Fatal("reason vocabulary contract drift")
	}
	for i, code := range known {
		if !code.Valid() || values[i] != string(code) {
			t.Fatal("reason contract drift", code)
		}
	}
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	if _, err := s.Store.TransitionWithCode(id, []model.State{model.Queued}, model.Failed, "fixture failure", model.ReasonTaskError); err != nil {
		t.Fatal(err)
	}
	path := "/v1/runs/" + id + "/events"
	w := request(s, "GET", path, strings.Repeat("a", 32), "", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), `"reason_code"`) || w.Header().Get("AgentOS-Events-Version") != "1" {
		t.Fatal("legacy SSE format changed", w.Code, w.Body.String())
	}
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	r.Header.Set("AgentOS-Events-Version", "2")
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reason_code":"task_error"`) || !strings.Contains(w.Body.String(), "event: end\n") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request(s, "GET", "/v1/runs/"+id, strings.Repeat("a", 32), "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"reason_code":"task_error"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, version := range []string{"", "0", "3", "1,2"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
		r.Header["Agentos-Events-Version"] = []string{version}
		w = httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 400 || errorCode(t, w) != "invalid_event_version" {
			t.Fatal("invalid event version accepted", version, w.Code)
		}
	}
}
