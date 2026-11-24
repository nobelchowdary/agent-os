package api

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
)

func TestPodDefaultPreservesExistingLegacyKeyAndRejectsCallerClock(t *testing.T) {
	s := testServer(t)
	token := strings.Repeat("a", 32)
	spec, err := s.Config.Resolve(model.CreateRequest{Template: "python"})
	if err != nil {
		t.Fatal(err)
	}
	spec.DeadlineScope = "" // An admission made by the previous binary.
	original, _, err := s.Store.Admit(model.Record{ID: strings.Repeat("c", 32), TenantID: "alice", Spec: spec}, "legacy", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	w := request(s, "POST", "/v1/runs", token, "legacy", `{"template":"python"}`)
	var got model.Record
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 200 || !reflect.DeepEqual(got, original) {
		t.Fatal("new clock default changed legacy retry", w.Code, got, err)
	}
	w = request(s, "POST", "/v1/runs", token, "new", `{"template":"python"}`)
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != 202 || got.Spec.DeadlineScope != model.DeadlinePod {
		t.Fatal("new admission did not persist Pod clock", w.Code, got, err)
	}
	if w := request(s, "POST", "/v1/runs", token, "legacy", `{"template":"python","timeout_seconds":61}`); w.Code != 409 {
		t.Fatal("legacy clock alias hid a changed request", w.Code, w.Body.String())
	}
	if w := request(s, "POST", "/v1/runs", token, "caller-clock", `{"template":"python","deadline_scope":"job"}`); w.Code != 400 {
		t.Fatal("caller selected administrator deadline clock", w.Code, w.Body.String())
	}
}
