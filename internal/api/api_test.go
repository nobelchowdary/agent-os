package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/config"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

type unavailable struct{}

func (unavailable) Ready(context.Context) error                  { return errors.New("private cluster details") }
func (unavailable) Submit(context.Context, model.Record) error   { return nil }
func (unavailable) Activate(context.Context, model.Record) error { return nil }
func (unavailable) Observe(context.Context, string) (backend.Observation, error) {
	return backend.Observation{}, nil
}
func (unavailable) Cancel(context.Context, string) error                { return nil }
func (unavailable) Logs(context.Context, string, int64) (string, error) { return "hello", nil }

func testServer(t *testing.T) *Server {
	t.Helper()
	cfg, err := config.Load("../../config/example.json")
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxActivePerTenant = 2
	cfg.MaxActiveTotal = 3
	db, err := store.Open(filepath.Join(t.TempDir(), "state", "runs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Server{Config: cfg, Store: db, Backend: unavailable{}, Principals: []config.Principal{{TenantID: "alice", TokenHash: sha256.Sum256([]byte(strings.Repeat("a", 32)))}, {TenantID: "bob", TokenHash: sha256.Sum256([]byte(strings.Repeat("b", 32)))}}, Render: func(r model.Record) (any, error) { return map[string]string{"runtime_class": r.Spec.RuntimeClass}, nil }}
}

func request(s *Server, method, path, token, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestTenantBoundaryAndDurableIdempotency(t *testing.T) {
	s := testServer(t)
	a, b := strings.Repeat("a", 32), strings.Repeat("b", 32)
	body := `{"template":"python"}`
	if w := request(s, "POST", "/v1/runs", "", "", body); w.Code != 401 {
		t.Fatal(w.Code)
	}
	w := request(s, "POST", "/v1/runs", a, "task-1", body)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	var first model.Record
	if err := json.Unmarshal(w.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	w = request(s, "POST", "/v1/runs", a, "task-1", body)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var again model.Record
	_ = json.Unmarshal(w.Body.Bytes(), &again)
	if again.ID != first.ID {
		t.Fatal("retry created a new run")
	}
	if w := request(s, "POST", "/v1/runs", a, "task-1", `{"template":"node"}`); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, suffix := range []string{"", "/logs", "/cancel"} {
		method := "GET"
		if suffix == "/cancel" {
			method = "POST"
		}
		if w := request(s, method, "/v1/runs/"+first.ID+suffix, b, "", ""); w.Code != 404 {
			t.Fatal("cross-tenant access", suffix, w.Code)
		}
	}
	if w := request(s, "GET", "/v1/runs", b, "", ""); strings.Contains(w.Body.String(), first.ID) {
		t.Fatal("cross-tenant listing")
	}
	if w := request(s, "POST", "/v1/runs", a, "task-2", body); w.Code != 202 {
		t.Fatal(w.Code)
	}
	if w := request(s, "POST", "/v1/runs", a, "task-3", body); w.Code != 429 {
		t.Fatal(w.Code)
	}
	if w := request(s, "POST", "/v1/runs/"+first.ID+"/cancel", a, "", ""); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	v, err := s.Store.Get("alice", first.ID)
	if err != nil || v.State != model.Cancelling {
		t.Fatal(v, err)
	}
	if w := request(s, "POST", "/v1/runs", a, "task-3", body); w.Code != 429 {
		t.Fatal("cancel released reservation before cleanup", w.Code)
	}
}

func TestRejectsPodSpecBypassAndMalformedBodies(t *testing.T) {
	s := testServer(t)
	token := strings.Repeat("a", 32)
	for _, body := range []string{
		`{"template":"python","image":"evil"}`,
		`{"template":"python","runtime_class":"runc"}`,
		`{"template":"python","runtime_profile":{"kind":"aws-nitro-isolated","runtime_class":"isolated-sandbox","required_handler":"isolated-sandbox"}}`,
		`{"template":"python","tenant_id":"bob"}`,
		`{"template":"python","resources":{"cpu_milli":64000}}`,
		`{"template":"python","timeout_seconds":301}`,
		`{"template":"python"} {}`,
		`{"template":"missing"}`,
		`{"template":"python","command":[""]}`,
		strings.Repeat("x", 40000),
	} {
		if w := request(s, "POST", "/v1/runs", token, "", body); w.Code != 400 {
			t.Fatalf("body %.80s: %d %s", body, w.Code, w.Body.String())
		}
	}
	active, err := s.Store.Active()
	if err != nil || len(active) != 0 {
		t.Fatal("invalid requests were admitted", active, err)
	}
}

func TestPreviewDoesNotAdmitOrExecute(t *testing.T) {
	s := testServer(t)
	s.Preview = true
	token := strings.Repeat("a", 32)
	if w := request(s, "POST", "/v1/plan", token, "", `{"template":"python"}`); w.Code != 200 || !strings.Contains(w.Body.String(), `"execution":false`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request(s, "POST", "/v1/runs", token, "", `{"template":"python"}`); w.Code != 503 {
		t.Fatal(w.Code)
	}
	active, _ := s.Store.Active()
	if len(active) != 0 {
		t.Fatal("preview persisted execution")
	}
	id := strings.Repeat("a", 32)
	if _, _, err := s.Store.Admit(model.Record{ID: id, TenantID: "alice"}, "", 2, 3); err != nil {
		t.Fatal(err)
	}
	if w := request(s, "POST", "/v1/runs/"+id+"/cancel", token, "", ""); w.Code != 503 {
		t.Fatal("preview cancellation allowed", w.Code)
	}
	r, err := s.Store.Get("alice", id)
	if err != nil || r.State != model.Queued {
		t.Fatal("preview changed live state", r, err)
	}
	w := request(s, "GET", "/readyz", "", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), "disabled") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestReadinessDoesNotLeakClusterErrors(t *testing.T) {
	s := testServer(t)
	w := request(s, "GET", "/readyz", "", "", "")
	if w.Code != 503 || strings.Contains(w.Body.String(), "private cluster") {
		t.Fatal(w.Code, w.Body.String())
	}
}
