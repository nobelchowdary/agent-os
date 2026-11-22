package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
)

func contract(t *testing.T) map[string]any {
	t.Helper()
	var spec map[string]any
	if err := json.Unmarshal(openAPISpec, &spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

func errorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal("non-JSON API error", w.Code, err)
	}
	code, ok := response["code"].(string)
	message, text := response["error"].(string)
	if !ok || !text || message == "" || len(response) != 2 {
		t.Fatal("invalid API error shape", response)
	}
	schemas := contract(t)["components"].(map[string]any)["schemas"].(map[string]any)
	values := schemas["Error"].(map[string]any)["properties"].(map[string]any)["code"].(map[string]any)["enum"].([]any)
	for _, value := range values {
		if value == code {
			return code
		}
	}
	t.Fatalf("undocumented API error code %q", code)
	return ""
}

// This walks the checked-in contract to generate route/authorization checks;
// it is deliberately not represented as a complete OpenAPI schema validator.
func TestOpenAPIReferencesAndAuthorizationMatrix(t *testing.T) {
	spec := contract(t)
	var walk func(any)
	walk = func(node any) {
		switch value := node.(type) {
		case map[string]any:
			if ref, ok := value["$ref"].(string); ok {
				if !strings.HasPrefix(ref, "#/") {
					t.Fatal("external contract reference", ref)
				}
				var target any = spec
				for _, key := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
					object, ok := target.(map[string]any)
					if !ok {
						t.Fatal("broken contract reference", ref)
					}
					target, ok = object[key]
					if !ok {
						t.Fatal("missing contract reference", ref)
					}
				}
			}
			for _, child := range value {
				walk(child)
			}
		case []any:
			for _, child := range value {
				walk(child)
			}
		}
	}
	walk(spec)
	s := testServer(t)
	paths := spec["paths"].(map[string]any)
	for path, raw := range paths {
		for method, operation := range raw.(map[string]any) {
			op := operation.(map[string]any)
			public := false
			if security, ok := op["security"].([]any); ok && len(security) == 0 {
				public = true
			}
			t.Run(method+" "+path, func(t *testing.T) {
				path = strings.ReplaceAll(path, "{id}", strings.Repeat("a", 32))
				w := request(s, strings.ToUpper(method), path, "", "", "")
				if !public {
					if w.Code != 401 || errorCode(t, w) != "unauthenticated" {
						t.Fatal(w.Code, w.Body.String())
					}
				} else if w.Code != 200 && w.Code != 503 {
					t.Fatal(w.Code)
				}
			})
		}
	}
	w := request(s, "GET", "/v1/openapi.json", strings.Repeat("a", 32), "", "")
	if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), openAPISpec) {
		t.Fatal("served contract differs from checked-in contract")
	}
}

func TestOpenAPIDerivedRequestValidation(t *testing.T) {
	schemas := contract(t)["components"].(map[string]any)["schemas"].(map[string]any)
	input := schemas["CreateRun"].(map[string]any)
	for _, path := range []string{"/v1/plan", "/v1/runs"} {
		s := testServer(t)
		for i, example := range input["x-contract-invalid-examples"].([]any) {
			encoded, _ := json.Marshal(example)
			w := request(s, "POST", path, strings.Repeat("a", 32), "contract", string(encoded))
			if w.Code != 400 || errorCode(t, w) != "invalid_request" {
				t.Fatalf("%s invalid example %d accepted: %d", path, i, w.Code)
			}
		}
		active, err := s.Store.Active()
		if err != nil || len(active) != 0 {
			t.Fatal("invalid contract input admitted work", len(active), err)
		}
		encoded, _ := json.Marshal(input["x-contract-valid-example"])
		w := request(s, "POST", path, strings.Repeat("a", 32), "contract", string(encoded))
		if w.Code != 200 && w.Code != 202 {
			t.Fatal("valid contract example rejected", w.Code, w.Body.String())
		}
	}
	declared := schemas["State"].(map[string]any)["enum"].([]any)
	actual := []model.State{model.Queued, model.Starting, model.Activating, model.Running, model.Cancelling, model.Succeeded, model.Failed, model.Cancelled}
	if len(declared) != len(actual) {
		t.Fatal("state contract drift")
	}
	for i, value := range actual {
		if declared[i] != string(value) {
			t.Fatal("state contract drift", value)
		}
	}
}

func TestStrictRequestsAndStableRoutingErrors(t *testing.T) {
	s := testServer(t)
	token := strings.Repeat("a", 32)
	for _, body := range []string{
		`{"template":"python","template":"node"}`,
		`{"template":"python","command":["python",null]}`,
		`{"template":"python","command":["python"],"command":["node"]}`,
		`{"template":"python","Timeout_Seconds":1}`,
		"{\"template\":\"python\",\"command\":[\"\xff\"]}",
		strings.Repeat(" ", 32769) + `{"template":"python"}`,
	} {
		w := request(s, "POST", "/v1/runs", token, "strict", body)
		if w.Code != 400 || errorCode(t, w) != "invalid_request" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, scenario := range []string{"media-type", "duplicate-auth", "duplicate-key", "empty-key"} {
		r := httptest.NewRequest("POST", "/v1/runs", strings.NewReader(`{"template":"python"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Idempotency-Key", "strict")
		status, code := 400, "invalid_idempotency_key"
		switch scenario {
		case "media-type":
			r.Header.Set("Content-Type", "text/plain")
			status, code = 415, "unsupported_media_type"
		case "duplicate-auth":
			r.Header.Add("Authorization", "Bearer "+strings.Repeat("b", 32))
			status, code = 401, "unauthenticated"
		case "duplicate-key":
			r.Header.Add("Idempotency-Key", "other")
		case "empty-key":
			r.Header.Set("Idempotency-Key", "")
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != status || errorCode(t, w) != code {
			t.Fatal(scenario, w.Code, w.Body.String())
		}
	}
	w := request(s, "GET", "/unknown", token, "", "")
	if w.Code != 404 || errorCode(t, w) != "route_not_found" {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request(s, "DELETE", "/v1/runs", token, "", "")
	if w.Code != 405 || errorCode(t, w) != "method_not_allowed" || w.Header().Get("Allow") == "" {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestStableErrorsQuotaConflictAndStorageRetry(t *testing.T) {
	s := testServer(t)
	token := strings.Repeat("a", 32)
	for i := 0; i < 2; i++ {
		if w := request(s, "POST", "/v1/runs", token, fmt.Sprint(i), `{"template":"python"}`); w.Code != 202 {
			t.Fatal(w.Code)
		}
	}
	w := request(s, "POST", "/v1/runs", token, "2", `{"template":"python"}`)
	if w.Code != 429 || errorCode(t, w) != "quota_exceeded" || w.Header().Get("Retry-After") == "" {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request(s, "POST", "/v1/runs", token, "0", `{"template":"node"}`)
	if w.Code != 409 || errorCode(t, w) != "idempotency_conflict" {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := s.Store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/runs", "/v1/status", "/readyz"} {
		w = request(s, "GET", path, token, "", "")
		if w.Code != 503 || errorCode(t, w) != "storage_unavailable" || w.Header().Get("Retry-After") == "" {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}

func FuzzCreateRequestContract(f *testing.F) {
	for _, seed := range []string{`{"template":"python"}`, `{"template":"python","command":[null]}`, `{"template":"python","template":"node"}`, `null`, `[]`, `{"template":"python","timeout_seconds":1.5}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 65536 {
			t.Skip()
		}
		s := testServer(t)
		w := request(s, "POST", "/v1/plan", strings.Repeat("a", 32), "", body)
		if w.Code != 200 && w.Code != 400 {
			t.Fatal("unexpected parser status", w.Code)
		}
		if w.Code == 400 {
			errorCode(t, w)
		}
		active, err := s.Store.Active()
		if err != nil || !reflect.DeepEqual(active, []model.Record{}) {
			t.Fatal("plan admitted work", err)
		}
	})
}
