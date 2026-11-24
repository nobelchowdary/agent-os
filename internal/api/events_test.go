package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/controller"
	"github.com/nobelchowdary/agent-os/internal/model"
)

func admitEventRun(t *testing.T, s *Server, id, tenant string) model.Record {
	t.Helper()
	v, _, err := s.Store.Admit(model.Record{ID: id, TenantID: tenant}, "", 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestEventReplayResumeAndTenantScope(t *testing.T) {
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	if _, err := s.Store.Transition(id, []model.State{model.Queued}, model.Failed, "reason\nwith newline"); err != nil {
		t.Fatal(err)
	}
	path := "/v1/runs/" + id + "/events"
	a, b := strings.Repeat("a", 32), strings.Repeat("b", 32)
	if w := request(s, "GET", path, "", "", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := request(s, "GET", path, b, "", ""); w.Code != 404 {
		t.Fatal(w.Code)
	}
	w := request(s, "GET", path, a, "", "")
	if w.Code != 200 || w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatal(w.Code, w.Header())
	}
	if strings.Count(w.Body.String(), "event: run\n") != 2 || !strings.Contains(w.Body.String(), "event: end\n") || !strings.Contains(w.Body.String(), `reason\nwith newline`) {
		t.Fatal(w.Body.String())
	}
	resumed := request(s, "GET", path+"?after=1", a, "", "")
	if resumed.Code != 200 || strings.Contains(resumed.Body.String(), "id: 1\n") || !strings.Contains(resumed.Body.String(), "id: 2\n") {
		t.Fatal(resumed.Code, resumed.Body.String())
	}
	if w := request(s, "GET", path+"?after=2", a, "", ""); w.Code != 204 {
		t.Fatal(w.Code)
	}
	for _, query := range []string{"after=3", "after=-1", "after=", "after=1&after=1", "after=1.0", "token=private", "after=9999999999999999999999999999999", "after=%ff"} {
		if w := request(s, "GET", path+"?"+query, a, "", ""); w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+a)
	r.Header.Set("Last-Event-ID", "1")
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if strings.Contains(w.Body.String(), "id: 1\n") || !strings.Contains(w.Body.String(), "id: 2\n") {
		t.Fatal(w.Body.String())
	}
	r = httptest.NewRequest("GET", path+"?after=1", nil)
	r.Header.Set("Authorization", "Bearer "+a)
	r.Header.Set("Last-Event-ID", "1")
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("ambiguous resume cursor accepted", w.Code)
	}
}

func TestLiveEventsDeliverCommitsAndReleaseOnDisconnect(t *testing.T) {
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	h := httptest.NewServer(s.Handler())
	defer h.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", h.URL+"/v1/runs/"+id+"/events?after=1", nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	response, err := h.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != ": connected\n" {
		t.Fatal(line, err)
	}
	if _, err := s.Store.Transition(id, []model.State{model.Queued}, model.Cancelling, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store.Transition(id, []model.State{model.Cancelling}, model.Cancelled, "cancel requested"); err != nil {
		t.Fatal(err)
	}
	var sequences []int
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.HasPrefix(line, "id: ") {
			var sequence int
			if _, err := fmt.Sscanf(line, "id: %d", &sequence); err != nil {
				t.Fatal(err)
			}
			sequences = append(sequences, sequence)
		}
	}
	if fmt.Sprint(sequences) != "[2 3]" {
		t.Fatal(sequences)
	}
	// Terminal completion closes the handler and releases its bounded slot.
	deadline := time.Now().Add(time.Second)
	for {
		s.streamMu.Lock()
		count := s.streams
		s.streamMu.Unlock()
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream slot leaked")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreamBoundsAndRequestCancellation(t *testing.T) {
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	for i := 0; i < maxTenantStreams; i++ {
		if !s.reserveStream("alice") {
			t.Fatal(i)
		}
	}
	if w := request(s, "GET", "/v1/runs/"+id+"/events", strings.Repeat("a", 32), "", ""); w.Code != 429 {
		t.Fatal(w.Code)
	}
	for i := 0; i < maxTenantStreams; i++ {
		s.releaseStream("alice")
	}
	for i := 0; i < maxStreams; i++ {
		if !s.reserveStream(fmt.Sprint(i)) {
			t.Fatal(i)
		}
	}
	if s.reserveStream("another") {
		t.Fatal("global stream limit exceeded")
	}
	for i := 0; i < maxStreams; i++ {
		s.releaseStream(fmt.Sprint(i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/v1/runs/"+id+"/events", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); s.Handler().ServeHTTP(w, r) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled stream retained handler")
	}
	if s.streams != 0 {
		t.Fatal("cancelled stream retained capacity")
	}
}

func TestClientDisconnectReleasesActiveStream(t *testing.T) {
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	h := httptest.NewServer(s.Handler())
	defer h.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", h.URL+"/v1/runs/"+id+"/events", nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	response, err := h.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	deadline := time.Now().Add(time.Second)
	for {
		s.streamMu.Lock()
		count := s.streams
		s.streamMu.Unlock()
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("disconnected live stream retained capacity")
		}
		time.Sleep(time.Millisecond)
	}
	if run, err := s.Store.Get("alice", id); err != nil || run.State != model.Queued {
		t.Fatal("disconnect changed execution", run.State, err)
	}
}

func TestHTTP2IdleStreamSurvivesPerWriteTimeout(t *testing.T) {
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	h := httptest.NewUnstartedServer(s.Handler())
	h.EnableHTTP2 = true
	h.StartTLS()
	defer h.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "GET", h.URL+"/v1/runs/"+id+"/events?after=1", nil)
	r.Header.Set("Authorization", "Bearer "+strings.Repeat("a", 32))
	response, err := h.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 {
		t.Fatal("test requires real HTTP/2")
	}
	// A deadline left armed during this idle interval resets HTTP/2 at 5s,
	// before the next heartbeat. The next committed event must still arrive.
	finished := make(chan error, 1)
	timer := time.AfterFunc(streamWriteTimeout+200*time.Millisecond, func() {
		_, err := s.Store.Transition(id, []model.State{model.Queued}, model.Failed, "fixture complete")
		finished <- err
	})
	defer timer.Stop()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal("idle stream reset", err)
	}
	if !strings.Contains(string(data), "id: 2\n") || !strings.Contains(string(data), "event: end\n") {
		t.Fatal(string(data))
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestStatusIsTenantScopedAndControllerAffectsReadiness(t *testing.T) {
	s := testServer(t)
	admitEventRun(t, s, strings.Repeat("a", 32), "alice")
	admitEventRun(t, s, strings.Repeat("b", 32), "bob")
	w := request(s, "GET", "/v1/status", strings.Repeat("a", 32), "", "")
	var status map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || status["tenant_active"] != float64(1) || strings.Contains(w.Body.String(), "bob") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := request(s, "GET", "/v1/status", "", "", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	snapshot := controller.Status{}
	s.ControllerStatus = func() controller.Status { return snapshot }
	for _, sample := range []controller.Status{{}, {Running: true}, {Running: true, LastCompletedAt: time.Now().Add(-time.Minute)}} {
		snapshot = sample
		w := request(s, "GET", "/readyz", "", "", "")
		if w.Code != 503 || !strings.Contains(w.Body.String(), "controller") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	snapshot = controller.Status{Running: true, LastCompletedAt: time.Now(), LastSuccessAt: time.Now()}
	if !s.controllerHealthy() {
		t.Fatal("fresh successful controller is unhealthy")
	}
}

type readyBackend struct{ unavailable }

func (readyBackend) Ready(context.Context) error { return nil }

func TestRunErrorsDoNotRemoveReadsOrCancellationFromReadyService(t *testing.T) {
	s := testServer(t)
	s.Backend = readyBackend{}
	run := admitEventRun(t, s, strings.Repeat("a", 32), "alice")
	s.ControllerStatus = func() controller.Status {
		return controller.Status{Running: true, LastCompletedAt: time.Now().Add(-time.Minute), LastProgressAt: time.Now(), ConsecutiveFailures: 100, RunErrors: 1, MaintenanceFailed: true}
	}
	if w := request(s, "GET", "/readyz", "", "", ""); w.Code != 200 {
		t.Fatalf("per-run failure removed the API from service: %d %s", w.Code, w.Body.String())
	}
	token := strings.Repeat("a", 32)
	for _, path := range []string{"/v1/status", "/v1/runs/" + run.ID} {
		if w := request(s, "GET", path, token, "", ""); w.Code != 200 {
			t.Fatal(path, w.Code)
		}
	}
	if w := request(s, "POST", "/v1/runs/"+run.ID+"/cancel", token, "", ""); w.Code != 202 {
		t.Fatal("per-run failure prevented cancellation", w.Code, w.Body.String())
	}
	// A stopped/stalled loop, broken store or missing sandbox prerequisites
	// still fails readiness; only per-run operation failures are isolated.
	s.ControllerStatus = func() controller.Status {
		return controller.Status{Running: true, LastProgressAt: time.Now().Add(-time.Minute)}
	}
	if w := request(s, "GET", "/readyz", "", "", ""); w.Code != 503 {
		t.Fatal("stalled controller was ready", w.Code)
	}
}

func TestListPaginationAPI(t *testing.T) {
	s := testServer(t)
	for i := 0; i < 3; i++ {
		admitEventRun(t, s, fmt.Sprintf("%032d", i), "alice")
	}
	token := strings.Repeat("a", 32)
	w := request(s, "GET", "/v1/runs?limit=2", token, "", "")
	var page struct {
		Runs []model.Record `json:"runs"`
		Next string         `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(page.Runs) != 2 || page.Next == "" {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request(s, "GET", "/v1/runs?limit=2&before="+page.Next, token, "", "")
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(page.Runs) != 1 || page.Next != "" {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=2&limit=3", "before=broken", "other=1", "limit=1;before=x"} {
		if w := request(s, "GET", "/v1/runs?"+query, token, "", ""); w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
}

type textLogBackend struct {
	workflowBackend
	text string
}

func (b *textLogBackend) Logs(context.Context, string, int64) (string, error) { return b.text, nil }

func TestLogResponsePreservesUTF8AndPublicByteLimit(t *testing.T) {
	s := testServer(t)
	id := strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	for _, raw := range []string{strings.Repeat("\xffx", 64*1024), strings.Repeat("€", 30000), strings.Repeat("x", 70000)} {
		s.Backend = &textLogBackend{text: raw}
		w := request(s, "GET", "/v1/runs/"+id+"/logs", strings.Repeat("a", 32), "", "")
		var value struct {
			Logs  string `json:"logs"`
			Limit int    `json:"limit_bytes"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || value.Limit != 65536 || len(value.Logs) > value.Limit || !utf8.ValidString(value.Logs) {
			t.Fatal("invalid public log bound", w.Code, len(value.Logs), value.Limit)
		}
	}
}
