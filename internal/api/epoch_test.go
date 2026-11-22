package api

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

func epochRequest(s *Server, path, tenantToken, lastID string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+tenantToken)
	r.Header.Set("AgentOS-Events-Version", "3")
	if lastID != "" {
		r.Header.Set("Last-Event-ID", lastID)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}

func TestEpochEventsAndPaginationAcrossOfflineRestore(t *testing.T) {
	s := testServer(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "source.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Store = db
	t.Cleanup(func() { _ = s.Store.Close() })
	a, b := strings.Repeat("a", 32), strings.Repeat("b", 32)
	for _, id := range []string{a, b} {
		admitEventRun(t, s, id, "alice")
		if _, err := db.TransitionWithCode(id, []model.State{model.Queued}, model.Failed, "fixture", model.ReasonTaskError); err != nil {
			t.Fatal(err)
		}
	}
	epoch, err := db.Epoch()
	if err != nil {
		t.Fatal(err)
	}
	events := "/v1/runs/" + a + "/events"
	w := epochRequest(s, events, a, "")
	if w.Code != 200 || w.Header().Get("AgentOS-Events-Version") != "3" || w.Header().Get("AgentOS-Store-Epoch") != epoch ||
		!strings.Contains(w.Body.String(), "id: "+epoch+":1\n") || !strings.Contains(w.Body.String(), `"store_epoch":"`+epoch+`"`) ||
		!strings.Contains(w.Body.String(), `"last_event_id":"`+epoch+`:2"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := epochRequest(s, events, a, epoch+":1"); w.Code != 200 || strings.Contains(w.Body.String(), "id: "+epoch+":1\n") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := epochRequest(s, events, a, epoch+":2"); w.Code != 204 || w.Header().Get("AgentOS-Store-Epoch") != epoch {
		t.Fatal(w.Code)
	}
	for _, cursor := range []string{"1", "", epoch + ":3", epoch + ":-1", strings.ToUpper(epoch) + ":1", epoch + ":", epoch + ":1:2", epoch + ":9999999999999999999999"} {
		w := epochRequest(s, events+"?after="+cursor, a, "")
		if w.Code != 400 || errorCode(t, w) != "invalid_cursor" {
			t.Fatal(cursor, w.Code, w.Body.String())
		}
	}
	if w := epochRequest(s, events+"?after=0", a, epoch+":1"); w.Code != 400 {
		t.Fatal("ambiguous cursor accepted", w.Code)
	}
	page := request(s, "GET", "/v1/runs?limit=1", a, "", "")
	var listing struct {
		Next string `json:"next_cursor"`
	}
	if err := json.Unmarshal(page.Body.Bytes(), &listing); err != nil || page.Code != 200 || listing.Next == "" {
		t.Fatal(page.Code, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(dir, "backup.db")
	info, err := store.BackupOffline(path, backup)
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(dir, "restored.db")
	if _, err := store.RestoreOffline(backup, restored, info.SHA256); err != nil {
		t.Fatal(err)
	}
	s.Store, err = store.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	newEpoch, err := s.Store.Epoch()
	if err != nil || newEpoch == epoch {
		t.Fatal("restore reused epoch", err)
	}
	// Even an old cursor still within the restored event count must fail.
	for _, old := range []string{epoch + ":1", epoch + ":2"} {
		w := epochRequest(s, events, a, old)
		if w.Code != 409 || errorCode(t, w) != "store_epoch_mismatch" {
			t.Fatal(w.Code, w.Body.String())
		}
		if w := epochRequest(s, events, b, old); w.Code != 404 || w.Header().Get("AgentOS-Store-Epoch") != "" {
			t.Fatal("foreign run disclosed epoch", w.Code)
		}
	}
	w = request(s, "GET", "/v1/runs?before="+listing.Next, a, "", "")
	if w.Code != 409 || errorCode(t, w) != "store_epoch_mismatch" {
		t.Fatal(w.Code, w.Body.String())
	}
	w = request(s, "GET", "/v1/runs?before="+listing.Next, b, "", "")
	if w.Code != 400 || errorCode(t, w) != "invalid_cursor" {
		t.Fatal("foreign list cursor disclosed restore", w.Code)
	}
	if w := epochRequest(s, events+"?after=0", a, ""); w.Code != 200 || w.Header().Get("AgentOS-Store-Epoch") != newEpoch {
		t.Fatal(w.Code)
	}
	if w := epochRequest(s, events, a, newEpoch+":2"); w.Code != 204 {
		t.Fatal(w.Code)
	}
	// Legacy projections remain wire compatible, with explicitly weaker restore semantics.
	if w := request(s, "GET", events+"?after=1", a, "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "id: 2\n") || strings.Contains(w.Body.String(), `"store_epoch"`) {
		t.Fatal(w.Code, w.Body.String())
	}
}
