package api

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

type deletedExecutionBackend struct{ unavailable }

func (deletedExecutionBackend) Logs(context.Context, string, int64) (string, error) {
	panic("retained result must not contact deleted execution")
}

func TestRetainedResultAndLogsSurviveRestartWithNoExecution(t *testing.T) {
	s := testServer(t)
	path := filepath.Join(t.TempDir(), "private", "result.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Store = db
	t.Cleanup(func() { _ = s.Store.Close() })
	id, a, b := strings.Repeat("a", 32), strings.Repeat("a", 32), strings.Repeat("b", 32)
	admitEventRun(t, s, id, "alice")
	exit := int32(3)
	now := time.Now().UTC()
	result := model.ExecutionResult{ExitCode: &exit, OutputHead: "first line\n", OutputTail: "traceback last line\n", OutputTruncated: true, OutputComplete: true, Status: "captured", CapturedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := db.SaveResult(id, result); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionWithCode(id, []model.State{model.Queued}, model.Failed, "failure", model.ReasonTaskError); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s.Store, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s.Backend = deletedExecutionBackend{}
	w := request(s, "GET", "/v1/runs/"+id, a, "", "")
	var record model.Record
	if err := json.Unmarshal(w.Body.Bytes(), &record); err != nil || w.Code != 200 || record.Result == nil || record.Result.ExitCode == nil || *record.Result.ExitCode != 3 || record.Result.OutputTail != result.OutputTail {
		t.Fatal(w.Code, err)
	}
	w = request(s, "GET", "/v1/runs/"+id+"/logs", a, "", "")
	var logs struct {
		Logs     string `json:"logs"`
		Source   string `json:"source"`
		ExitCode *int32 `json:"exit_code"`
		Complete bool   `json:"output_complete"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &logs); err != nil || w.Code != 200 || logs.Source != "retained" || !logs.Complete || logs.ExitCode == nil || *logs.ExitCode != 3 || !strings.HasPrefix(logs.Logs, result.OutputHead) || !strings.HasSuffix(logs.Logs, result.OutputTail) {
		t.Fatal(w.Code, err)
	}
	for _, suffix := range []string{"", "/logs"} {
		if w := request(s, "GET", "/v1/runs/"+id+suffix, b, "", ""); w.Code != 404 || strings.Contains(w.Body.String(), "traceback") {
			t.Fatal("foreign result leaked", w.Code)
		}
	}
	if w := request(s, "GET", "/v1/runs", a, "", ""); w.Code != 200 || strings.Contains(w.Body.String(), `"result"`) {
		t.Fatal("list included payload", w.Code)
	}
}

func TestResultExpiryAndUnavailableNeverFallBackToPodLogs(t *testing.T) {
	for _, status := range []string{"expired", "unavailable"} {
		t.Run(status, func(t *testing.T) {
			s := testServer(t)
			s.Backend = deletedExecutionBackend{}
			id, a := strings.Repeat("a", 32), strings.Repeat("a", 32)
			admitEventRun(t, s, id, "alice")
			now := time.Now().UTC()
			result := model.ExecutionResult{Status: "unavailable", CapturedAt: now, ExpiresAt: now.Add(time.Hour)}
			want := 404
			if status == "expired" {
				result.Status = "captured"
				result.OutputHead = "expired secret output"
				result.CapturedAt = now.Add(-2 * time.Hour)
				result.ExpiresAt = now.Add(-time.Hour)
				want = 410
			}
			if err := s.Store.SaveResult(id, result); err != nil {
				t.Fatal(err)
			}
			w := request(s, "GET", "/v1/runs/"+id+"/logs", a, "", "")
			if w.Code != want {
				t.Fatal(w.Code, w.Body.String())
			}
			if want == 410 && errorCode(t, w) != "result_expired" {
				t.Fatal(w.Body.String())
			}
			w = request(s, "GET", "/v1/runs/"+id, a, "", "")
			if w.Code != 200 || strings.Contains(w.Body.String(), "expired secret output") || !strings.Contains(w.Body.String(), `"status":"`+status+`"`) {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}

func TestArchivedLogRenderingPreservesBoundAndTailWithMultibyteOutput(t *testing.T) {
	s := testServer(t)
	s.Backend = deletedExecutionBackend{}
	id, a := strings.Repeat("a", 32), strings.Repeat("a", 32)
	admitEventRun(t, s, id, "alice")
	now := time.Now().UTC()
	tail := strings.Repeat("界", 21840) + "final traceback"
	if err := s.Store.SaveResult(id, model.ExecutionResult{OutputTail: tail, OutputTruncated: true, OutputComplete: true, Status: "captured", CapturedAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	w := request(s, "GET", "/v1/runs/"+id+"/logs", a, "", "")
	var logs struct {
		Logs string `json:"logs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &logs); err != nil || w.Code != 200 || !utf8.ValidString(logs.Logs) || len(logs.Logs) > 65536 || !strings.HasSuffix(logs.Logs, "final traceback") {
		t.Fatal(w.Code, err, len(logs.Logs))
	}
}
