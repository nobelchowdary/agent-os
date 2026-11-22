package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/controller"
	"github.com/nobelchowdary/agent-os/internal/model"
)

// This fixture acknowledges execution without running any submitted command.
// It tests the real client/HTTP/store/controller path, not Kubernetes isolation.
type workflowBackend struct{ staged, activated atomic.Bool }

func (*workflowBackend) Ready(context.Context) error { return nil }
func (b *workflowBackend) Submit(context.Context, model.Record) error {
	b.staged.Store(true)
	return nil
}
func (b *workflowBackend) Activate(context.Context, model.Record) error {
	if !b.staged.Load() {
		return errors.New("activation before staging")
	}
	b.activated.Store(true)
	return nil
}
func (b *workflowBackend) Observe(context.Context, string) (backend.Observation, error) {
	if !b.activated.Load() {
		return backend.Observation{State: model.Starting}, nil
	}
	return backend.Observation{State: model.Succeeded, Reason: "test fixture acknowledgment only"}, nil
}
func (*workflowBackend) Cancel(context.Context, string) error { return nil }
func (*workflowBackend) Logs(context.Context, string, int64) (string, error) {
	return "fixture result; no code executed\n", nil
}

func TestPythonClientAgainstRealAPIAndController(t *testing.T) {
	s := testServer(t)
	b := &workflowBackend{}
	s.Backend = b
	ctx, cancel := context.WithCancel(context.Background())
	c := &controller.Controller{Store: s.Store, Backend: b}
	s.ControllerStatus = c.Status
	done := make(chan struct{})
	go func() { defer close(done); c.Run(ctx) }()
	defer func() { cancel(); <-done }()
	h := httptest.NewServer(s.Handler())
	defer h.Close()
	module, err := filepath.Abs("../../sdk/python")
	if err != nil {
		t.Fatal(err)
	}
	deadline, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	script := `import os
from agentos_client import AgentOSClient
c = AgentOSClient(os.environ['AGENTOS_TEST_URL'], token_env='AGENTOS_TEST_TOKEN')
command = ['python', '-c', "raise RuntimeError('submitted code must not execute in this test')"]
r = c.create_run('python', command, 30, idempotency_key='integration-one')
again = c.create_run('python', command, 30, idempotency_key='integration-one')
assert r['id'] == again['id']
finished = c.wait(r['id'], timeout=5)
assert finished['state'] == 'succeeded'
events = list(c.iter_events(r['id'], timeout=5))
assert [e['sequence'] for e in events] == list(range(1, len(finished['events']) + 1))
assert events[-1]['state'] == 'succeeded'
assert c.logs(r['id'])['logs'] == 'fixture result; no code executed\n'
assert c.cancel(r['id'])['state'] == 'succeeded'
print('real HTTP/client lifecycle contract passed; execution is a test fixture')
`
	command := exec.CommandContext(deadline, "python3", "-c", script)
	command.Env = append(os.Environ(), "PYTHONPATH="+module, "AGENTOS_TEST_URL="+h.URL, "AGENTOS_TEST_TOKEN="+strings.Repeat("a", 32))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python client interoperability failed: %v\n%s", err, output)
	}
	if !b.staged.Load() || !b.activated.Load() {
		t.Fatal("workflow bypassed staged activation")
	}
}
