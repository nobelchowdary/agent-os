package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

func TestReadyPositiveCacheSingleFlightExpiresAndNeverCachesFailure(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	now := time.Now()
	b.readyNow = func() time.Time { return now }
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Ready(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(client.Actions()) != 3 {
		t.Fatal("repeated declaration lookups within positive cache", len(client.Actions()))
	}
	now = now.Add(3 * time.Second)
	if err := b.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 6 {
		t.Fatal("cache did not expire", len(client.Actions()))
	}
	if err := client.NetworkingV1().NetworkPolicies("work").Delete(ctx, DenyAllPolicy, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Second)
	client.ClearActions()
	for i := 0; i < 2; i++ {
		if err := b.Ready(ctx); err == nil {
			t.Fatal("unsafe prerequisite cached")
		}
	}
	if len(client.Actions()) != 5 {
		// The successful RuntimeClass lookup is shared independently; the
		// failing policy and namespace checks still run on both attempts.
		t.Fatal("failure was cached or positive RuntimeClass was refetched", len(client.Actions()))
	}
	if err := client.Tracker().Add(fixtures()[2]); err != nil {
		t.Fatal(err)
	}
	if err := b.Ready(ctx); err != nil {
		t.Fatal("failure prevented immediate recovery", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := b.Ready(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("cache bypassed cancelled request")
	}
}

func TestReadyWaiterHonorsOwnDeadline(t *testing.T) {
	b, client := newTestBackend(t, fixtures()...)
	entered, release := make(chan struct{}), make(chan struct{})
	client.PrependReactor("get", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		close(entered)
		<-release
		return false, nil, nil
	})
	done := make(chan error, 1)
	go func() { done <- b.Ready(context.Background()) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Ready(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBoundedOutputRetainsNonoverlappingHeadAndActualTail(t *testing.T) {
	for _, n := range []int{0, 1, resultPartBytes - 1, resultPartBytes, resultPartBytes + 1, 2 * resultPartBytes, 4 * resultPartBytes} {
		body := []byte(strings.Repeat("a", n))
		if n > 0 {
			body[n-1] = 'z'
		}
		head, tail, count, complete, err := boundedOutput(bytes.NewReader(body), captureScanBytes)
		if err != nil || count != int64(n) || !complete || len(head) > resultPartBytes || len(tail) > resultPartBytes {
			t.Fatal(n, count, complete)
		}
		if n <= 2*resultPartBytes && !bytes.Equal(append(append([]byte{}, head...), tail...), body) {
			t.Fatal("short output overlapped/lost bytes", n)
		}
		if n > 2*resultPartBytes && (!bytes.Equal(head, body[:resultPartBytes]) || !bytes.Equal(tail, body[n-resultPartBytes:])) {
			t.Fatal("did not retain final tail")
		}
	}
	head, tail, count, complete, err := boundedOutput(bytes.NewReader([]byte("abcdef")), 3)
	if err != nil || string(head) != "abc" || len(tail) != 0 || count != 4 || complete {
		t.Fatal("scan limit mislabeled complete", string(head), count, complete)
	}
	if output, clipped := validOutput(bytes.Repeat([]byte{255, 'a'}, resultPartBytes/2), false); len(output) > resultPartBytes || !utf8.ValidString(output) || !clipped {
		t.Fatal("invalid bounded UTF8")
	}
}

func TestCaptureResultKeepsExitCodeAndLongOutputTail(t *testing.T) {
	b, _ := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	pod := ownedPod(job, "pod-1")
	pod.Status = terminatedAgent(3, "Error", "")
	body := "first line\n" + strings.Repeat("x", 4*resultPartBytes) + "\nlast traceback\n"
	for _, status := range []int{200, 400, 404, 503} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/apis/batch/v1/namespaces/work/jobs/agentos-run-1":
				_ = json.NewEncoder(w).Encode(job)
			case "/api/v1/namespaces/work/pods":
				_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{*pod}})
			case "/api/v1/namespaces/work/pods/pod-1/log":
				if r.URL.Query().Get("limitBytes") != "16777217" || r.URL.Query().Get("container") != "agent" || r.URL.Query().Get("follow") == "true" {
					t.Error("unbounded/following capture")
				}
				if status != 200 {
					http.Error(w, "fixture unavailable", status)
					return
				}
				w.Header().Set("Content-Type", "text/plain")
				_, _ = io.WriteString(w, body)
			default:
				http.NotFound(w, r)
			}
		}))
		client, err := clientset.NewForConfig(&rest.Config{Host: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		b, err = New(client, "work", "gvisor")
		if err != nil {
			t.Fatal(err)
		}
		result, err := b.CaptureResult(context.Background(), "run-1")
		server.Close()
		if (status == 503) != (err != nil) || result.ExitCode == nil || *result.ExitCode != 3 || result.CapturedAt.IsZero() {
			t.Fatal("lost observed exit", err, result.ExitCode)
		}
		if status != 200 {
			if result.Status != "unavailable" || result.OutputComplete {
				t.Fatal("invented output")
			}
			continue
		}
		if result.Status != "captured" || !result.OutputComplete || !result.OutputTruncated || !strings.HasPrefix(result.OutputHead, "first line\n") || !strings.HasSuffix(result.OutputTail, "last traceback\n") {
			t.Fatal("lost head/tail")
		}
	}
}

func TestCaptureResultRetainsPartialOutputWhenLogReadFails(t *testing.T) {
	b, _ := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	pod := ownedPod(job, "pod-1")
	pod.Status = terminatedAgent(7, "Error", "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/work/jobs/agentos-run-1":
			_ = json.NewEncoder(w).Encode(job)
		case "/api/v1/namespaces/work/pods":
			_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{*pod}})
		case "/api/v1/namespaces/work/pods/pod-1/log":
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, "partial output\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := clientset.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	b, err = New(client, "work", "gvisor")
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.CaptureResult(context.Background(), "run-1")
	if err == nil || result.Status != "captured" || result.OutputHead != "partial output\n" || result.ExitCode == nil || *result.ExitCode != 7 || result.OutputComplete || !result.OutputTruncated {
		t.Fatalf("stream failure lost partial result or became final: %+v %v", result, err)
	}
}

func TestInventoryNeverAdoptsAndCleanupMatchesDurableSpecification(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	unknown := job.DeepCopy()
	unknown.Name = "unknown"
	unknown.Labels = map[string]string{ManagedByLabel: "agent-os"}
	if _, err := client.BatchV1().Jobs("work").Create(ctx, unknown, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	entries, err := b.ListExecutions(ctx)
	if err != nil || len(entries) != 2 {
		t.Fatal(entries, err)
	}
	if entries[0].ID != "run-1" || entries[1].ID != "" {
		t.Fatal("malformed inventory adopted identity", entries)
	}
	changed := fixtureRun()
	changed.TenantID = "other-tenant"
	client.ClearActions()
	if err := b.Cleanup(ctx, changed); err == nil {
		t.Fatal("cleanup accepted wrong tenant fingerprint")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("mismatch performed destructive action")
		}
	}
	if err := b.Cleanup(ctx, fixtureRun()); err != nil {
		t.Fatal(err)
	}
	jobs, err := client.BatchV1().Jobs("work").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 1 || jobs.Items[0].Name != "unknown" {
		t.Fatal("cleanup deleted unknown Job", err)
	}
}
