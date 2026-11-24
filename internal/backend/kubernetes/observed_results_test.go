package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

func TestObservedCaptureMakesNoJobPodOrRuntimeRefetch(t *testing.T) {
	b, _ := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	pod := ownedPod(job, "pod-1")
	pod.Status = terminatedAgent(3, "Error", "untrusted termination message")
	rc := &nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc", Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")}}}
	pod.Spec.Overhead = rc.Overhead.PodFixed.DeepCopy()
	var mu sync.Mutex
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/work/jobs/agentos-run-1":
			_ = json.NewEncoder(w).Encode(job)
		case "/api/v1/namespaces/work/pods":
			_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{*pod}})
		case "/apis/node.k8s.io/v1/runtimeclasses/gvisor":
			_ = json.NewEncoder(w).Encode(rc)
		case "/api/v1/namespaces/work/pods/pod-1/log":
			if r.URL.Query().Get("limitBytes") != "16777217" || r.URL.Query().Get("container") != "agent" || r.URL.Query().Get("follow") == "true" {
				t.Error("unbounded/following observed capture")
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, "untrusted task output\n")
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
	ob, err := b.Observe(context.Background(), "run-1")
	if err != nil || ob.State != model.Failed || ob.Capture == nil {
		t.Fatal("terminal observation missing receipt", err)
	}
	for i := 0; i < 2; i++ {
		result, err := b.CaptureObservedResult(context.Background(), "run-1", ob)
		if err != nil || result.ExitCode == nil || *result.ExitCode != 3 || result.OutputHead != "untrusted task output\n" || !result.OutputComplete {
			t.Fatal("observed capture lost result", result, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := map[string]int{
		"/apis/batch/v1/namespaces/work/jobs/agentos-run-1": 1,
		"/api/v1/namespaces/work/pods":                      1,
		"/apis/node.k8s.io/v1/runtimeclasses/gvisor":        1,
		"/api/v1/namespaces/work/pods/pod-1/log":            2,
	}
	if !reflect.DeepEqual(requests, want) {
		t.Fatalf("observed capture repeated validation requests: got %v, want %v", requests, want)
	}
}

func TestObservedCaptureFreezesDeepCopiedPodEvidence(t *testing.T) {
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "original-job"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	pod := ownedPod(job, "original-pod")
	pod.Status = terminatedAgent(7, "Error", "")
	pods := &corev1.PodList{Items: []corev1.Pod{*pod}}
	// Deliberately return shared objects, rather than the fake tracker's usual
	// copies, to detect aliases into the evidence used for capture.
	client.PrependReactor("get", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) { return true, job, nil })
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { return true, pods, nil })
	ob, err := b.Observe(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "replacement-job"
	pods.Items[0].Name = "replacement-pod"
	pods.Items[0].Labels[RunIDLabel] = "other-run"
	pods.Items[0].OwnerReferences[0].UID = job.UID
	pods.Items[0].Spec.Containers[0].Image = "unapproved"
	pods.Items[0].Status.ContainerStatuses[0].State.Terminated.ExitCode = 99
	client.ClearActions()
	result, err := b.CaptureObservedResult(context.Background(), "run-1", ob)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 7 || !result.OutputComplete {
		t.Fatal("source mutation changed frozen evidence", result, err)
	}
	receipt := ob.Capture.(*resultReceipt)
	if receipt.jobUID != "original-job" || receipt.pods[0].Name != "original-pod" || receipt.pods[0].Spec.Containers[0].Image == "unapproved" {
		t.Fatal("receipt retained mutable source aliases")
	}
	actions := client.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != "get" || actions[0].GetResource().Resource != "pods" || actions[0].GetSubresource() != "log" {
		t.Fatal("capture refetched mutable evidence", actions)
	}
	encoded, err := json.Marshal(ob)
	if err != nil || strings.Contains(string(encoded), "Capture") || strings.Contains(string(encoded), "original-job") || strings.Contains(string(encoded), "original-pod") {
		t.Fatal("opaque evidence escaped JSON payload", err)
	}
}

func TestObservedCaptureRejectsForeignOrChangedReceiptsWithoutRequests(t *testing.T) {
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("work").Create(context.Background(), ownedPod(job, "pod-1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ob, err := b.Observe(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	foreign, foreignClient := newTestBackend(t)
	for _, tc := range []struct {
		name   string
		runID  string
		b      *Backend
		mutate func(*backend.Observation)
	}{
		{"cross run", "run-2", b, func(*backend.Observation) {}},
		{"foreign backend", "run-1", foreign, func(*backend.Observation) {}},
		{"missing receipt", "run-1", b, func(o *backend.Observation) { o.Capture = nil }},
		{"arbitrary token", "run-1", b, func(o *backend.Observation) { o.Capture = struct{}{} }},
		{"typed nil", "run-1", b, func(o *backend.Observation) { o.Capture = (*resultReceipt)(nil) }},
		{"forged empty receipt", "run-1", b, func(o *backend.Observation) { o.Capture = &resultReceipt{} }},
		{"zero UID", "run-1", b, func(o *backend.Observation) {
			receipt := *o.Capture.(*resultReceipt)
			receipt.jobUID = ""
			o.Capture = &receipt
		}},
		{"different Job UID", "run-1", b, func(o *backend.Observation) {
			receipt := *o.Capture.(*resultReceipt)
			receipt.jobUID = "replacement-job"
			o.Capture = &receipt
		}},
		{"changed state", "run-1", b, func(o *backend.Observation) { o.State = model.Failed }},
		{"nonterminal state", "run-1", b, func(o *backend.Observation) { o.State = model.Running }},
		{"changed reason", "run-1", b, func(o *backend.Observation) { o.Reason = "changed" }},
		{"changed code", "run-1", b, func(o *backend.Observation) { o.ReasonCode = model.ReasonUnknown }},
		{"changed timestamp", "run-1", b, func(o *backend.Observation) { o.FinishedAt = time.Now() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := ob
			tc.mutate(&changed)
			client.ClearActions()
			foreignClient.ClearActions()
			result, err := tc.b.CaptureObservedResult(context.Background(), tc.runID, changed)
			if err == nil || result.Status != "unavailable" || result.ExitCode != nil || result.OutputHead != "" || len(client.Actions()) != 0 || len(foreignClient.Actions()) != 0 {
				t.Fatal("invalid receipt read or adopted execution evidence", result, err)
			}
		})
	}
}

func TestObservedCapturePreservesTerminalPodLookupErrorAndFinishClock(t *testing.T) {
	for _, condition := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(string(condition), func(t *testing.T) {
			b, client := newTestBackend(t)
			job, _ := b.Render(fixtureRun())
			job.UID = "job-uid"
			finished := metav1.NewTime(time.Now().Add(-time.Hour).UTC())
			job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, LastTransitionTime: finished}}
			if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			lookupErr := errors.New("Pod lookup unavailable")
			client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, lookupErr })
			ob, err := b.Observe(context.Background(), "run-1")
			if err != nil || !ob.State.Terminal() || !ob.FinishedAt.Equal(finished.Time) {
				t.Fatal("optional lookup erased terminal Job evidence", err)
			}
			client.ClearActions()
			result, err := b.CaptureObservedResult(context.Background(), "run-1", ob)
			if !errors.Is(err, lookupErr) || result.Status != "unavailable" || len(client.Actions()) != 0 {
				t.Fatal("capture lost original Pod error or repeated lookup", result, err)
			}
		})
	}
}
