package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

func batchFixture(t *testing.T, b *Backend, id string) (*batchv1.Job, *corev1.Pod) {
	t.Helper()
	run := fixtureRun()
	run.ID = id
	job, err := b.Render(run)
	if err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID("job-" + id)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	pod := ownedPod(job, "pod-"+id)
	pod.Labels = ownedLabels(id)
	pod.Status = terminatedAgent(0, "Completed", "")
	return job, pod
}

func TestObservationBatch64UsesOneCollectionReadEachAndOnlyLogsForCapture(t *testing.T) {
	b, _ := newTestBackend(t)
	jobs, pods := batchv1.JobList{}, corev1.PodList{}
	ids := []string{}
	for i := range 64 {
		id := fmt.Sprintf("run-%d", i)
		job, pod := batchFixture(t, b, id)
		jobs.Items = append(jobs.Items, *job)
		pods.Items = append(pods.Items, *pod)
		ids = append(ids, id)
	}
	var mu sync.Mutex
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.URL.Path]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/work/jobs", "/api/v1/namespaces/work/pods":
			selector := r.URL.Query().Get("labelSelector")
			if !strings.Contains(selector, RunIDLabel+" in (") || strings.Contains(selector, ManagedByLabel) || r.URL.Query().Get("limit") != "100" {
				t.Error("missing bounded cohort selector", selector)
			}
			if strings.Contains(r.URL.Path, "/jobs") {
				_ = json.NewEncoder(w).Encode(jobs)
			} else {
				_ = json.NewEncoder(w).Encode(pods)
			}
		default:
			if strings.HasSuffix(r.URL.Path, "/log") {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = io.WriteString(w, "done\n")
			} else {
				t.Error("unexpected extra API read", r.URL.Path)
				http.NotFound(w, r)
			}
		}
	}))
	defer server.Close()
	client, err := clientset.NewForConfig(&rest.Config{Host: server.URL, QPS: 1000, Burst: 2000})
	if err != nil {
		t.Fatal(err)
	}
	b, err = New(client, "work", "gvisor")
	if err != nil {
		t.Fatal(err)
	}
	view, err := b.PrepareObservations(context.Background(), ids)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ob, err := view.Observe(context.Background(), id)
			if err != nil || ob.State != model.Succeeded || ob.Capture == nil {
				t.Error("missing terminal receipt", id, err)
				return
			}
			result, err := b.CaptureObservedResult(context.Background(), id, ob)
			if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || result.OutputHead != "done\n" || !result.OutputComplete {
				t.Error("capture lost evidence", id, err)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 66 || requests["/apis/batch/v1/namespaces/work/jobs"] != 1 || requests["/api/v1/namespaces/work/pods"] != 1 {
		t.Fatal("batch repeated collection reads", requests)
	}
	for _, id := range ids {
		if requests["/api/v1/namespaces/work/pods/pod-"+id+"/log"] != 1 {
			t.Fatal("missing unique bounded log capture")
		}
	}
}

func TestObservationBatchPaginationFailureNeverUsesPartialPodEvidence(t *testing.T) {
	for _, resource := range []string{"jobs", "pods"} {
		for _, failure := range []string{"expired", "transport", "loop", "page_bound", "item_bound"} {
			t.Run(resource+"/"+failure, func(t *testing.T) {
				b, client := newTestBackend(t)
				job, pod := batchFixture(t, b, "run-1")
				if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				if _, err := client.CoreV1().Pods("work").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				calls := 0
				client.PrependReactor("list", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
					calls++
					if calls > 1 && failure == "expired" {
						return true, nil, apierrors.NewResourceExpired("expired")
					}
					if calls > 1 && failure == "transport" {
						return true, nil, errors.New("unavailable")
					}
					next := "next"
					if failure == "page_bound" {
						next = fmt.Sprintf("page-%d", calls)
					}
					if resource == "jobs" {
						items := []batchv1.Job{*job}
						if calls > 1 {
							items = nil
						}
						if failure == "item_bound" {
							items = make([]batchv1.Job, 257)
							for i := range items {
								items[i] = *job.DeepCopy()
								items[i].Name = fmt.Sprintf("job-%d", i)
							}
							next = ""
						}
						return true, &batchv1.JobList{ListMeta: metav1.ListMeta{Continue: next}, Items: items}, nil
					}
					items := []corev1.Pod{*pod}
					if calls > 1 {
						items = nil
					}
					if failure == "item_bound" {
						items = make([]corev1.Pod, 257)
						for i := range items {
							items[i] = *pod.DeepCopy()
						}
						next = ""
					}
					return true, &corev1.PodList{ListMeta: metav1.ListMeta{Continue: next}, Items: items}, nil
				})
				view, err := b.PrepareObservations(context.Background(), []string{"run-1"})
				if resource == "jobs" {
					if err == nil || view != nil {
						t.Fatal("partial Job list produced view")
					}
					return
				}
				if err != nil || view == nil {
					t.Fatal("optional Pod read suppressed terminal Job", err)
				}
				ob, err := view.Observe(context.Background(), "run-1")
				if err != nil || ob.State != model.Succeeded || ob.Capture == nil {
					t.Fatal("authoritative terminal outcome lost", err)
				}
				result, err := b.CaptureObservedResult(context.Background(), "run-1", ob)
				if err == nil || result.Status != "unavailable" || result.ExitCode != nil {
					t.Fatal("partial Pod list leaked capture evidence", result, err)
				}
				typed := view.(*observationView)
				if typed.pods != nil || typed.podErr == nil {
					t.Fatal("partial Pods retained")
				}
				if calls > 4 {
					t.Fatal("unbounded page fetch", calls)
				}
			})
		}
	}
}

func TestObservationBatchViewsRemainIndependentAndValidateEveryExecution(t *testing.T) {
	for _, mutation := range []string{"none", "image", "foreign_uid", "foreign_managed", "wrong_job_label", "wrong_job_hash", "contradictory_job"} {
		t.Run(mutation, func(t *testing.T) {
			b, client := newTestBackend(t)
			job, pod := batchFixture(t, b, "run-1")
			switch mutation {
			case "image":
				pod.Spec.Containers[0].Image = "unapproved"
			case "foreign_uid":
				pod.OwnerReferences[0].UID = "foreign"
			case "foreign_managed":
				delete(pod.Labels, ManagedByLabel)
			case "wrong_job_label":
				job.Labels[RunIDLabel] = "other"
			case "wrong_job_hash":
				delete(job.Annotations, SpecHashAnnotation)
			case "contradictory_job":
				job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue})
			}
			jobs := &batchv1.JobList{Items: []batchv1.Job{*job}}
			pods := &corev1.PodList{Items: []corev1.Pod{*pod}}
			client.PrependReactor("list", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) { return true, jobs, nil })
			client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) { return true, pods, nil })
			ids := []string{"run-1"}
			view, err := b.PrepareObservations(context.Background(), ids)
			if err != nil {
				t.Fatal(err)
			}
			ids[0] = "changed"
			jobs.Items[0].Status.Conditions = nil
			pods.Items[0].Spec.Containers[0].Image = "changed-after-snapshot"
			second, err := b.PrepareObservations(context.Background(), []string{"run-1"})
			if err != nil {
				t.Fatal(err)
			}
			ob, err := view.Observe(context.Background(), "run-1")
			switch mutation {
			case "none":
				if err != nil || ob.State != model.Succeeded || ob.Capture == nil {
					t.Fatal("source or other view changed evidence", err)
				}
				result, err := b.CaptureObservedResult(context.Background(), "run-1", ob)
				if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
					t.Fatal(result, err)
				}
			case "image":
				if err != nil || ob.State != model.Cancelling || ob.ReasonCode != model.ReasonInfraFailure || ob.Capture != nil {
					t.Fatal("mutation accepted", ob, err)
				}
			case "foreign_uid", "foreign_managed":
				if err != nil || ob.State != model.Succeeded {
					t.Fatal("terminal precedence changed", err)
				}
				if _, err := b.CaptureObservedResult(context.Background(), "run-1", ob); err == nil {
					t.Fatal("foreign Pod used for capture")
				}
			default:
				if err == nil {
					t.Fatal("invalid Job accepted", ob)
				}
			}
			if mutation == "none" {
				next, err := second.Observe(context.Background(), "run-1")
				if err != nil || next.State != model.Cancelling {
					t.Fatal("fresh view did not see later mutation", next, err)
				}
			}
			if _, err := view.Observe(context.Background(), "outside"); err == nil {
				t.Fatal("foreign ID accepted")
			}
		})
	}
}

func TestObservationBatchMissingJobUsesFreshOwnershipRead(t *testing.T) {
	b, client := newTestBackend(t)
	job, pod := batchFixture(t, b, "run-1")
	view, err := b.PrepareObservations(context.Background(), []string{"run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("work").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ob, err := view.Observe(context.Background(), "run-1")
	if err != nil || ob.State != model.Succeeded {
		t.Fatal("list miss manufactured disappearance", ob, err)
	}
	if err := client.BatchV1().Jobs("work").Delete(context.Background(), job.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := view.Observe(context.Background(), "run-1"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatal("missing Job lost not-found contract", err)
	}
}

func TestObservationBatchSuccessfulPaginationAndInputBounds(t *testing.T) {
	b, client := newTestBackend(t)
	job, pod := batchFixture(t, b, "run-1")
	counts := map[string]int{}
	for _, resource := range []string{"jobs", "pods"} {
		client.PrependReactor("list", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
			key := action.GetResource().Resource
			counts[key]++
			next := ""
			if counts[key] == 1 {
				next = "page-2"
			}
			if key == "jobs" {
				list := &batchv1.JobList{ListMeta: metav1.ListMeta{Continue: next}}
				if counts[key] == 1 {
					list.Items = []batchv1.Job{*job}
				}
				return true, list, nil
			}
			list := &corev1.PodList{ListMeta: metav1.ListMeta{Continue: next}}
			if counts[key] == 2 {
				list.Items = []corev1.Pod{*pod}
			}
			return true, list, nil
		})
	}
	view, err := b.PrepareObservations(context.Background(), []string{"run-1"})
	if err != nil {
		t.Fatal(err)
	}
	ob, err := view.Observe(context.Background(), "run-1")
	if err != nil || ob.State != model.Succeeded {
		t.Fatal(ob, err)
	}
	if !reflect.DeepEqual(counts, map[string]int{"jobs": 2, "pods": 2}) {
		t.Fatal(counts)
	}
	for _, ids := range [][]string{nil, {"run-1", "run-1"}, {"invalid/id"}, make([]string, 65)} {
		if v, err := b.PrepareObservations(context.Background(), ids); err == nil || v != nil {
			t.Fatal("invalid cohort accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := view.Observe(ctx, "run-1"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read ignored", err)
	}
}

func TestObservationBatchNeverSuppliesMutationAuthority(t *testing.T) {
	b, client := newTestBackend(t)
	job, pod := batchFixture(t, b, "run-1")
	ctx := context.Background()
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	view, err := b.PrepareObservations(ctx, []string{"run-1"})
	if err != nil {
		t.Fatal(err)
	}
	// A replacement appears after preparation. Cleanup must fence the actual
	// current Job UID, never a stale UID held by the observation view.
	job.UID = "replacement-job"
	pod.OwnerReferences[0].UID = job.UID
	if _, err := client.BatchV1().Jobs("work").Update(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods("work").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deleted := false
	client.PrependReactor("delete", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		opts := action.(clienttesting.DeleteAction).GetDeleteOptions()
		if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "replacement-job" {
			t.Fatal("cleanup used stale observation UID")
		}
		deleted = true
		return false, nil, nil
	})
	err = b.Cleanup(ctx, fixtureRun())
	if !errors.Is(err, backend.ErrPending) || !deleted {
		t.Fatal("fresh cleanup contract changed", err)
	}
	// The same read view is immutable even after cleanup operations.
	ob, err := view.Observe(ctx, "run-1")
	if err != nil || ob.State != model.Succeeded {
		t.Fatal("cleanup mutated read snapshot", err)
	}
	receipt := ob.Capture.(*resultReceipt)
	if receipt.jobUID == "replacement-job" {
		t.Fatal("view changed through mutation path")
	}
}

func TestObservationBatchPodReadFailureCannotFinishActiveJob(t *testing.T) {
	b, client := newTestBackend(t)
	job, _ := batchFixture(t, b, "run-1")
	job.Status.Conditions = nil
	if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewServiceUnavailable("unavailable")
	})
	view, err := b.PrepareObservations(context.Background(), []string{"run-1"})
	if err != nil {
		t.Fatal(err)
	}
	ob, err := view.Observe(context.Background(), "run-1")
	if err == nil || ob.Capture != nil || ob.State.Terminal() {
		t.Fatal("optional Pod failure manufactured an active outcome", ob, err)
	}
}
