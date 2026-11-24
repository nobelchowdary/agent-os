package kubernetes

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func runtimeReads(actions []clienttesting.Action) int {
	count := 0
	for _, action := range actions {
		if action.GetVerb() == "get" && action.GetResource().Resource == "runtimeclasses" {
			count++
		}
	}
	return count
}

func TestRuntimeCacheCoalescesCopiesAndExpires(t *testing.T) {
	b, client := newTestBackend(t, fixtures()...)
	now := time.Now()
	b.readyNow = func() time.Time { return now }
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	client.PrependReactor("get", "runtimeclasses", func(clienttesting.Action) (bool, runtime.Object, error) {
		once.Do(func() { close(entered); <-release })
		return false, nil, nil
	})
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := b.runtimeDeclaration(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			value.Handler = "caller-mutation"
		}()
	}
	<-entered
	close(release)
	wg.Wait()
	if got := runtimeReads(client.Actions()); got != 1 {
		t.Fatal("RuntimeClass lookup was not coalesced", got)
	}
	value, err := b.runtimeDeclaration(context.Background())
	if err != nil || value.Handler != "runsc" {
		t.Fatal("caller mutated shared declaration", value, err)
	}
	if err := client.NodeV1().RuntimeClasses().Delete(context.Background(), "gvisor", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.runtimeDeclaration(context.Background()); err != nil {
		t.Fatal("positive entry did not retain declared TTL", err)
	}
	now = now.Add(3 * time.Second)
	for range 2 {
		if _, err := b.runtimeDeclaration(context.Background()); err == nil {
			t.Fatal("deleted declaration survived expiration")
		}
	}
	if got := runtimeReads(client.Actions()); got != 3 {
		t.Fatal("missing declaration failure cached", got)
	}
}

func TestRuntimeCacheSharesReadyAndActualPodValidation(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	rc, _ := client.NodeV1().RuntimeClasses().Get(ctx, "gvisor", metav1.GetOptions{})
	rc.Scheduling = &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox-node": "true"}}
	if _, err := client.NodeV1().RuntimeClasses().Update(ctx, rc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.readyNow = func() time.Time { return now }
	job, err := b.Render(fixtureRun())
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "job-uid"
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := ownedPod(job, "runtime-added-selector")
	pod.Spec.NodeSelector = map[string]string{"sandbox-node": "true"}
	if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if err := b.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	for range 64 {
		if _, err := b.executionPods(ctx, "run-1", job); err != nil {
			t.Fatal(err)
		}
	}
	if got := runtimeReads(client.Actions()); got != 1 {
		t.Fatal("Pod validation refetched Ready declaration", got)
	}
	rc.Handler = "runc"
	if _, err := client.NodeV1().RuntimeClasses().Update(ctx, rc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(3 * time.Second)
	if _, err := b.executionPods(ctx, "run-1", job); !errors.Is(err, errPodSpecChanged) {
		t.Fatal("expired handler mutation was not rejected", err)
	}
}

func TestRuntimeCacheWaiterDeadlineAndInvalidHandler(t *testing.T) {
	b, client := newTestBackend(t, fixtures()...)
	b.runtimeChecking = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := b.runtimeDeclaration(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiter ignored its deadline", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("waiting caller fetched another declaration")
	}
	b.runtimeChecking = nil
	rc, err := client.NodeV1().RuntimeClasses().Get(context.Background(), "gvisor", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rc.Handler = "runc"
	if _, err := client.NodeV1().RuntimeClasses().Update(context.Background(), rc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	for range 2 {
		value, err := b.runtimeDeclaration(context.Background())
		if err != nil || value.Handler != "runc" {
			t.Fatal("fresh invalid declaration hidden", err)
		}
	}
	if runtimeReads(client.Actions()) != 2 {
		t.Fatal("invalid handler declaration cached")
	}
}

func TestReadyDoesNotExtendRuntimeDeclarationStaleness(t *testing.T) {
	b, client := newTestBackend(t, fixtures()...)
	now := time.Now()
	b.readyNow = func() time.Time { return now }
	if _, err := b.runtimeDeclaration(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if err := b.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.NodeV1().RuntimeClasses().Delete(context.Background(), "gvisor", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := b.Ready(context.Background()); err == nil {
		t.Fatal("stacked caches extended stale RuntimeClass readiness beyond three seconds")
	}
}
