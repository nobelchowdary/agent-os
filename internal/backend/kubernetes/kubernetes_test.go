package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
)

func fixtureRun() model.Record {
	return model.Record{ID: "run-1", TenantID: "tenant-1", Spec: model.Spec{
		Template: "python", Image: "python:3.13-slim", RuntimeClass: "gvisor", Command: []string{"python", "-c", "print('hello')"},
		Resources: model.Resources{CPUMilli: 500, MemoryMiB: 128, DiskMiB: 64}, TimeoutSeconds: 90,
	}}
}

func newTestBackend(t *testing.T, objects ...runtime.Object) (*Backend, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset(objects...)
	// The fake client does not allocate the server identity/version needed for
	// fenced activation. Simulate only that API-server metadata here.
	client.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		job := action.(clienttesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		if job.UID == "" {
			job.UID = "job-uid"
		}
		if job.ResourceVersion == "" {
			job.ResourceVersion = "1"
		}
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "work"); err != nil {
			return true, nil, err
		}
		return true, job, nil
	})
	b, err := New(client, "work", "gvisor")
	if err != nil {
		t.Fatal(err)
	}
	return b, client
}

func fixtures() []runtime.Object {
	return []runtime.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "work"}},
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc"},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: DenyAllPolicy, Namespace: "work"}, Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{PartOfLabel: "agent-os"}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
		}},
	}
}

func TestConstructionRequiresExplicitSandboxConfiguration(t *testing.T) {
	c := fake.NewSimpleClientset()
	for _, name := range []string{"", "default", "runc", "crun", "io.containerd.runc.v2", "INVALID"} {
		if _, err := New(c, "work", name); err == nil {
			t.Errorf("accepted runtime %q", name)
		}
	}
	if _, err := New(c, "", "gvisor"); err == nil {
		t.Error("accepted empty namespace")
	}
}

func TestOfflineBackendOnlyRenders(t *testing.T) {
	var typedNil *fake.Clientset
	for _, client := range []clientset.Interface{nil, typedNil} {
		b, err := New(client, "work", "gvisor")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Render(fixtureRun()); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := b.Ready(ctx); err == nil {
			t.Error("offline Ready succeeded")
		}
		if err := b.Submit(ctx, fixtureRun()); err == nil {
			t.Error("offline Submit succeeded")
		}
		if err := b.Activate(ctx, fixtureRun()); err == nil {
			t.Error("offline Activate succeeded")
		}
		if _, err := b.Observe(ctx, "run-1"); err == nil {
			t.Error("offline Observe succeeded")
		}
		if err := b.Cancel(ctx, "run-1"); err == nil {
			t.Error("offline Cancel succeeded")
		}
		if _, err := b.Logs(ctx, "run-1", 100); err == nil {
			t.Error("offline Logs succeeded")
		}
	}
}

func TestReadyRequiresRuntimeAndDefaultDenyInstallation(t *testing.T) {
	ctx := context.Background()
	b, _ := newTestBackend(t, fixtures()...)
	if err := b.Ready(ctx); err != nil {
		t.Fatalf("qualified fixtures: %v", err)
	}
	for _, scenario := range []string{"namespace-missing", "runtime-missing", "ordinary-handler", "policy-missing", "wrong-selector", "run-specific-selector", "allow-rule", "missing-egress"} {
		t.Run(scenario, func(t *testing.T) {
			objects := fixtures()
			switch scenario {
			case "namespace-missing":
				objects = objects[1:]
			case "runtime-missing":
				objects = append(objects[:1], objects[2:]...)
			case "ordinary-handler":
				objects[1].(*nodev1.RuntimeClass).Handler = "runc"
			case "policy-missing":
				objects = objects[:2]
			case "wrong-selector":
				objects[2].(*networkingv1.NetworkPolicy).Spec.PodSelector.MatchLabels[PartOfLabel] = "elsewhere"
			case "run-specific-selector":
				objects[2].(*networkingv1.NetworkPolicy).Spec.PodSelector.MatchLabels[RunIDLabel] = "run-1"
			case "allow-rule":
				objects[2].(*networkingv1.NetworkPolicy).Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
			case "missing-egress":
				objects[2].(*networkingv1.NetworkPolicy).Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
			}
			b, _ := newTestBackend(t, objects...)
			if err := b.Ready(ctx); err == nil {
				t.Fatal("accepted missing or unsafe prerequisite")
			}
		})
	}
}

func TestRenderConstrainsWorkloadAndDoesNotContactCluster(t *testing.T) {
	b, client := newTestBackend(t)
	run := fixtureRun()
	job, err := b.Render(run)
	if err != nil {
		t.Fatal(err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("offline render contacted cluster")
	}
	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest["apiVersion"] != "batch/v1" || manifest["kind"] != "Job" {
		t.Fatalf("render is not a Kubernetes manifest: %s", encoded)
	}
	if job.Name != "agentos-run-1" || job.Namespace != "work" {
		t.Fatalf("unexpected identity: %+v", job.ObjectMeta)
	}
	if *job.Spec.BackoffLimit != 0 || *job.Spec.ActiveDeadlineSeconds != 90 || job.Spec.TTLSecondsAfterFinished != nil {
		t.Fatal("retry/deadline/retention policy is unsafe")
	}
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		t.Fatal("initial execution must be suspended")
	}
	p := job.Spec.Template.Spec
	if p.RuntimeClassName == nil || *p.RuntimeClassName != "gvisor" || p.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatal("runtime boundary missing")
	}
	if p.AutomountServiceAccountToken == nil || *p.AutomountServiceAccountToken || p.ServiceAccountName != WorkloadServiceAccount {
		t.Fatal("unexpected workload credentials")
	}
	if p.HostNetwork || p.HostPID || p.HostIPC || len(p.InitContainers) != 0 || len(p.Containers) != 1 {
		t.Fatal("unexpected host access or sidecars")
	}
	if *p.SecurityContext.RunAsUser != 10001 || !*p.SecurityContext.RunAsNonRoot || *p.SecurityContext.FSGroup != 10001 || p.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatal("pod security context missing")
	}
	c := p.Containers[0]
	if *c.SecurityContext.AllowPrivilegeEscalation || !*c.SecurityContext.ReadOnlyRootFilesystem || !reflect.DeepEqual(c.SecurityContext.Capabilities.Drop, []corev1.Capability{"ALL"}) {
		t.Fatal("container privilege controls missing")
	}
	if len(c.Env) != 0 || len(c.EnvFrom) != 0 || len(c.VolumeDevices) != 0 {
		t.Fatal("unexpected injected environment or devices")
	}
	if c.Resources.Limits.Cpu().MilliValue() != 500 || c.Resources.Limits.Memory().Value() != 128*1024*1024 || c.Resources.Limits.StorageEphemeral().Value() != 64*1024*1024 {
		t.Fatalf("wrong resources: %+v", c.Resources)
	}
	if !reflect.DeepEqual(c.Resources.Requests, c.Resources.Limits) {
		t.Fatal("requests do not match limits")
	}
	if len(p.Volumes) != 2 || len(c.VolumeMounts) != 2 {
		t.Fatal("unexpected storage")
	}
	for _, v := range p.Volumes {
		if v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil || v.EmptyDir.SizeLimit.Sign() <= 0 || v.HostPath != nil || v.PersistentVolumeClaim != nil {
			t.Fatalf("unbounded/host storage: %+v", v)
		}
	}
	if job.Spec.Template.Labels[PartOfLabel] != "agent-os" {
		t.Fatal("default-deny selector does not match")
	}
	// The render owns a copy of the command, not the caller's backing array.
	run.Spec.Command[0] = "mutated"
	if c.Command[0] != "python" {
		t.Fatal("caller can mutate rendered command")
	}
}

func TestRenderRejectsUnboundedOrMismatchedRun(t *testing.T) {
	b, _ := newTestBackend(t)
	for _, mutate := range []func(*model.Record){
		func(r *model.Record) { r.ID = "../bad" },
		func(r *model.Record) { r.Spec.RuntimeClass = "runc" },
		func(r *model.Record) { r.Spec.Resources.CPUMilli = 0 },
		func(r *model.Record) { r.Spec.Resources.MemoryMiB = 0 },
		func(r *model.Record) { r.Spec.Resources.DiskMiB = 1 },
		func(r *model.Record) { r.Spec.TimeoutSeconds = 0 },
		func(r *model.Record) { r.Spec.Image = "" },
		func(r *model.Record) { r.Spec.Command = nil },
	} {
		run := fixtureRun()
		mutate(&run)
		if _, err := b.Render(run); err == nil {
			t.Fatalf("accepted invalid run: %+v", run)
		}
	}
}

func TestSubmitIsIdempotentAndRejectsConflictingSpecifications(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	run := fixtureRun()
	if err := b.Submit(ctx, run); err != nil {
		t.Fatalf("initial create: %v", err)
	}
	initial, _ := client.BatchV1().Jobs("work").Get(ctx, "agentos-run-1", metav1.GetOptions{})
	if initial.Spec.Suspend == nil || !*initial.Spec.Suspend {
		t.Fatal("initial create can execute without activation")
	}
	if err := b.Submit(ctx, run); err != nil {
		t.Fatalf("retry: %v", err)
	}
	staged, _ := client.BatchV1().Jobs("work").Get(ctx, "agentos-run-1", metav1.GetOptions{})
	if staged.Spec.Suspend == nil || !*staged.Spec.Suspend {
		t.Fatal("repeated Submit activated the execution")
	}
	if err := b.Activate(ctx, run); err != nil {
		t.Fatalf("explicit activation: %v", err)
	}
	jobs, _ := client.BatchV1().Jobs("work").List(ctx, metav1.ListOptions{})
	if len(jobs.Items) != 1 {
		t.Fatalf("got %d Jobs", len(jobs.Items))
	}
	if jobs.Items[0].Spec.Suspend == nil || *jobs.Items[0].Spec.Suspend {
		t.Fatal("second reconciliation did not activate")
	}
	if err := b.Activate(ctx, run); err != nil {
		t.Fatalf("active retry: %v", err)
	}
	patches := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			patches++
		}
	}
	if patches != 1 {
		t.Fatalf("expected one activation patch, got %d", patches)
	}
	for _, mutate := range []func(*model.Record){
		func(r *model.Record) { r.TenantID = "other-tenant" },
		func(r *model.Record) { r.Spec.Image = "other:image" },
		func(r *model.Record) { r.Spec.TimeoutSeconds++ },
	} {
		other := fixtureRun()
		mutate(&other)
		if err := b.Submit(ctx, other); err == nil {
			t.Fatal("adopted conflicting run")
		}
		if err := b.Activate(ctx, other); err == nil {
			t.Fatal("activated conflicting run")
		}
	}
	job := jobs.Items[0].DeepCopy()
	// A retained annotation cannot conceal changed execution/security fields.
	yes := true
	job.Spec.Template.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = &yes
	if _, err := client.BatchV1().Jobs("work").Update(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Submit(ctx, run); err == nil {
		t.Fatal("adopted drifted execution")
	}
}

func TestSubmitRechecksPrerequisitesAfterPositiveCacheExpires(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	now := time.Now()
	b.readyNow = func() time.Time { return now }
	if err := b.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	// The earlier declaration check must not authorize creation once its
	// bounded positive cache has expired.
	if err := client.NetworkingV1().NetworkPolicies("work").Delete(ctx, DenyAllPolicy, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	client.ClearActions()
	if err := b.Submit(ctx, fixtureRun()); err == nil {
		t.Fatal("submitted without default-deny prerequisite")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
			t.Fatal("created Job before checking current prerequisites")
		}
	}
}

func TestSubmitAcceptsAPIDefaultedExecution(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	// client-go's external API scheme has no API-server defaulting functions.
	// Exercise the normal Job/PodTemplate defaults explicitly instead of using
	// scheme.Default, which would silently do nothing here. Defaults verified
	// against kubernetes v1.37.2 pkg/apis/{batch,core}/v1/defaults.go.
	client.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		job := action.(clienttesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.UID = "server-generated-uid"
		job.ResourceVersion = "123"
		mode, replacement, no := batchv1.NonIndexedCompletion, batchv1.TerminatingOrFailed, false
		job.Spec.CompletionMode = &mode
		job.Spec.PodReplacementPolicy = &replacement
		job.Spec.ManualSelector = &no
		job.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"batch.kubernetes.io/controller-uid": string(job.UID)}}
		job.Spec.Template.Labels["batch.kubernetes.io/controller-uid"] = string(job.UID)
		job.Spec.Template.Labels["batch.kubernetes.io/job-name"] = job.Name
		p := &job.Spec.Template.Spec
		grace := int64(30)
		p.DeprecatedServiceAccount = p.ServiceAccountName
		p.DNSPolicy = corev1.DNSClusterFirst
		p.TerminationGracePeriodSeconds = &grace
		p.SchedulerName = corev1.DefaultSchedulerName
		p.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("0.5")
		p.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("134217728")
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "work"); err != nil {
			return true, nil, err
		}
		return true, job, nil
	})
	if err := b.Submit(ctx, fixtureRun()); err != nil {
		t.Fatalf("API-defaulted create was rejected: %v", err)
	}
	if err := b.Submit(ctx, fixtureRun()); err != nil {
		t.Fatalf("API-defaulted idempotent retry was rejected: %v", err)
	}
	if err := b.Activate(ctx, fixtureRun()); err != nil {
		t.Fatalf("API-defaulted activation was rejected: %v", err)
	}
}

func TestLateCreateAfterCancellationCannotExecute(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	var delayed *batchv1.Job
	client.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		delayed = action.(clienttesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		return true, nil, context.DeadlineExceeded
	})
	if err := b.Submit(ctx, fixtureRun()); err == nil {
		t.Fatal("expected ambiguous create timeout")
	}
	if err := b.Cancel(ctx, "run-1"); err != nil {
		t.Fatalf("cancel before delayed commit: %v", err)
	}
	// The API server commits the original request after cancellation has already
	// observed absence. No further Submit is issued for the cancelled record.
	delayed.UID, delayed.ResourceVersion = "delayed-uid", "1"
	if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), delayed, "work"); err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs("work").Get(ctx, delayed.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		t.Fatal("late creation can execute a cancelled run")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			t.Fatal("ambiguous creation triggered activation")
		}
	}
}

func TestActivationCannotRecreateConcurrentlyDeletedJob(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	if err := b.Submit(ctx, fixtureRun()); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("patch", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "work", "agentos-run-1"); err != nil {
			t.Error(err)
		}
		return false, nil, nil
	})
	if err := b.Activate(ctx, fixtureRun()); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("deleted execution must be reported missing: %v", err)
	}
	if _, err := client.BatchV1().Jobs("work").Get(ctx, "agentos-run-1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("activation recreated Job: %v", err)
	}
}

func TestActivateMissingJobNeverCreatesReplacement(t *testing.T) {
	b, client := newTestBackend(t, fixtures()...)
	if err := b.Activate(context.Background(), fixtureRun()); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("got %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("activation attempted to create an execution")
		}
	}
}

func TestAmbiguousActivationRetriesWithoutCreatingOrReactivating(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	if err := b.Submit(ctx, fixtureRun()); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	client.PrependReactor("patch", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) {
		obj, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), "work", "agentos-run-1")
		if err != nil {
			return true, nil, err
		}
		job := obj.(*batchv1.Job).DeepCopy()
		active := false
		job.Spec.Suspend = &active
		if err := client.Tracker().Update(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "work"); err != nil {
			return true, nil, err
		}
		return true, nil, context.DeadlineExceeded
	})
	if err := b.Activate(ctx, fixtureRun()); err == nil {
		t.Fatal("expected ambiguous activation response")
	}
	if err := b.Activate(ctx, fixtureRun()); err != nil {
		t.Fatalf("activation retry: %v", err)
	}
	patches := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("activation retry created an execution")
		}
		if action.GetVerb() == "patch" {
			patches++
		}
	}
	if patches != 1 {
		t.Fatalf("expected one activation, got %d patches", patches)
	}
}

func TestActivateRechecksDefaultDenyAfterPositiveCacheExpires(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	now := time.Now()
	b.readyNow = func() time.Time { return now }
	if err := b.Submit(ctx, fixtureRun()); err != nil {
		t.Fatal(err)
	}
	if err := client.NetworkingV1().NetworkPolicies("work").Delete(ctx, DenyAllPolicy, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	client.ClearActions()
	if err := b.Activate(ctx, fixtureRun()); err == nil {
		t.Fatal("activated after prerequisite removal")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			t.Fatal("activation bypassed current prerequisite check")
		}
	}
}

func TestActivateDoesNotReactivateCompletedJob(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	job, _ := b.Render(fixtureRun())
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	if err := b.Activate(ctx, fixtureRun()); err != nil {
		t.Fatal(err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "patch" {
			t.Fatal("completed Job was reactivated")
		}
	}
}

func TestForeignJobsAreNeverObservedAdoptedDeletedOrRead(t *testing.T) {
	ctx := context.Background()
	foreign := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "agentos-run-1", Namespace: "work"}}
	b, client := newTestBackend(t, append(fixtures(), foreign)...)
	if err := b.Submit(ctx, fixtureRun()); err == nil {
		t.Error("adopted foreign job")
	}
	if err := b.Activate(ctx, fixtureRun()); err == nil {
		t.Error("activated foreign job")
	}
	if _, err := b.Observe(ctx, "run-1"); err == nil {
		t.Error("observed foreign job")
	}
	if err := b.Cancel(ctx, "run-1"); err == nil {
		t.Error("deleted foreign job")
	}
	if _, err := b.Logs(ctx, "run-1", 100); err == nil {
		t.Error("read foreign job logs")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatalf("destructive action on foreign job: %+v", action)
		}
	}
}

func TestObserveUsesTerminalConditionsAndBoundsReasons(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t)
	if _, err := b.Observe(ctx, "missing"); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	job, _ := b.Render(fixtureRun())
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := ownedPod(job, "observed-agent")
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
	if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		status    batchv1.JobStatus
		state     model.State
		suspended bool
	}{
		{"starting", batchv1.JobStatus{}, model.Starting, true},
		{"suspended-with-stale-active-count", batchv1.JobStatus{Active: 1}, model.Starting, true},
		{"active", batchv1.JobStatus{Active: 1}, model.Running, false},
		{"failed", batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: strings.Repeat("untrusted", 10000)}}}, model.Failed, false},
		{"complete", batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}, model.Succeeded, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job.Status = tc.status
			job.Spec.Suspend = &tc.suspended
			if _, err := client.BatchV1().Jobs("work").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			obs, err := b.Observe(ctx, "run-1")
			if err != nil || obs.State != tc.state || len(obs.Reason) > 100 {
				t.Fatalf("got %+v, %v", obs, err)
			}
		})
	}
}

func ownedPod(job *batchv1.Job, name string) *corev1.Pod {
	yes := true
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "work", UID: types.UID(name + "-uid"), Labels: ownedLabels("run-1"), Annotations: job.Spec.Template.DeepCopy().Annotations, OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: &yes}}}, Spec: *job.Spec.Template.Spec.DeepCopy()}
}

func TestCancelUsesForegroundAndWaitsForJobAndPods(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := ownedPod(job, "pod-1")
	if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deleteChecked := false
	client.PrependReactor("delete", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
		opts := action.(clienttesting.DeleteAction).GetDeleteOptions()
		if opts.PropagationPolicy == nil || *opts.PropagationPolicy != metav1.DeletePropagationForeground || opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != "job-uid" {
			t.Error("missing foreground deletion or UID precondition")
		}
		deleteChecked = true
		return false, nil, nil
	})
	if err := b.Cancel(ctx, "run-1"); !errors.Is(err, ErrCancelPending) {
		t.Fatalf("must retain reservation after observing Pod: %v", err)
	}
	if !deleteChecked {
		t.Fatal("job was not deleted")
	}
	if err := b.Cancel(ctx, "run-1"); err != nil {
		t.Fatalf("retry after disappearance: %v", err)
	}
	if err := b.Cancel(ctx, "run-1"); err != nil {
		t.Fatalf("repeated cancellation: %v", err)
	}
}

func TestCancelDoesNotSucceedWhileJobRemains(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("delete", "jobs", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, nil })
	if err := b.Cancel(ctx, "run-1"); !errors.Is(err, ErrCancelPending) {
		t.Fatalf("got %v", err)
	}
}

func TestForeignRunLabeledPodBlocksDestructiveCancellation(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	foreign := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "work", Labels: ownedLabels("run-1")}}
	if _, err := client.CoreV1().Pods("work").Create(ctx, foreign, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(ctx, "run-1"); err == nil {
		t.Fatal("accepted foreign Pod")
	}
	if _, err := client.BatchV1().Jobs("work").Get(ctx, job.Name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		t.Fatal("deleted before validating pod ownership")
	}
	if _, err := b.Logs(ctx, "run-1", 100); err == nil {
		t.Fatal("accepted foreign Pod logs")
	}
}

func TestLogsRequiresSoleOwnedPodAndValidLimit(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t)
	for _, limit := range []int64{0, -1, maxLogBytes + 1} {
		if _, err := b.Logs(ctx, "run-1", limit); err == nil {
			t.Errorf("accepted limit %d", limit)
		}
	}
	job, _ := b.Render(fixtureRun())
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Logs(ctx, "run-1", 100); !errors.Is(err, backend.ErrNotFound) {
		t.Fatalf("no pods: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := client.CoreV1().Pods("work").Create(ctx, ownedPod(job, fmt.Sprintf("pod-%d", i)), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Logs(ctx, "run-1", 100); err == nil {
		t.Fatal("accepted ambiguous Pods")
	}
}

func TestLogsEnforcesByteLimitEvenIfServerDoesNot(t *testing.T) {
	b, _ := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	pod := ownedPod(job, "pod-1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/work/jobs/agentos-run-1":
			_ = json.NewEncoder(w).Encode(job)
		case "/api/v1/namespaces/work/pods":
			_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{*pod}})
		case "/api/v1/namespaces/work/pods/pod-1/log":
			if r.URL.Query().Get("limitBytes") != "5" || r.URL.Query().Get("container") != "agent" {
				t.Errorf("unexpected log parameters: %s", r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "text/plain")
			_, _ = fmt.Fprint(w, "abcdefghijk")
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
	got, err := b.Logs(context.Background(), "run-1", 5)
	if err != nil || got != "abcde" {
		t.Fatalf("got %q, %v", got, err)
	}
}
