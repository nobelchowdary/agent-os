package kubernetes

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
)

func terminatedAgent(exit int32, reason, message string) corev1.PodStatus {
	return corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: exit, Reason: reason, Message: message},
	}}}}
}

func waitingAgent(reason, message string) corev1.PodStatus {
	return corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message},
	}}}}
}

func TestObserveClassifiesOnlyStructuredOwnedEvidence(t *testing.T) {
	complete := []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	failed := []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "untrusted OOMKilled DeadlineExceeded policy_denied"}}
	cases := []struct {
		name       string
		conditions []batchv1.JobCondition
		pods       []corev1.PodStatus
		state      model.State
		code       model.ReasonCode
	}{
		{"completed Job wins over stale Pod failure", complete, []corev1.PodStatus{terminatedAgent(137, "OOMKilled", "")}, model.Succeeded, model.ReasonCompleted},
		{"failed Job without cause", failed, nil, model.Failed, model.ReasonUnknown},
		{"Job deadline overrides Pod image wait", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}, []corev1.PodStatus{waitingAgent("ImagePullBackOff", "")}, model.Failed, model.ReasonDeadlineExceeded},
		{"deadline failure target needs cleanup", []batchv1.JobCondition{{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}, nil, model.Cancelling, model.ReasonDeadlineExceeded},
		{"other failure target cause unknown", []batchv1.JobCondition{{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}, nil, model.Cancelling, model.ReasonUnknown},
		{"OOM terminal Job", failed, []corev1.PodStatus{terminatedAgent(137, "OOMKilled", "")}, model.Failed, model.ReasonOOMKilled},
		{"OOM Pod before terminal Job needs cleanup", nil, []corev1.PodStatus{terminatedAgent(137, "OOMKilled", "")}, model.Cancelling, model.ReasonOOMKilled},
		{"nonzero task exit", failed, []corev1.PodStatus{terminatedAgent(1, "Error", "")}, model.Failed, model.ReasonTaskError},
		{"exit137 alone is not OOM evidence", failed, []corev1.PodStatus{terminatedAgent(137, "Error", "OOMKilled")}, model.Failed, model.ReasonTaskError},
		{"generic Pod eviction has unknown attribution", failed, []corev1.PodStatus{{Phase: corev1.PodFailed, Reason: "Evicted", Message: "secret arbitrary detail"}}, model.Failed, model.ReasonUnknown},
		{"generic eviction before Job terminal needs cleanup", nil, []corev1.PodStatus{{Phase: corev1.PodFailed, Reason: "Evicted"}}, model.Cancelling, model.ReasonUnknown},
		{"invalid image requires cleanup", nil, []corev1.PodStatus{waitingAgent("InvalidImageName", "secret registry credential")}, model.Cancelling, model.ReasonImagePullFailed},
		{"transient image pull can recover", nil, []corev1.PodStatus{waitingAgent("ErrImagePull", "")}, model.Starting, model.ReasonImagePullFailed},
		{"image backoff can recover", nil, []corev1.PodStatus{waitingAgent("ImagePullBackOff", "")}, model.Starting, model.ReasonImagePullFailed},
		{"terminal Job does not infer cause from transient image wait", failed, []corev1.PodStatus{waitingAgent("ImagePullBackOff", "")}, model.Failed, model.ReasonUnknown},
		{"waiting message cannot impersonate reason", nil, []corev1.PodStatus{waitingAgent("ContainerCreating", "InvalidImageName OOMKilled")}, model.Starting, ""},
		{"Pod message cannot impersonate eviction", failed, []corev1.PodStatus{{Phase: corev1.PodFailed, Message: "Evicted"}}, model.Failed, model.ReasonUnknown},
		{"zero exit cannot impersonate OOM", failed, []corev1.PodStatus{terminatedAgent(0, "OOMKilled", "")}, model.Failed, model.ReasonUnknown},
		{"conflicting owned Pod causes remain unknown", failed, []corev1.PodStatus{terminatedAgent(137, "OOMKilled", ""), {Phase: corev1.PodFailed, Reason: "Evicted"}}, model.Failed, model.ReasonUnknown},
		{"false Job condition is not a terminal fact", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionFalse, Reason: "DeadlineExceeded"}}, nil, model.Starting, ""},
		{"stale terminated state is ignored", nil, []corev1.PodStatus{{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}}}}}, model.Running, ""},
		{"another container cannot classify agent cause", nil, []corev1.PodStatus{{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "other", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}}}}}, model.Starting, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, client := newTestBackend(t)
			job, _ := b.Render(fixtureRun())
			job.UID = "job-uid"
			active := false
			job.Spec.Suspend = &active
			job.Status = batchv1.JobStatus{Active: 1, Conditions: tc.conditions}
			if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			for i, status := range tc.pods {
				pod := ownedPod(job, string(rune('a'+i)))
				pod.Status = status
				if _, err := client.CoreV1().Pods("work").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			ob, err := b.Observe(context.Background(), "run-1")
			if err != nil || ob.State != tc.state || ob.ReasonCode != tc.code {
				t.Fatalf("got %+v, %v; want %s/%s", ob, err, tc.state, tc.code)
			}
			if strings.Contains(ob.Reason, "secret") || strings.Contains(ob.Reason, "untrusted") {
				t.Fatal("untrusted status message escaped into outcome reason")
			}
		})
	}
}

func TestObserveTerminalJobRemainsAuthoritativeWithoutPodEvidence(t *testing.T) {
	for _, complete := range []bool{false, true} {
		b, client := newTestBackend(t)
		job, _ := b.Render(fixtureRun())
		condition := batchv1.JobFailed
		state, code := model.Failed, model.ReasonUnknown
		if complete {
			condition, state, code = batchv1.JobComplete, model.Succeeded, model.ReasonCompleted
		}
		job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
		now := metav1.Now()
		job.DeletionTimestamp = &now
		if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("Pod API unavailable with sensitive detail")
		})
		ob, err := b.Observe(context.Background(), "run-1")
		if err != nil || ob.State != state || ob.ReasonCode != code {
			t.Fatalf("terminal fact lost: %+v, %v", ob, err)
		}
	}
}

func TestObserveRefusesForeignPodFailureEvidence(t *testing.T) {
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := ownedPod(job, "foreign")
	pod.OwnerReferences[0].UID = "another-job-uid"
	pod.Status = terminatedAgent(137, "OOMKilled", "")
	if _, err := client.CoreV1().Pods("work").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	ob, err := b.Observe(context.Background(), "run-1")
	if err != nil || ob.State != model.Failed || ob.ReasonCode != model.ReasonUnknown {
		t.Fatalf("foreign Pod supplied an outcome cause: %+v, %v", ob, err)
	}
	job.Status.Conditions = nil
	if _, err := client.BatchV1().Jobs("work").UpdateStatus(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Observe(context.Background(), "run-1"); err == nil {
		t.Fatal("nonterminal foreign Pod evidence was accepted")
	}
}
