package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func observeFacts(t *testing.T, conditions []batchv1.JobCondition, pods ...corev1.Pod) (backend.Observation, error) {
	t.Helper()
	b, client := newTestBackend(t)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	suspended := false
	job.Spec.Suspend = &suspended
	// Active includes Pending Pods: the count must not establish process start.
	job.Status = batchv1.JobStatus{Active: 1, Conditions: conditions}
	if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for i, facts := range pods {
		pod := ownedPod(job, fmt.Sprintf("facts-%d", i))
		pod.Status, pod.DeletionTimestamp = facts.Status, facts.DeletionTimestamp
		if _, err := client.CoreV1().Pods("work").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	return b.Observe(context.Background(), "run-1")
}

func TestObserveOutcomePrecedenceIsIndependentOfConditionAndPodOrder(t *testing.T) {
	complete := batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
	failed := batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}
	deadline := failed
	deadline.Reason = "DeadlineExceeded"
	target := batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}
	oom := corev1.Pod{Status: terminatedAgent(137, "OOMKilled", "untrusted deadline_exceeded")}
	task := corev1.Pod{Status: terminatedAgent(1, "Error", "untrusted oom_killed")}
	for _, tc := range []struct {
		name       string
		conditions []batchv1.JobCondition
		pods       []corev1.Pod
		state      model.State
		code       model.ReasonCode
		invalid    bool
	}{
		{"contradictory terminal conditions", []batchv1.JobCondition{complete, failed}, []corev1.Pod{oom}, "", "", true},
		{"completion contradicts deadline", []batchv1.JobCondition{complete, deadline}, []corev1.Pod{oom}, "", "", true},
		{"completion contradicts failure target", []batchv1.JobCondition{complete, target}, []corev1.Pod{oom}, "", "", true},
		{"completion overrides stale Pod OOM", []batchv1.JobCondition{complete}, []corev1.Pod{oom, task}, model.Succeeded, model.ReasonCompleted, false},
		{"terminal deadline overrides Pod OOM", []batchv1.JobCondition{deadline}, []corev1.Pod{oom, task}, model.Failed, model.ReasonDeadlineExceeded, false},
		{"deadline target still requires cleanup", []batchv1.JobCondition{target}, []corev1.Pod{oom, task}, model.Cancelling, model.ReasonDeadlineExceeded, false},
		{"conflicting Pod facts remain unknown", []batchv1.JobCondition{failed}, []corev1.Pod{oom, task}, model.Failed, model.ReasonUnknown, false},
		{"false failure does not contradict success", []batchv1.JobCondition{complete, {Type: batchv1.JobFailed, Status: corev1.ConditionFalse}}, []corev1.Pod{oom}, model.Succeeded, model.ReasonCompleted, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, reverseConditions := range []bool{false, true} {
				for _, reversePods := range []bool{false, true} {
					conditions := append([]batchv1.JobCondition{}, tc.conditions...)
					pods := append([]corev1.Pod{}, tc.pods...)
					if reverseConditions {
						for i, j := 0, len(conditions)-1; i < j; i, j = i+1, j-1 {
							conditions[i], conditions[j] = conditions[j], conditions[i]
						}
					}
					if reversePods {
						for i, j := 0, len(pods)-1; i < j; i, j = i+1, j-1 {
							pods[i], pods[j] = pods[j], pods[i]
						}
					}
					ob, err := observeFacts(t, conditions, pods...)
					if tc.invalid {
						if err == nil || err.Error() != "execution has contradictory Job outcome conditions" || ob.State != "" {
							t.Fatalf("contradictory Job facts authorized an outcome: %+v, %v", ob, err)
						}
					} else if err != nil || ob.State != tc.state || ob.ReasonCode != tc.code {
						t.Fatalf("precedence changed with ordering: %+v, %v", ob, err)
					}
				}
			}
		})
	}
}

func TestObserveDisruptionAttributionUsesStructuredEvidence(t *testing.T) {
	failed := []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	for _, reason := range []string{corev1.PodReasonTerminationByKubelet, corev1.PodReasonPreemptionByScheduler,
		"DeletionByTaintManager", "EvictionByEvictionAPI", "DeletionByPodGC"} {
		t.Run(reason, func(t *testing.T) {
			pod := corev1.Pod{Status: terminatedAgent(143, "Error", "task error text")}
			pod.Status.Reason = "Evicted"
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: reason}}
			for _, terminal := range []bool{false, true} {
				var conditions []batchv1.JobCondition
				want := model.Cancelling
				if terminal {
					conditions, want = failed, model.Failed
				}
				ob, err := observeFacts(t, conditions, pod)
				if err != nil || ob.State != want || ob.ReasonCode != model.ReasonInfraFailure {
					t.Fatalf("platform termination was attributed to task: %+v, %v", ob, err)
				}
			}
		})
	}
	for _, tc := range []struct {
		name      string
		condition *corev1.PodCondition
		message   string
	}{
		{"generic eviction", nil, ""},
		{"own limit prose is not reliable attribution", nil, "Usage of EmptyDir volume exceeds the limit"},
		{"platform prose is not a condition", nil, "TerminationByKubelet node pressure"},
		{"false marker", &corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionFalse, Reason: corev1.PodReasonTerminationByKubelet}, ""},
		{"unknown marker status", &corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionUnknown, Reason: corev1.PodReasonTerminationByKubelet}, ""},
		{"unsupported disruption reason", &corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "UnverifiedController"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: tc.message}}
			if tc.condition != nil {
				pod.Status.Conditions = []corev1.PodCondition{*tc.condition}
			}
			ob, err := observeFacts(t, failed, pod)
			if err != nil || ob.State != model.Failed || ob.ReasonCode != model.ReasonUnknown || strings.Contains(ob.Reason, tc.message) && tc.message != "" {
				t.Fatalf("unattributed eviction invented a cause: %+v, %v", ob, err)
			}
		})
	}
	t.Run("live disruption marker can be withdrawn", func(t *testing.T) {
		pod := corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning,
			Conditions:        []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: "EvictionByEvictionAPI"}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
		ob, err := observeFacts(t, nil, pod)
		if err != nil || ob.State != model.Running || ob.ReasonCode != "" {
			t.Fatalf("marker alone triggered cleanup: %+v %v", ob, err)
		}
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		ob, err = observeFacts(t, nil, pod)
		if err != nil || ob.State != model.Cancelling || ob.ReasonCode != model.ReasonInfraFailure {
			t.Fatalf("actual platform deletion was not classified: %+v %v", ob, err)
		}
	})
	t.Run("OOM and platform disruption have ambiguous ordering", func(t *testing.T) {
		pod := corev1.Pod{Status: terminatedAgent(137, "OOMKilled", "")}
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: corev1.PodReasonTerminationByKubelet}}
		ob, err := observeFacts(t, failed, pod)
		if err != nil || ob.State != model.Failed || ob.ReasonCode != model.ReasonUnknown {
			t.Fatalf("competing specific facts invented an ordering: %+v %v", ob, err)
		}
	})
}

func TestObserveRequiresAgentExecutionEvidenceForRunning(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pods  []corev1.Pod
		state model.State
	}{
		{"active Job with no observed Pod", nil, model.Starting},
		{"active Job with Pending Pod", []corev1.Pod{{Status: waitingAgent("ContainerCreating", "")}}, model.Starting},
		{"Running Pod phase without agent process evidence", []corev1.Pod{{Status: corev1.PodStatus{Phase: corev1.PodRunning}}}, model.Starting},
		{"current agent process running", []corev1.Pod{{Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}}, model.Running},
		{"zero exit awaiting Job condition has already started", []corev1.Pod{{Status: terminatedAgent(0, "Completed", "")}}, model.Running},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The zero-exit fixture is nonfailed: wait for the Job's completion
			// condition rather than asserting a new terminal outcome from a Pod.
			if tc.name == "zero exit awaiting Job condition has already started" {
				tc.pods[0].Status.Phase = corev1.PodSucceeded
			}
			ob, err := observeFacts(t, nil, tc.pods...)
			if err != nil || ob.State != tc.state || ob.ReasonCode != "" {
				t.Fatalf("unexpected start evidence: %+v %v", ob, err)
			}
		})
	}
}
