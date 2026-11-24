package kubernetes

import (
	"context"
	"testing"
	"time"

	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestObserveTerminalTimestampIsAuthoritativeAcrossReconnect(t *testing.T) {
	finished := metav1.NewTime(time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	later := metav1.NewTime(finished.Add(time.Minute))
	for _, tc := range []struct {
		name       string
		conditions []batchv1.JobCondition
		completion *metav1.Time
		want       time.Time
	}{
		{"completion time", []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: later}}, &finished, finished.Time},
		{"success condition fallback", []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: finished}}, nil, finished.Time},
		{"failure condition", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: finished}}, nil, finished.Time},
		{"deadline failure", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded", LastTransitionTime: finished}}, nil, finished.Time},
		{"earliest terminal transition", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: later}, {Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: finished}}, nil, finished.Time},
		{"failure target is not termination", []batchv1.JobCondition{{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, LastTransitionTime: finished}}, nil, time.Time{}},
		{"false condition is not termination", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionFalse, LastTransitionTime: finished}}, nil, time.Time{}},
		{"missing terminal timestamp", []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}, nil, time.Time{}},
		{"unstarted completion field is not termination", nil, &finished, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, client := newTestBackend(t)
			job, _ := b.Render(fixtureRun())
			job.Status = batchv1.JobStatus{Conditions: tc.conditions, CompletionTime: tc.completion}
			if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			ob, err := b.Observe(context.Background(), "run-1")
			if err != nil || !ob.FinishedAt.Equal(tc.want) {
				t.Fatalf("terminal clock lost: %+v %v; want %v", ob, err, tc.want)
			}
		})
	}
}

func TestObserveEvictionDistinguishesStorageLimitsFromPlatformRejection(t *testing.T) {
	failed := []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	running := corev1.ContainerStatus{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
	terminated := terminatedAgent(137, "Error", "untrusted").ContainerStatuses[0]
	zeroRunning, zeroTerminated := *running.DeepCopy(), *terminated.DeepCopy()
	stamp := metav1.NewTime(time.Now().UTC().Add(-time.Minute))
	running.State.Running.StartedAt = stamp
	terminated.State.Terminated.StartedAt = stamp
	waiting := waitingAgent("ContainerCreating", "untrusted").ContainerStatuses[0]
	trueTarget := corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: corev1.PodReasonTerminationByKubelet}
	falseTarget := trueTarget
	falseTarget.Status = corev1.ConditionFalse
	unknownTarget := trueTarget
	unknownTarget.Status = corev1.ConditionUnknown
	futureTarget := trueTarget
	futureTarget.Reason = "FuturePlatformDisruption"
	for _, tc := range []struct {
		name       string
		statuses   []corev1.ContainerStatus
		conditions []corev1.PodCondition
		code       model.ReasonCode
	}{
		{"own storage limit while running", []corev1.ContainerStatus{running}, nil, model.ReasonEvicted},
		{"own storage limit after termination", []corev1.ContainerStatus{terminated}, nil, model.ReasonEvicted},
		{"running state without start timestamp", []corev1.ContainerStatus{zeroRunning}, nil, model.ReasonInfraFailure},
		{"synthesized termination without start timestamp", []corev1.ContainerStatus{zeroTerminated}, nil, model.ReasonInfraFailure},
		{"false disruption marker", []corev1.ContainerStatus{terminated}, []corev1.PodCondition{falseTarget}, model.ReasonEvicted},
		{"unknown disruption marker", []corev1.ContainerStatus{terminated}, []corev1.PodCondition{unknownTarget}, model.ReasonEvicted},
		{"node pressure after start", []corev1.ContainerStatus{terminated}, []corev1.PodCondition{trueTarget}, model.ReasonInfraFailure},
		{"future disruption reason", []corev1.ContainerStatus{terminated}, []corev1.PodCondition{futureTarget}, model.ReasonInfraFailure},
		{"node admission rejection", nil, nil, model.ReasonInfraFailure},
		{"waiting agent never started", []corev1.ContainerStatus{waiting}, nil, model.ReasonInfraFailure},
		{"another container started", []corev1.ContainerStatus{{Name: "other", State: terminated.State}}, nil, model.ReasonInfraFailure},
		{"stale start is insufficient", []corev1.ContainerStatus{{Name: "agent", State: waiting.State, LastTerminationState: terminated.State}}, nil, model.ReasonInfraFailure},
		{"competing OOM evidence", terminatedAgent(137, "OOMKilled", "").ContainerStatuses, nil, model.ReasonUnknown},
		{"conflicting agent statuses", []corev1.ContainerStatus{running, waiting}, nil, model.ReasonUnknown},
		{"conflicting current states", []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: running.State.Running, Waiting: waiting.State.Waiting}}}, nil, model.ReasonUnknown},
		{"conflicting disruption statuses", []corev1.ContainerStatus{terminated}, []corev1.PodCondition{trueTarget, falseTarget}, model.ReasonUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "untrusted OOMKilled", ContainerStatuses: tc.statuses, Conditions: tc.conditions}}
			for _, terminal := range []bool{false, true} {
				var conditions []batchv1.JobCondition
				want := model.Cancelling
				if terminal {
					conditions, want = failed, model.Failed
				}
				ob, err := observeFacts(t, conditions, pod)
				if err != nil || ob.State != want || ob.ReasonCode != tc.code {
					t.Fatalf("unexpected eviction attribution: %+v %v; want %s/%s", ob, err, want, tc.code)
				}
			}
		})
	}
}
