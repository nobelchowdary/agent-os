package kubernetes

import (
	"context"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPersistedDeadlineScopeSelectsOneClockAndRejectsMutation(t *testing.T) {
	b, client := newTestBackend(t)
	for _, scope := range []model.DeadlineScope{"", model.DeadlineJob, model.DeadlinePod} {
		run := fixtureRun()
		run.Spec.DeadlineScope = scope
		job, err := b.Render(run)
		if err != nil {
			t.Fatal(err)
		}
		jobDeadline, podDeadline := job.Spec.ActiveDeadlineSeconds, job.Spec.Template.Spec.ActiveDeadlineSeconds
		if scope == model.DeadlinePod {
			if jobDeadline != nil || podDeadline == nil || *podDeadline != run.Spec.TimeoutSeconds {
				t.Fatal("new Pod clock retained a Job clock or lost its deadline")
			}
		} else if podDeadline != nil || jobDeadline == nil || *jobDeadline != run.Spec.TimeoutSeconds {
			t.Fatal("legacy or explicit Job clock changed")
		}
		changed := job.DeepCopy()
		seconds := run.Spec.TimeoutSeconds + 1
		changed.Spec.Template.Spec.ActiveDeadlineSeconds = &seconds
		if sameExecution(changed, job) {
			t.Fatal("accepted a changed Pod execution clock")
		}
	}
	run := fixtureRun()
	run.Spec.DeadlineScope = "process"
	if _, err := b.Render(run); err == nil {
		t.Fatal("render accepted an unsupported clock")
	}
	if len(client.Actions()) != 0 {
		t.Fatal("offline deadline rendering contacted Kubernetes")
	}
}

func TestPodDeadlineBeforeAndAfterJobTermination(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		b, client := newTestBackend(t)
		run := fixtureRun()
		run.Spec.DeadlineScope = model.DeadlinePod
		job, err := b.Render(run)
		if err != nil {
			t.Fatal(err)
		}
		job.UID = "job-uid"
		if terminal {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
		}
		if _, err := client.BatchV1().Jobs("work").Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		pod := ownedPod(job, "deadline-pod")
		pod.Status = terminatedAgent(137, "Error", "untrusted output")
		pod.Status.Reason = "DeadlineExceeded"
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: corev1.PodReasonTerminationByKubelet}}
		if _, err := client.CoreV1().Pods("work").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		ob, err := b.Observe(context.Background(), run.ID)
		want := model.Cancelling
		if terminal {
			want = model.Failed
		}
		if err != nil || ob.State != want || ob.ReasonCode != model.ReasonDeadlineExceeded {
			t.Fatalf("Pod deadline terminal=%v: %+v %v", terminal, ob, err)
		}
	}
}

func TestPodClockStagingAndActivationFenceDeadlineMutation(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		b, client := newTestBackend(t, fixtures()...)
		run := fixtureRun()
		run.Spec.DeadlineScope = model.DeadlinePod
		ctx := context.Background()
		if err := b.Submit(ctx, run); err != nil {
			t.Fatal(err)
		}
		job, err := client.BatchV1().Jobs("work").Get(ctx, "agentos-"+run.ID, metav1.GetOptions{})
		if err != nil || job.Spec.Suspend == nil || !*job.Spec.Suspend || job.Spec.ActiveDeadlineSeconds != nil || job.Spec.Template.Spec.ActiveDeadlineSeconds == nil {
			t.Fatal("Pod-clock staging lost suspended execution or its persisted clock", job, err)
		}
		if mutate {
			job.Spec.Template.Spec.ActiveDeadlineSeconds = nil
			if _, err := client.BatchV1().Jobs("work").Update(ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		err = b.Activate(ctx, run)
		if mutate && err == nil || !mutate && err != nil {
			t.Fatal("activation did not enforce the persisted Pod clock", mutate, err)
		}
		job, err = client.BatchV1().Jobs("work").Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil || job.Spec.Suspend == nil || *job.Spec.Suspend != mutate {
			t.Fatal("deadline mutation was activated or matching execution stayed suspended", mutate, job, err)
		}
	}
}

func TestPodDeadlineRequiresStructuredConfiguredEvidence(t *testing.T) {
	seconds := int64(30)
	for _, tc := range []struct {
		name     string
		status   corev1.PodStatus
		deadline *int64
		want     model.ReasonCode
	}{
		{"deadline without container status", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "DeadlineExceeded"}, &seconds, model.ReasonDeadlineExceeded},
		{"message alone", corev1.PodStatus{Phase: corev1.PodFailed, Message: "DeadlineExceeded"}, &seconds, model.ReasonUnknown},
		{"missing configured deadline", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "DeadlineExceeded"}, nil, model.ReasonUnknown},
		{"not failed", corev1.PodStatus{Phase: corev1.PodPending, Reason: "DeadlineExceeded"}, &seconds, ""},
		{"competing OOM", func() corev1.PodStatus {
			s := terminatedAgent(137, "OOMKilled", "")
			s.Reason = "DeadlineExceeded"
			return s
		}(), &seconds, model.ReasonUnknown},
		{"competing preemption", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "DeadlineExceeded", Conditions: []corev1.PodCondition{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue, Reason: corev1.PodReasonPreemptionByScheduler}}}, &seconds, model.ReasonUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := podFailureCode([]corev1.Pod{{Spec: corev1.PodSpec{ActiveDeadlineSeconds: tc.deadline}, Status: tc.status}})
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
