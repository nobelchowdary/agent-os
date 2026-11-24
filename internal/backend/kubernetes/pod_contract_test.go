package kubernetes

import (
	"context"
	"errors"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestActualPodMutationTriggersCleanupAndQuarantinesOutput(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mutate    func(*corev1.Pod)
		fieldPath string
	}{
		{"image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = "unapproved" }, "spec.containers[0].image"},
		{"command", func(p *corev1.Pod) { p.Spec.Containers[0].Command = []string{"other"} }, "spec.containers[0].command"},
		{"environment", func(p *corev1.Pod) { p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "INJECTED", Value: "value"}} }, "spec.containers[0].env"},
		{"sidecar", func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "extra", Image: "unapproved"})
		}, "spec.containers"},
		{"init container", func(p *corev1.Pod) { p.Spec.InitContainers = []corev1.Container{{Name: "extra", Image: "unapproved"}} }, "spec.initContainers"},
		{"ephemeral container", func(p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "unapproved"}}}
		}, "spec.ephemeralContainers"},
		{"host network", func(p *corev1.Pod) { p.Spec.HostNetwork = true }, "spec.hostNetwork"},
		{"runtime", func(p *corev1.Pod) { value := "runc"; p.Spec.RuntimeClassName = &value }, "spec.runtimeClassName"},
		{"service account", func(p *corev1.Pod) { p.Spec.ServiceAccountName = "default" }, "spec.serviceAccountName"},
		{"token", func(p *corev1.Pod) { yes := true; p.Spec.AutomountServiceAccountToken = &yes }, "spec.automountServiceAccountToken"},
		{"volume", func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
		}, "spec.volumes"},
		{"mount", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].MountPath = "/other" }, "spec.containers[0].volumeMounts[0].mountPath"},
		{"container privilege", func(p *corev1.Pod) { yes := true; p.Spec.Containers[0].SecurityContext.Privileged = &yes }, "spec.containers[0].securityContext.privileged"},
		{"pod user", func(p *corev1.Pod) { root := int64(0); p.Spec.SecurityContext.RunAsUser = &root }, "spec.securityContext.runAsUser"},
		{"grace", func(p *corev1.Pod) { value := int64(120); p.Spec.TerminationGracePeriodSeconds = &value }, "spec.terminationGracePeriodSeconds"},
		{"resources", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("2Gi")
		}, "spec.containers[0].resources.limits"},
		{"provider policy", func(p *corev1.Pod) { p.Annotations = map[string]string{NitroNetworkAnnotation: "false"} }, "metadata.annotations"},
		{"DNS policy", func(p *corev1.Pod) { p.Spec.DNSPolicy = corev1.DNSDefault }, "spec.dnsPolicy"},
	} {
		for _, terminal := range []string{"active", "complete", "failed"} {
			t.Run(tc.name+"/"+terminal, func(t *testing.T) {
				ctx := context.Background()
				b, client := newTestBackend(t)
				run := fixtureRun()
				run.Spec.TerminationGracePeriodSeconds = 10
				job, _ := b.Render(run)
				job.UID = "job-uid"
				suspended := false
				job.Spec.Suspend = &suspended
				if terminal != "active" {
					kind := batchv1.JobComplete
					if terminal == "failed" {
						kind = batchv1.JobFailed
					}
					job.Status.Conditions = []batchv1.JobCondition{{Type: kind, Status: corev1.ConditionTrue}}
				}
				if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				pod := ownedPod(job, "mutated")
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
				tc.mutate(pod)
				if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				ob, err := b.Observe(ctx, run.ID)
				if err != nil || ob.State != model.Cancelling || ob.ReasonCode != model.ReasonInfraFailure {
					t.Fatalf("unapproved Pod authorized an outcome: %+v %v", ob, err)
				}
				wantReason := errPodSpecChanged.Error() + ": " + tc.fieldPath
				if ob.Reason != wantReason {
					t.Fatalf("Pod mutation reason = %q, want redacted schema field %q", ob.Reason, wantReason)
				}
				result, err := b.CaptureResult(ctx, run.ID)
				if !errors.Is(err, errPodSpecChanged) || result.Status != "unavailable" {
					t.Fatalf("unapproved output was captured: %+v %v", result, err)
				}
				if err.Error() != wantReason {
					t.Fatal("result quarantine error did not retain only the redacted schema field")
				}
				if _, err := b.Logs(ctx, run.ID, 100); !errors.Is(err, errPodSpecChanged) || err.Error() != wantReason {
					t.Fatalf("unapproved output was readable: %v", err)
				}
				if err := b.Cleanup(ctx, run); !errors.Is(err, backend.ErrPending) {
					t.Fatalf("mutated Pod blocked ownership-fenced cleanup: %v", err)
				}
				if err := b.Cleanup(ctx, run); err != nil {
					t.Fatalf("cleanup failed to converge: %v", err)
				}
			})
		}
	}
}

func TestActualPodAcceptsOnlyNormalDefaultsAndDeclaredRuntimeAdditions(t *testing.T) {
	ctx := context.Background()
	b, client := newTestBackend(t, fixtures()...)
	job, _ := b.Render(fixtureRun())
	job.UID = "job-uid"
	suspended := false
	job.Spec.Suspend = &suspended
	if _, err := client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod := ownedPod(job, "defaulted")
	pod.Spec = *normalizedPodSpec(&pod.Spec)
	pod.Spec.NodeName = "scheduler-assigned-node"
	pod.Spec.Tolerations = standardPodTolerations()
	pod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("134217728")
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if ob, err := b.Observe(ctx, "run-1"); err != nil || ob.State != model.Running {
		t.Fatalf("normal API defaults rejected: %+v %v", ob, err)
	}
	runtimeClass, err := client.NodeV1().RuntimeClasses().Get(ctx, "gvisor", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runtimeClass.Overhead = &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")}}
	runtimeClass.Scheduling = &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox-node": "true"}, Tolerations: []corev1.Toleration{{Key: "sandbox-node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}}
	if _, err := client.NodeV1().RuntimeClasses().Update(ctx, runtimeClass, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Overhead = runtimeClass.Overhead.PodFixed.DeepCopy()
	pod.Spec.NodeSelector = map[string]string{"sandbox-node": "true"}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, runtimeClass.Scheduling.Tolerations...)
	if _, err := client.CoreV1().Pods("work").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if ob, err := b.Observe(ctx, "run-1"); err != nil || ob.State != model.Running {
		t.Fatalf("declared RuntimeClass admission rejected: %+v %v", ob, err)
	}
	pod.Spec.NodeSelector["unapproved"] = "true"
	if _, err := client.CoreV1().Pods("work").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if ob, err := b.Observe(ctx, "run-1"); err != nil || ob.State != model.Cancelling || ob.ReasonCode != model.ReasonInfraFailure {
		t.Fatalf("arbitrary scheduling mutation accepted: %+v %v", ob, err)
	}
}

func TestTerminationGraceMatchesDurableRunIncludingLegacyDefault(t *testing.T) {
	b, _ := newTestBackend(t)
	for _, recorded := range []int64{0, 1, 5, 10, 30} {
		run := fixtureRun()
		run.Spec.TerminationGracePeriodSeconds = recorded
		want, err := b.Render(run)
		if err != nil {
			t.Fatal(err)
		}
		if recorded != 0 && (want.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *want.Spec.Template.Spec.TerminationGracePeriodSeconds != recorded) {
			t.Fatal("administrator grace missing from rendered Job")
		}
		for _, actual := range []int64{-1, 0, 1, 5, 10, 30, 120} {
			got := want.DeepCopy()
			got.Spec.Template.Spec.TerminationGracePeriodSeconds = &actual
			if actual == -1 {
				got.Spec.Template.Spec.TerminationGracePeriodSeconds = nil
			}
			valid := actual == recorded && actual > 0 || recorded == 0 && (actual == -1 || actual == 30)
			if sameExecution(got, want) != valid {
				t.Fatalf("recorded grace %d actual %d matching changed", recorded, actual)
			}
		}
	}
}
