package kubernetes

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodExecutionMismatchReportsRedactedSchemaField(t *testing.T) {
	const untrusted = "diagnostic-redaction-sentinel"
	for _, tc := range []struct {
		name     string
		prepare  func(*corev1.PodTemplateSpec)
		mutate   func(*corev1.Pod)
		wantPath string
	}{
		{name: "image", mutate: func(p *corev1.Pod) { p.Spec.Containers[0].Image = untrusted }, wantPath: "spec.containers[0].image"},
		{name: "command", mutate: func(p *corev1.Pod) { p.Spec.Containers[0].Command[2] = untrusted }, wantPath: "spec.containers[0].command[2]"},
		{name: "environment value", prepare: func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "APP_SETTING", Value: "approved"}}
		}, mutate: func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].Value = untrusted }, wantPath: "spec.containers[0].env[0].value"},
		{name: "environment name", mutate: func(p *corev1.Pod) {
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: untrusted, Value: untrusted}}
		}, wantPath: "spec.containers[0].env"},
		{name: "secret reference", prepare: func(p *corev1.PodTemplateSpec) {
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "APP_SETTING", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "approved"}, Key: "key"}}}}
		}, mutate: func(p *corev1.Pod) {
			p.Spec.Containers[0].Env[0].ValueFrom.SecretKeyRef.Name = untrusted
		}, wantPath: "spec.containers[0].env[0].valueFrom.secretKeyRef.name"},
		{name: "resource quantity", mutate: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("2Gi")
		}, wantPath: "spec.containers[0].resources.limits"},
		{name: "resource name", mutate: func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceName(untrusted)] = resource.MustParse("1")
		}, wantPath: "spec.containers[0].resources.limits"},
		{name: "mount", mutate: func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts[0].MountPath = "/" + untrusted
		}, wantPath: "spec.containers[0].volumeMounts[0].mountPath"},
		{name: "volume source", mutate: func(p *corev1.Pod) {
			p.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/" + untrusted}}
		}, wantPath: "spec.volumes[0].hostPath"},
		{name: "container privilege", mutate: func(p *corev1.Pod) {
			yes := true
			p.Spec.Containers[0].SecurityContext.Privileged = &yes
		}, wantPath: "spec.containers[0].securityContext.privileged"},
		{name: "pod user", mutate: func(p *corev1.Pod) {
			root := int64(0)
			p.Spec.SecurityContext.RunAsUser = &root
		}, wantPath: "spec.securityContext.runAsUser"},
		{name: "provider annotation", mutate: func(p *corev1.Pod) {
			p.Annotations["sandbox.amazonaws.com/"+untrusted] = untrusted
		}, wantPath: "metadata.annotations"},
		{name: "required annotation", prepare: func(p *corev1.PodTemplateSpec) {
			p.Annotations[untrusted] = "approved"
		}, mutate: func(p *corev1.Pod) { p.Annotations[untrusted] = untrusted }, wantPath: "metadata.annotations"},
		{name: "required label", prepare: func(p *corev1.PodTemplateSpec) {
			p.Labels[untrusted] = "approved"
		}, mutate: func(p *corev1.Pod) { p.Labels[untrusted] = untrusted }, wantPath: "metadata.labels"},
		{name: "runtime", mutate: func(p *corev1.Pod) { value := untrusted; p.Spec.RuntimeClassName = &value }, wantPath: "spec.runtimeClassName"},
		{name: "node selector", mutate: func(p *corev1.Pod) { p.Spec.NodeSelector = map[string]string{untrusted: untrusted} }, wantPath: "spec.nodeSelector"},
		{name: "toleration", mutate: func(p *corev1.Pod) {
			p.Spec.Tolerations = []corev1.Toleration{{Key: untrusted, Operator: corev1.TolerationOpExists}}
		}, wantPath: "spec.tolerations"},
		{name: "overhead", mutate: func(p *corev1.Pod) {
			p.Spec.Overhead = corev1.ResourceList{corev1.ResourceName(untrusted): resource.MustParse("1")}
		}, wantPath: "spec.overhead"},
		{name: "grace", mutate: func(p *corev1.Pod) { value := int64(120); p.Spec.TerminationGracePeriodSeconds = &value }, wantPath: "spec.terminationGracePeriodSeconds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := newTestBackend(t)
			job, err := b.Render(fixtureRun())
			if err != nil {
				t.Fatal(err)
			}
			if job.Spec.Template.Annotations == nil {
				job.Spec.Template.Annotations = map[string]string{}
			}
			if tc.prepare != nil {
				tc.prepare(&job.Spec.Template)
			}
			pod := ownedPod(job, "untrusted-pod-name")
			pod.Labels = job.Spec.Template.DeepCopy().Labels
			tc.mutate(pod)
			err = podExecutionMismatch(pod, &job.Spec.Template, nil)
			var mismatch *podSpecMismatchError
			if !errors.Is(err, errPodSpecChanged) || !errors.As(err, &mismatch) {
				t.Fatal("missing typed Pod contract mismatch")
			}
			if mismatch.fieldPath != tc.wantPath {
				t.Fatalf("mismatch path = %q, want %q", mismatch.fieldPath, tc.wantPath)
			}
			if strings.Contains(err.Error(), untrusted) || strings.Contains(err.Error(), pod.Name) {
				t.Fatal("Pod mismatch diagnostic exposed untrusted field content")
			}
			if samePodExecution(pod, &job.Spec.Template, nil) {
				t.Fatal("diagnostic changed the existing Pod acceptance decision")
			}
		})
	}
}

func TestPodSpecChangedReasonDiscardsSurroundingErrors(t *testing.T) {
	want := errPodSpecChanged.Error() + ": spec.containers[0].image"
	err := fmt.Errorf("untrusted surrounding message: %w", podMismatchAt("spec.containers[0].image"))
	if got := podSpecChangedReason(err); got != want {
		t.Fatal("reason did not retain only the redacted schema field")
	}
	if got := podSpecChangedReason(fmt.Errorf("untrusted surrounding message: %w", errPodSpecChanged)); got != errPodSpecChanged.Error() {
		t.Fatal("untyped mismatch included surrounding error text")
	}
}

func TestPodExecutionMismatchPreservesAdmissionPermissions(t *testing.T) {
	b, _ := newTestBackend(t)
	job, err := b.Render(fixtureRun())
	if err != nil {
		t.Fatal(err)
	}
	pod := ownedPod(job, "defaulted")
	pod.Spec = *normalizedPodSpec(&pod.Spec)
	pod.Spec.NodeName = "scheduler-assigned-node"
	pod.Spec.Tolerations = standardPodTolerations()
	pod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("134217728")
	pod.Spec.Containers[0].Env = []corev1.EnvVar{}
	if err := podExecutionMismatch(pod, &job.Spec.Template, nil); err != nil {
		t.Fatalf("normal API defaults/semantic equality rejected: %v", err)
	}
	runtimeClass := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gvisor"},
		Handler:    "runsc",
		Overhead:   &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")}},
		Scheduling: &nodev1.Scheduling{
			NodeSelector: map[string]string{"sandbox-node": "true"},
			Tolerations:  []corev1.Toleration{{Key: "sandbox-node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
		},
	}
	pod.Spec.Overhead = runtimeClass.Overhead.PodFixed.DeepCopy()
	pod.Spec.NodeSelector = map[string]string{"sandbox-node": "true"}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, runtimeClass.Scheduling.Tolerations...)
	if err := podExecutionMismatch(pod, &job.Spec.Template, runtimeClass); err != nil {
		t.Fatalf("declared RuntimeClass admission rejected: %v", err)
	}
	if !samePodExecution(pod, &job.Spec.Template, runtimeClass) {
		t.Fatal("diagnostic changed the existing Pod acceptance decision")
	}
	for _, tc := range []struct {
		name     string
		mutate   func(*nodev1.RuntimeClass, *corev1.Pod, *corev1.PodTemplateSpec)
		wantPath string
	}{
		{name: "name", mutate: func(r *nodev1.RuntimeClass, _ *corev1.Pod, _ *corev1.PodTemplateSpec) { r.Name = "other" }, wantPath: "spec.runtimeClassName"},
		{name: "selector conflict", mutate: func(_ *nodev1.RuntimeClass, _ *corev1.Pod, template *corev1.PodTemplateSpec) {
			template.Spec.NodeSelector = map[string]string{"sandbox-node": "approved"}
		}, wantPath: "spec.nodeSelector"},
		{name: "undeclared overhead", mutate: func(_ *nodev1.RuntimeClass, p *corev1.Pod, _ *corev1.PodTemplateSpec) {
			p.Spec.Overhead[corev1.ResourceMemory] = resource.MustParse("128Mi")
		}, wantPath: "spec.overhead"},
		{name: "undeclared selector", mutate: func(_ *nodev1.RuntimeClass, p *corev1.Pod, _ *corev1.PodTemplateSpec) {
			p.Spec.NodeSelector["other"] = "true"
		}, wantPath: "spec.nodeSelector"},
		{name: "undeclared toleration", mutate: func(_ *nodev1.RuntimeClass, p *corev1.Pod, _ *corev1.PodTemplateSpec) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "other", Operator: corev1.TolerationOpExists})
		}, wantPath: "spec.tolerations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate, declaration := pod.DeepCopy(), runtimeClass.DeepCopy()
			template := job.Spec.Template.DeepCopy()
			tc.mutate(declaration, candidate, template)
			err := podExecutionMismatch(candidate, template, declaration)
			var mismatch *podSpecMismatchError
			if !errors.As(err, &mismatch) || mismatch.fieldPath != tc.wantPath {
				t.Fatalf("undeclared RuntimeClass mutation diagnostic = %v, want %s", err, tc.wantPath)
			}
		})
	}
}
