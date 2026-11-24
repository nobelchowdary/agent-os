package kubernetes

import (
	"context"
	"errors"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

var errPodSpecChanged = errors.New("execution Pod differs from the approved Job template")

// executionPods validates actual Pod admission, separately from the Job's
// admission check. Deletion uses runPods instead: a changed execution boundary
// must never prevent ownership-fenced cleanup of that workload.
func (b *Backend) executionPods(ctx context.Context, id string, job *batchv1.Job) ([]corev1.Pod, error) {
	pods, err := b.runPods(ctx, id, job.UID)
	if err != nil {
		return nil, err
	}
	return b.validateExecutionPods(ctx, job, pods)
}

func (b *Backend) validateExecutionPods(ctx context.Context, job *batchv1.Job, pods []corev1.Pod) ([]corev1.Pod, error) {
	var err error
	var runtimeClass *nodev1.RuntimeClass
	for i := range pods {
		mismatch := podExecutionMismatch(&pods[i], &job.Spec.Template, nil)
		if mismatch == nil {
			continue
		}
		if !couldBeRuntimeAdmission(&pods[i], &job.Spec.Template) {
			return nil, mismatch
		}
		// RuntimeClass admission modifies only overhead and scheduling. A live
		// declaration may authorize those additions, never execution changes.
		if runtimeClass == nil {
			runtimeClass, err = b.runtimeDeclaration(ctx)
			if err != nil {
				return nil, fmt.Errorf("verify execution RuntimeClass: %w", err)
			}
			if runtimeClass.Handler != b.profile.RequiredHandler {
				return nil, podMismatchAt("runtimeClass.handler")
			}
		}
		if mismatch := podExecutionMismatch(&pods[i], &job.Spec.Template, runtimeClass); mismatch != nil {
			return nil, mismatch
		}
	}
	return pods, nil
}

func couldBeRuntimeAdmission(pod *corev1.Pod, template *corev1.PodTemplateSpec) bool {
	for key, value := range template.Spec.NodeSelector {
		if pod.Spec.NodeSelector[key] != value {
			return false
		}
	}
	candidate := pod.DeepCopy()
	candidate.Spec.Overhead = template.Spec.Overhead.DeepCopy()
	candidate.Spec.NodeSelector = template.Spec.DeepCopy().NodeSelector
	candidate.Spec.Tolerations = template.Spec.DeepCopy().Tolerations
	return samePodExecution(candidate, template, nil)
}

// Compare the complete execution spec. v1.37 API defaults are normalized;
// scheduler binding and exact standard tolerations are permitted. RuntimeClass
// admission's verified additions are the only other accepted spec changes.
func samePodExecution(pod *corev1.Pod, template *corev1.PodTemplateSpec, runtimeClass *nodev1.RuntimeClass) bool {
	return podExecutionMismatch(pod, template, runtimeClass) == nil
}

func normalizedPodSpec(spec *corev1.PodSpec) *corev1.PodSpec {
	result := spec.DeepCopy()
	if result.DeprecatedServiceAccount == "" {
		result.DeprecatedServiceAccount = result.ServiceAccountName
	}
	if result.DNSPolicy == "" {
		result.DNSPolicy = corev1.DNSClusterFirst
	}
	if result.SchedulerName == "" {
		result.SchedulerName = corev1.DefaultSchedulerName
	}
	grace := effectiveGrace(result.TerminationGracePeriodSeconds)
	result.TerminationGracePeriodSeconds = &grace
	if result.EnableServiceLinks == nil {
		yes := true
		result.EnableServiceLinks = &yes
	}
	if result.Priority == nil {
		zero := int32(0)
		result.Priority = &zero
	}
	if result.PreemptionPolicy == nil {
		policy := corev1.PreemptLowerPriority
		result.PreemptionPolicy = &policy
	}
	return result
}

func standardPodTolerations() []corev1.Toleration {
	seconds := int64(300)
	return []corev1.Toleration{
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		// Render always requests nonzero CPU and memory, so workloads are not
		// BestEffort; Kubernetes adds this scheduling toleration for that QoS.
		{Key: corev1.TaintNodeMemoryPressure, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
}

func sameTolerations(got, want, allowed []corev1.Toleration) bool {
	remaining := append([]corev1.Toleration(nil), got...)
	for _, required := range want {
		found := false
		for i, actual := range remaining {
			if apiequality.Semantic.DeepEqual(actual, required) {
				remaining = append(remaining[:i], remaining[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for _, actual := range remaining {
		found := false
		for _, addition := range allowed {
			found = found || apiequality.Semantic.DeepEqual(actual, addition)
		}
		if !found {
			return false
		}
	}
	return true
}
