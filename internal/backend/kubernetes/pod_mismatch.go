package kubernetes

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

// podSpecMismatchError contains only a Kubernetes schema path. In particular,
// map keys, object names and field values must never enter a persisted reason
// or an error returned by the log/result paths.
type podSpecMismatchError struct {
	fieldPath string
}

func (e *podSpecMismatchError) Error() string {
	return errPodSpecChanged.Error() + ": " + e.fieldPath
}

func (e *podSpecMismatchError) Unwrap() error { return errPodSpecChanged }

func podMismatchAt(fieldPath string) error {
	return &podSpecMismatchError{fieldPath: fieldPath}
}

// podSpecChangedReason deliberately discards any surrounding error text. Only
// the typed, redacted diagnostic is safe to retain in run/event history.
func podSpecChangedReason(err error) string {
	var mismatch *podSpecMismatchError
	if errors.As(err, &mismatch) {
		return mismatch.Error()
	}
	return errPodSpecChanged.Error()
}

// podExecutionMismatch checks the same complete execution contract as
// samePodExecution, and reports the first differing schema field without its
// value. Normalization and RuntimeClass admission permissions are unchanged.
func podExecutionMismatch(pod *corev1.Pod, template *corev1.PodTemplateSpec, runtimeClass *nodev1.RuntimeClass) error {
	if !sameGrace(pod.Spec.TerminationGracePeriodSeconds, template.Spec.TerminationGracePeriodSeconds) {
		return podMismatchAt("spec.terminationGracePeriodSeconds")
	}
	for key, value := range template.Labels {
		if pod.Labels[key] != value {
			return podMismatchAt("metadata.labels")
		}
	}
	for key, value := range template.Annotations {
		if pod.Annotations[key] != value {
			return podMismatchAt("metadata.annotations")
		}
	}
	for key, value := range pod.Annotations {
		if strings.HasPrefix(key, "sandbox.amazonaws.com/") {
			if expected, ok := template.Annotations[key]; !ok || value != expected {
				return podMismatchAt("metadata.annotations")
			}
		}
	}
	p, q := normalizedPodSpec(&pod.Spec), normalizedPodSpec(&template.Spec)
	// NodeName is assigned by the scheduler after admission. The template's
	// runtime and scheduling restrictions remain part of the comparison.
	p.NodeName = q.NodeName
	allowed := standardPodTolerations()
	if runtimeClass != nil {
		if q.RuntimeClassName == nil || runtimeClass.Name != *q.RuntimeClassName {
			return podMismatchAt("spec.runtimeClassName")
		}
		if runtimeClass.Overhead != nil {
			q.Overhead = runtimeClass.Overhead.PodFixed.DeepCopy()
		}
		if runtimeClass.Scheduling != nil {
			if q.NodeSelector == nil {
				q.NodeSelector = map[string]string{}
			}
			for key, value := range runtimeClass.Scheduling.NodeSelector {
				if existing, ok := q.NodeSelector[key]; ok && existing != value {
					return podMismatchAt("spec.nodeSelector")
				}
				q.NodeSelector[key] = value
			}
			allowed = append(allowed, runtimeClass.Scheduling.Tolerations...)
		}
	}
	if !sameTolerations(p.Tolerations, q.Tolerations, allowed) {
		return podMismatchAt("spec.tolerations")
	}
	p.Tolerations, q.Tolerations = nil, nil
	if fieldPath := firstPodFieldDifference(reflect.ValueOf(p), reflect.ValueOf(q), "spec"); fieldPath != "" {
		return podMismatchAt(fieldPath)
	}
	return nil
}

// firstPodFieldDifference retains Kubernetes semantic equality (including
// equivalent quantities and nil/empty collections). Paths use JSON names from
// compiled API types and numeric slice indexes only. Maps stop at their parent
// field: even a map key may contain a credential or other untrusted content.
func firstPodFieldDifference(actual, expected reflect.Value, fieldPath string) string {
	if !actual.IsValid() || !expected.IsValid() || actual.Type() != expected.Type() {
		return fieldPath
	}
	if apiequality.Semantic.DeepEqual(actual.Interface(), expected.Interface()) {
		return ""
	}
	switch actual.Kind() {
	case reflect.Pointer, reflect.Interface:
		if actual.IsNil() || expected.IsNil() {
			return fieldPath
		}
		return firstPodFieldDifference(actual.Elem(), expected.Elem(), fieldPath)
	case reflect.Struct:
		for i := 0; i < actual.NumField(); i++ {
			field := actual.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			name := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
			if name == "-" || name == "" && !field.Anonymous {
				continue
			}
			nextPath := fieldPath
			if name != "" {
				nextPath += "." + name
			}
			if difference := firstPodFieldDifference(actual.Field(i), expected.Field(i), nextPath); difference != "" {
				return difference
			}
		}
	case reflect.Slice, reflect.Array:
		if actual.Len() != expected.Len() {
			return fieldPath
		}
		for i := 0; i < actual.Len(); i++ {
			nextPath := fmt.Sprintf("%s[%d]", fieldPath, i)
			if difference := firstPodFieldDifference(actual.Index(i), expected.Index(i), nextPath); difference != "" {
				return difference
			}
		}
	}
	return fieldPath
}
