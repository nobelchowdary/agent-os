package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/nobelchowdary/agent-os/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

const observationListLimit = 100
const observationListMaxItems = 256
const observationListMaxPages = 4

// observationView is immutable and local to one controller pass. It holds
// independent read snapshots, not a transaction across Kubernetes resources.
// Its evidence can never authorize create, activation or deletion operations.
type observationView struct {
	backend *Backend
	ids     map[string]bool
	jobs    map[string]*batchv1.Job
	pods    map[string][]corev1.Pod
	podErr  error
}

var _ backend.ObservationBatcher = (*Backend)(nil)

// PrepareObservations lists the bounded active cohort instead of rereading the
// same collections per run. Foreign run-labeled resources remain visible for
// ownership checks; a managed-by filter would incorrectly hide them.
func (b *Backend) PrepareObservations(ctx context.Context, ids []string) (backend.Observer, error) {
	if b.client == nil {
		return nil, errors.New("kubernetes client is not configured")
	}
	if len(ids) == 0 || len(ids) > 64 {
		return nil, errors.New("execution observation cohort must contain 1 to 64 runs")
	}
	v := &observationView{backend: b, ids: map[string]bool{}, jobs: map[string]*batchv1.Job{}, pods: map[string][]corev1.Pod{}}
	values := append([]string(nil), ids...)
	for _, id := range values {
		if _, err := jobName(id); err != nil {
			return nil, err
		}
		if v.ids[id] {
			return nil, errors.New("execution observation cohort has duplicate runs")
		}
		v.ids[id] = true
	}
	sort.Strings(values)
	requirement, err := labels.NewRequirement(RunIDLabel, selection.In, values)
	if err != nil {
		return nil, errors.New("invalid execution observation selector")
	}
	selector := labels.NewSelector().Add(*requirement).String()
	if err := listObservationPages(ctx, func(options metav1.ListOptions) (string, int, error) {
		jobs, err := b.client.BatchV1().Jobs(b.namespace).List(ctx, options)
		if err != nil {
			return "", 0, err
		}
		for i := range jobs.Items {
			job := &jobs.Items[i]
			if _, exists := v.jobs[job.Name]; exists {
				return "", 0, errors.New("duplicate execution in observation list")
			}
			v.jobs[job.Name] = job.DeepCopy()
		}
		return jobs.Continue, len(jobs.Items), nil
	}, selector); err != nil {
		return nil, fmt.Errorf("list execution observation Jobs: %w", err)
	}
	if err := listObservationPages(ctx, func(options metav1.ListOptions) (string, int, error) {
		pods, err := b.client.CoreV1().Pods(b.namespace).List(ctx, options)
		if err != nil {
			return "", 0, err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			id := pod.Labels[RunIDLabel]
			if !v.ids[id] {
				return "", 0, errors.New("unexpected run in observation Pod list")
			}
			v.pods[id] = append(v.pods[id], *pod.DeepCopy())
		}
		return pods.Continue, len(pods.Items), nil
	}, selector); err != nil {
		// Pod inspection is optional for authoritative terminal Job outcomes,
		// but no part of an incomplete list can be used as Pod evidence.
		v.pods = nil
		v.podErr = fmt.Errorf("list execution observation Pods: %w", err)
	}
	return v, nil
}

func listObservationPages(ctx context.Context, list func(metav1.ListOptions) (string, int, error), selector string) error {
	next, count := "", 0
	seen := map[string]bool{}
	for page := 0; page < observationListMaxPages; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		continuation, items, err := list(metav1.ListOptions{LabelSelector: selector, Limit: observationListLimit, Continue: next})
		if err != nil {
			return err // Never return a partial view, including expired snapshots.
		}
		count += items
		if count > observationListMaxItems {
			return errors.New("execution observation list exceeds bounded inventory")
		}
		if continuation == "" {
			return ctx.Err()
		}
		if seen[continuation] {
			return errors.New("execution observation list did not advance")
		}
		seen[continuation], next = true, continuation
	}
	return errors.New("execution observation list exceeds page limit")
}

func (v *observationView) Observe(ctx context.Context, id string) (backend.Observation, error) {
	if err := ctx.Err(); err != nil {
		return backend.Observation{}, err
	}
	if !v.ids[id] {
		return backend.Observation{}, errors.New("run is outside the execution observation cohort")
	}
	name, err := jobName(id)
	if err != nil {
		return backend.Observation{}, err
	}
	job := v.jobs[name]
	if job == nil {
		// An absent or relabeled Job is not disappearance evidence from this
		// list alone. Preserve the direct GET ownership/not-found contract.
		return v.backend.Observe(ctx, id)
	}
	if err := v.backend.validateOwnedJob(id, job); err != nil {
		return backend.Observation{}, err
	}
	return v.backend.observeJob(id, job, func() ([]corev1.Pod, error) {
		if v.podErr != nil {
			return nil, v.podErr
		}
		pods := v.pods[id]
		if err := v.backend.validateRunPods(id, job.UID, pods); err != nil {
			return nil, err
		}
		return v.backend.validateExecutionPods(ctx, job, pods)
	})
}
