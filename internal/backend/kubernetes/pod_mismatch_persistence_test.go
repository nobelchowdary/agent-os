package kubernetes

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/controller"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodMismatchReasonPersistsThroughCleanupAndRestart(t *testing.T) {
	const untrusted = "diagnostic-redaction-sentinel"
	for _, tc := range []struct {
		name      string
		mutate    func(*corev1.Pod)
		fieldPath string
	}{
		{"image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = untrusted }, "spec.containers[0].image"},
		{"resource map key", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceName(untrusted)] = resource.MustParse("1")
		}, "spec.containers[0].resources.limits"},
		{"mount", func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].MountPath = "/" + untrusted }, "spec.containers[0].volumeMounts[0].mountPath"},
		{"environment", func(p *corev1.Pod) {
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: untrusted, Value: untrusted}}
		}, "spec.containers[0].env"},
		{"provider annotation", func(p *corev1.Pod) {
			p.Annotations = map[string]string{"sandbox.amazonaws.com/" + untrusted: untrusted}
		}, "metadata.annotations"},
		{"first of multiple changes", func(p *corev1.Pod) {
			p.Spec.Containers[0].Image = untrusted
			p.Spec.Containers[0].Env = []corev1.EnvVar{{Name: untrusted, Value: untrusted}}
		}, "spec.containers[0].image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			databasePath := filepath.Join(t.TempDir(), "state", "runs.db")
			db, err := store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			run, _, err := db.Admit(fixtureRun(), "", 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Transition(run.ID, []model.State{model.Queued}, model.Running, "execution observed"); err != nil {
				t.Fatal(err)
			}
			b, client := newTestBackend(t)
			job, err := b.Render(run)
			if err != nil {
				t.Fatal(err)
			}
			job, err = client.BatchV1().Jobs("work").Create(ctx, job, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			pod := ownedPod(job, "untrusted-workload-name")
			tc.mutate(pod)
			if _, err := client.CoreV1().Pods("work").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			c := &controller.Controller{Store: db, Backend: b}
			if err := c.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			wantReason := errPodSpecChanged.Error() + ": " + tc.fieldPath
			assertStoredReason := func(wantState model.State) model.Record {
				t.Helper()
				record, err := db.Get(run.TenantID, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if record.State != wantState || record.ReasonCode != model.ReasonInfraFailure || record.Reason != wantReason {
					t.Fatal("durable mutation reason/state/code did not retain the redacted schema field")
				}
				last := record.Events[len(record.Events)-1]
				if last.State != wantState || last.ReasonCode != model.ReasonInfraFailure || last.Reason != wantReason {
					t.Fatal("lifecycle event did not retain the redacted schema field")
				}
				for _, event := range record.Events {
					if strings.Contains(event.Reason, untrusted) || strings.Contains(event.Reason, pod.Name) {
						t.Fatal("stored lifecycle event exposed untrusted Pod content")
					}
				}
				return record
			}
			assertStoredReason(model.Cancelling)
			if _, _, err := db.Admit(model.Record{ID: "another", TenantID: run.TenantID, Spec: run.Spec}, "", 1, 1); !errors.Is(err, store.ErrQuota) {
				t.Fatal("mutation released quota before cleanup")
			}
			if err := c.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			pending := assertStoredReason(model.Cancelling)
			if pending.Result == nil || pending.Result.Status != "unavailable" || pending.Result.OutputHead != "" || pending.Result.OutputTail != "" {
				t.Fatal("mutation did not quarantine the durable result before cleanup")
			}
			if err := c.Reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			assertStoredReason(model.Failed)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = store.Open(databasePath)
			if err != nil {
				t.Fatal(err)
			}
			assertStoredReason(model.Failed)
		})
	}
}
