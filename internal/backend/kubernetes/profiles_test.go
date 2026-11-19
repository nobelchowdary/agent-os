package kubernetes

import (
	"context"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func nitroProfile() model.RuntimeProfile {
	return model.RuntimeProfile{Kind: "aws-nitro-isolated", RuntimeClass: "isolated-sandbox", RequiredHandler: "isolated-sandbox"}
}

func nitroRun() model.Record {
	r := fixtureRun()
	p := nitroProfile()
	r.Spec.RuntimeClass, r.Spec.RuntimeProfile = p.RuntimeClass, &p
	return r
}

func newNitroBackend(t *testing.T) (*Backend, *fake.Clientset) {
	t.Helper()
	objects := fixtures()
	objects[1] = &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "isolated-sandbox"}, Handler: "isolated-sandbox",
		Overhead: &nodev1.Overhead{PodFixed: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceName(NitroResource): resource.MustParse("1"),
		}},
		Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{NitroResource: "true"}},
	}
	_, client := newTestBackend(t, objects...)
	b, err := NewWithProfile(client, "work", nitroProfile())
	if err != nil {
		t.Fatal(err)
	}
	return b, client
}

func TestExplicitProfilesRejectUnsupportedOrWeakenedCombinations(t *testing.T) {
	for _, p := range []model.RuntimeProfile{
		{},
		{Kind: "gvisor", RuntimeClass: "gvisor"},
		{Kind: "gvisor", RuntimeClass: "gvisor", RequiredHandler: "runc"},
		{Kind: "gvisor", RuntimeClass: "isolated-sandbox", RequiredHandler: "runsc"},
		{Kind: "aws-nitro-isolated", RuntimeClass: "gvisor", RequiredHandler: "isolated-sandbox"},
		{Kind: "aws-nitro-isolated", RuntimeClass: "isolated-sandbox", RequiredHandler: "runsc"},
		{Kind: "aws-nitro-confidential", RuntimeClass: "confidential-sandbox", RequiredHandler: "confidential-sandbox"},
	} {
		if _, err := NewWithProfile(nil, "work", p); err == nil {
			t.Errorf("accepted unsupported profile %+v", p)
		}
	}
	if _, err := New(nil, "work", "isolated-sandbox"); err == nil {
		t.Fatal("legacy constructor inferred Nitro from a class name")
	}
}

func TestNitroReadyRequiresDeclaredHandlerSchedulingAndOverhead(t *testing.T) {
	ctx := context.Background()
	for _, scenario := range []string{"valid", "wrong-handler", "missing-overhead", "small-overhead", "no-slot", "two-slots", "no-scheduling", "wrong-selector"} {
		t.Run(scenario, func(t *testing.T) {
			b, client := newNitroBackend(t)
			rc, err := client.NodeV1().RuntimeClasses().Get(ctx, "isolated-sandbox", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "wrong-handler":
				rc.Handler = "runsc"
			case "missing-overhead":
				rc.Overhead = nil
			case "small-overhead":
				rc.Overhead.PodFixed[corev1.ResourceMemory] = resource.MustParse("128Mi")
			case "no-slot":
				delete(rc.Overhead.PodFixed, corev1.ResourceName(NitroResource))
			case "two-slots":
				rc.Overhead.PodFixed[corev1.ResourceName(NitroResource)] = resource.MustParse("2")
			case "no-scheduling":
				rc.Scheduling = nil
			case "wrong-selector":
				rc.Scheduling.NodeSelector[NitroResource] = "false"
			}
			if _, err := client.NodeV1().RuntimeClasses().Update(ctx, rc, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			err = b.Ready(ctx)
			if (scenario == "valid") != (err == nil) {
				t.Fatalf("Ready=%v", err)
			}
		})
	}
}

func TestNitroRenderBindsProfileAndBoundsMemoryScratch(t *testing.T) {
	b, err := NewWithProfile(nil, "work", nitroProfile())
	if err != nil {
		t.Fatal(err)
	}
	run := nitroRun()
	job, err := b.Render(run)
	if err != nil {
		t.Fatal(err)
	}
	p := job.Spec.Template.Spec
	if *p.RuntimeClassName != "isolated-sandbox" || job.Spec.Template.Annotations[NitroNetworkAnnotation] != "true" || p.NodeSelector[NitroResource] != "true" {
		t.Fatal("Nitro placement/network contract missing", job.Spec.Template)
	}
	if !*job.Spec.Suspend || *p.AutomountServiceAccountToken || *p.Containers[0].SecurityContext.AllowPrivilegeEscalation || !*p.Containers[0].SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("profile weakened the existing workload restrictions")
	}
	var scratch int64
	for _, v := range p.Volumes {
		if v.EmptyDir == nil || v.EmptyDir.Medium != corev1.StorageMediumMemory || v.EmptyDir.SizeLimit == nil {
			t.Fatal("Nitro scratch relies on invisible guest disk accounting")
		}
		scratch += v.EmptyDir.SizeLimit.Value()
	}
	if scratch != run.Spec.Resources.DiskMiB*1024*1024 {
		t.Fatal("scratch bounds changed", scratch)
	}
	run.Spec.RuntimeProfile = nil
	if _, err := b.Render(run); err == nil {
		t.Fatal("Nitro accepted a legacy/unbound run")
	}
	run = nitroRun()
	run.Spec.RuntimeProfile.RequiredHandler = "runsc"
	if _, err := b.Render(run); err == nil {
		t.Fatal("Nitro accepted a changed persisted handler")
	}
	run = nitroRun()
	run.Spec.Resources.DiskMiB = run.Spec.Resources.MemoryMiB
	if _, err := b.Render(run); err == nil {
		t.Fatal("Nitro accepted scratch at its full memory limit")
	}
}

func TestProfileParticipatesInFingerprintAndCannotSwitchPersistedBoundary(t *testing.T) {
	legacy := fixtureRun()
	explicit := fixtureRun()
	explicit.Spec.RuntimeProfile = &model.RuntimeProfile{Kind: "gvisor", RuntimeClass: "gvisor", RequiredHandler: "runsc"}
	a, _ := specHash(legacy)
	// Golden fingerprint from the pre-profile JSON representation. Existing
	// suspended Jobs must remain adoptable after upgrading this alpha.
	if a != "ecfc264b9b963f7463c502568d9641ec28bfffe58e73978b373f28ee5021a737" {
		t.Fatal("legacy execution fingerprint changed", a)
	}
	b, _ := specHash(explicit)
	if a == b {
		t.Fatal("explicit runtime profile not bound into execution fingerprint")
	}
	explicit.Spec.RuntimeProfile.RequiredHandler = "different"
	c, _ := specHash(explicit)
	if b == c {
		t.Fatal("required handler not bound into execution fingerprint")
	}
	gvisor, _ := New(nil, "work", "gvisor")
	if _, err := gvisor.Render(nitroRun()); err == nil {
		t.Fatal("persisted Nitro run switched to gVisor")
	}
}

func TestNitroStagingAndActivationRefusePolicyOrPlacementDrift(t *testing.T) {
	ctx := context.Background()
	for _, scenario := range []string{"valid", "safe-overhead", "network-removed", "network-disabled", "extra-provider-policy", "placement", "disk-scratch", "small-overhead"} {
		t.Run(scenario, func(t *testing.T) {
			b, client := newNitroBackend(t)
			run := nitroRun()
			if err := b.Submit(ctx, run); err != nil {
				t.Fatal(err)
			}
			job, err := client.BatchV1().Jobs("work").Get(ctx, "agentos-run-1", metav1.GetOptions{})
			if err != nil || !*job.Spec.Suspend {
				t.Fatal("Submit did not stage suspended compute", err)
			}
			switch scenario {
			case "safe-overhead", "small-overhead":
				job.Spec.Template.Spec.Overhead = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi"), corev1.ResourceName(NitroResource): resource.MustParse("1")}
				if scenario == "small-overhead" {
					job.Spec.Template.Spec.Overhead[corev1.ResourceMemory] = resource.MustParse("128Mi")
				}
			case "network-removed":
				delete(job.Spec.Template.Annotations, NitroNetworkAnnotation)
			case "network-disabled":
				job.Spec.Template.Annotations[NitroNetworkAnnotation] = "false"
			case "extra-provider-policy":
				job.Spec.Template.Annotations["sandbox.amazonaws.com/policy"] = ""
			case "placement":
				delete(job.Spec.Template.Spec.NodeSelector, NitroResource)
			case "disk-scratch":
				job.Spec.Template.Spec.Volumes[0].EmptyDir.Medium = ""
			}
			if _, err := client.BatchV1().Jobs("work").Update(ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			err = b.Activate(ctx, run)
			valid := scenario == "valid" || scenario == "safe-overhead"
			if valid != (err == nil) {
				t.Fatalf("Activate=%v", err)
			}
			got, _ := client.BatchV1().Jobs("work").Get(ctx, job.Name, metav1.GetOptions{})
			if *got.Spec.Suspend == valid {
				t.Fatal("activation did not preserve staged safety")
			}
			if !valid {
				for _, action := range client.Actions() {
					if action.GetVerb() == "patch" && action.GetResource().Resource == "jobs" {
						t.Fatal("invalid runtime state reached activation")
					}
				}
			}
		})
	}
}

func TestNitroSubmitRejectsChangedAdmissionAnnotationsBeforeActivation(t *testing.T) {
	for _, scenario := range []string{"network", "fingerprint"} {
		t.Run(scenario, func(t *testing.T) {
			b, client := newNitroBackend(t)
			client.PrependReactor("create", "jobs", func(action clienttesting.Action) (bool, runtime.Object, error) {
				job := action.(clienttesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				job.UID, job.ResourceVersion = "admission-uid", "1"
				if scenario == "network" {
					job.Spec.Template.Annotations[NitroNetworkAnnotation] = "false"
				} else {
					job.Annotations[SpecHashAnnotation] = "changed"
				}
				return true, job, nil
			})
			if err := b.Submit(context.Background(), nitroRun()); err == nil {
				t.Fatal("accepted admission mutation of protected runtime fields")
			}
			deleted := false
			for _, action := range client.Actions() {
				if action.GetVerb() == "patch" {
					t.Fatal("staging activated the mutated workload")
				}
				deleted = deleted || action.GetVerb() == "delete"
			}
			if !deleted {
				t.Fatal("did not request cleanup of admission-mutated Job")
			}
		})
	}
}
