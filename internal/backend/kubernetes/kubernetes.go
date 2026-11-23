// Package kubernetes executes administrator-resolved runs as isolated Jobs.
// Nodes must already provide the configured RuntimeClass; this package cannot
// verify the implementation of that handler or a cluster's network policy.
package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

const (
	RunIDLabel                   = "agentos.dev/run-id"
	ManagedByLabel               = "app.kubernetes.io/managed-by"
	SpecHashAnnotation           = "agentos.dev/spec-sha256"
	PartOfLabel                  = "app.kubernetes.io/part-of"
	DenyAllPolicy                = "agentos-deny-all"
	WorkloadServiceAccount       = "agentos-workload"
	NitroNetworkAnnotation       = "sandbox.amazonaws.com/network-isolated"
	NitroResource                = "eks.amazonaws.com/nitro-sandbox"
	maxLogBytes            int64 = 1 << 20
)

// ErrCancelPending means deletion was requested but Job/Pod disappearance has
// not yet been observed. The caller must retain the run's reservation and retry.
var ErrCancelPending = backend.ErrPending

type Backend struct {
	client        kubernetes.Interface
	namespace     string
	runtimeClass  string
	profile       model.RuntimeProfile
	readyMu       sync.Mutex
	readyUntil    time.Time
	readyChecking chan struct{}
	readyNow      func() time.Time
}

var _ backend.Backend = (*Backend)(nil)

// New constructs a backend without performing network requests. A nil client
// permits offline Render only; every cluster operation then returns an error.
// Ready checks prerequisites. An explicit sandbox RuntimeClass is mandatory: no fallback to
// the node's default container runtime is attempted.
func New(client kubernetes.Interface, namespace, runtimeClass string) (*Backend, error) {
	return NewWithProfile(client, namespace, model.RuntimeProfile{Kind: "gvisor", RuntimeClass: runtimeClass, RequiredHandler: "runsc"})
}

// NewWithProfile accepts a complete administrator-owned runtime contract. It
// checks declarations, not the actual node runtime, CNI, hardware or AWS setup.
// Nitro is isolated mode only, with its trusted-parent and vsock limitations;
// this does not configure or qualify the required AWS infrastructure.
func NewWithProfile(client kubernetes.Interface, namespace string, profile model.RuntimeProfile) (*Backend, error) {
	if client != nil && reflect.ValueOf(client).Kind() == reflect.Ptr && reflect.ValueOf(client).IsNil() {
		client = nil
	}
	if len(validation.IsDNS1123Label(namespace)) != 0 {
		return nil, errors.New("a valid workload namespace is required")
	}
	if len(validation.IsDNS1123Subdomain(profile.RuntimeClass)) != 0 || ordinaryRuntime(profile.RuntimeClass) {
		return nil, errors.New("an explicit sandbox RuntimeClass is required")
	}
	switch profile.Kind {
	case "gvisor":
		if profile.RequiredHandler != "runsc" || profile.RuntimeClass == "isolated-sandbox" || profile.RuntimeClass == "confidential-sandbox" {
			return nil, errors.New("gvisor profile requires a dedicated RuntimeClass and runsc handler")
		}
	case "aws-nitro-isolated":
		if profile.RuntimeClass != "isolated-sandbox" || profile.RequiredHandler != "isolated-sandbox" {
			return nil, errors.New("aws-nitro-isolated profile requires isolated-sandbox RuntimeClass and handler")
		}
	default:
		return nil, errors.New("unsupported execution runtime profile")
	}
	return &Backend{client: client, namespace: namespace, runtimeClass: profile.RuntimeClass, profile: profile}, nil
}

func ordinaryRuntime(name string) bool {
	switch strings.ToLower(name) {
	case "", "default", "runc", "runc-v2", "io.containerd.runc.v2", "crun":
		return true
	default:
		return false
	}
}

func (b *Backend) Ready(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.readyMu.Lock()
		if b.now().Before(b.readyUntil) {
			b.readyMu.Unlock()
			return nil
		}
		if pending := b.readyChecking; pending != nil {
			b.readyMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-pending:
				continue
			}
		}
		b.readyChecking = make(chan struct{})
		b.readyMu.Unlock()
		err := b.checkPrerequisites(ctx)
		if err == nil {
			err = ctx.Err()
		}
		b.readyMu.Lock()
		if err == nil {
			// Declaration checks are shared by queue, submit and activation.
			// Positive entries expire after three seconds; failures never cache.
			b.readyUntil = b.now().Add(3 * time.Second)
		} else {
			b.readyUntil = time.Time{}
		}
		close(b.readyChecking)
		b.readyChecking = nil
		b.readyMu.Unlock()
		return err
	}
}

func (b *Backend) now() time.Time {
	if b.readyNow != nil {
		return b.readyNow()
	}
	return time.Now()
}

func (b *Backend) checkPrerequisites(ctx context.Context) error {
	if b.client == nil {
		return errors.New("kubernetes client is not configured; only offline rendering is available")
	}
	ns, err := b.client.CoreV1().Namespaces().Get(ctx, b.namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("workload namespace: %w", err)
	}
	if ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating {
		return errors.New("workload namespace is terminating")
	}
	rc, err := b.client.NodeV1().RuntimeClasses().Get(ctx, b.runtimeClass, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("sandbox RuntimeClass: %w", err)
	}
	if rc.Handler != b.profile.RequiredHandler {
		return errors.New("RuntimeClass handler differs from the administrator-owned runtime profile")
	}
	if b.profile.Kind == "aws-nitro-isolated" {
		// AWS's documented RuntimeClass reserves a sandbox slot plus 256 MiB
		// guest overhead and selects sandbox-capable nodes. Actual capacity,
		// EnclaveOptions, kernel and matched guest/runtime binaries need QA.
		if rc.Overhead == nil || !validNitroOverhead(rc.Overhead.PodFixed) || rc.Scheduling == nil || rc.Scheduling.NodeSelector[NitroResource] != "true" {
			return errors.New("Nitro RuntimeClass requires sandbox node selection and declared memory/slot overhead")
		}
	}
	policy, err := b.client.NetworkingV1().NetworkPolicies(b.namespace).Get(ctx, DenyAllPolicy, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("default-deny installation prerequisite: %w", err)
	}
	selector := policy.Spec.PodSelector
	if len(selector.MatchLabels) != 1 || selector.MatchLabels[PartOfLabel] != "agent-os" || len(selector.MatchExpressions) != 0 || len(policy.Spec.Ingress) != 0 || len(policy.Spec.Egress) != 0 {
		return errors.New("default-deny installation prerequisite has an invalid selector or allow rules")
	}
	ingress, egress := false, false
	for _, kind := range policy.Spec.PolicyTypes {
		if kind == "Ingress" {
			ingress = true
		}
		if kind == "Egress" {
			egress = true
		}
	}
	if !ingress || !egress {
		return errors.New("default-deny installation prerequisite must isolate ingress and egress")
	}
	// This checks only the declared installation prerequisite. Other additive
	// allow policies and whether the CNI enforces these rules require separate QA.
	return nil
}

func validNitroOverhead(overhead corev1.ResourceList) bool {
	memory := overhead[corev1.ResourceMemory]
	slots := overhead[corev1.ResourceName(NitroResource)]
	return memory.Cmp(resource.MustParse("256Mi")) >= 0 && slots.Cmp(resource.MustParse("1")) == 0
}

func jobName(id string) (string, error) {
	if len(id) > 54 || len(validation.IsDNS1123Label(id)) != 0 {
		return "", errors.New("invalid run ID")
	}
	return "agentos-" + id, nil
}

func ownedLabels(id string) map[string]string {
	return map[string]string{RunIDLabel: id, ManagedByLabel: "agent-os", PartOfLabel: "agent-os"}
}

func specHash(run model.Record) (string, error) {
	// Tenant ownership is part of idempotency, although tenant IDs are not exposed
	// in Kubernetes labels. A reused ID cannot silently adopt another tenant's run.
	encoded, err := json.Marshal(struct {
		Tenant             string
		Spec               model.Spec
		RuntimeAnnotations map[string]string `json:",omitempty"`
	}{run.TenantID, run.Spec, runtimeAnnotations(run.Spec)})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func runtimeAnnotations(spec model.Spec) map[string]string {
	if spec.RuntimeProfile != nil && spec.RuntimeProfile.Kind == "aws-nitro-isolated" {
		return map[string]string{NitroNetworkAnnotation: "true"}
	}
	return nil
}

// Render produces the same validated Job used by Submit, without cluster access.
func (b *Backend) Render(run model.Record) (*batchv1.Job, error) {
	name, err := jobName(run.ID)
	if err != nil {
		return nil, err
	}
	s := run.Spec
	if s.RuntimeClass != b.runtimeClass {
		return nil, errors.New("run RuntimeClass differs from configured sandbox RuntimeClass")
	}
	if s.RuntimeProfile == nil {
		if b.profile.Kind != "gvisor" {
			return nil, errors.New("legacy run without a persisted runtime profile is gVisor-only")
		}
	} else if *s.RuntimeProfile != b.profile {
		return nil, errors.New("persisted runtime profile differs from administrator configuration")
	}
	if strings.TrimSpace(s.Image) == "" || len(s.Command) == 0 || s.Command[0] == "" {
		return nil, errors.New("resolved image and command are required")
	}
	if s.Resources.CPUMilli <= 0 || s.Resources.MemoryMiB <= 0 || s.Resources.DiskMiB < 2 || s.TimeoutSeconds <= 0 {
		return nil, errors.New("positive CPU, memory, disk (at least 2 MiB) and timeout bounds are required")
	}
	if b.profile.Kind == "aws-nitro-isolated" && s.Resources.DiskMiB >= s.Resources.MemoryMiB {
		return nil, errors.New("Nitro memory-backed scratch must be smaller than the workload memory limit")
	}
	hash, err := specHash(run)
	if err != nil {
		return nil, err
	}
	zero, one := int32(0), int32(1)
	uid, no, yes := int64(10001), false, true
	deadline := s.TimeoutSeconds
	runtime := b.runtimeClass
	cpu := *resource.NewMilliQuantity(s.Resources.CPUMilli, resource.DecimalSI)
	mem := resource.MustParse(fmt.Sprintf("%dMi", s.Resources.MemoryMiB))
	disk := resource.MustParse(fmt.Sprintf("%dMi", s.Resources.DiskMiB))
	// Separate bounded scratch volumes share the total ephemeral-storage limit.
	workspace := resource.MustParse(fmt.Sprintf("%dMi", s.Resources.DiskMiB/2))
	tmp := resource.MustParse(fmt.Sprintf("%dMi", s.Resources.DiskMiB-s.Resources.DiskMiB/2))
	limits := corev1.ResourceList{corev1.ResourceCPU: cpu, corev1.ResourceMemory: mem, corev1.ResourceEphemeralStorage: disk}
	fsPolicy := corev1.FSGroupChangeOnRootMismatch
	suspended := true
	job := &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: b.namespace, Labels: ownedLabels(run.ID), Annotations: map[string]string{SpecHashAnnotation: hash}},
		Spec: batchv1.JobSpec{
			BackoffLimit: &zero, ActiveDeadlineSeconds: &deadline, Parallelism: &one, Completions: &one, Suspend: &suspended,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: ownedLabels(run.ID)},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, RuntimeClassName: &runtime,
					ServiceAccountName: WorkloadServiceAccount, AutomountServiceAccountToken: &no,
					SecurityContext: &corev1.PodSecurityContext{RunAsUser: &uid, RunAsGroup: &uid, RunAsNonRoot: &yes, FSGroup: &uid, FSGroupChangePolicy: &fsPolicy, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					Containers: []corev1.Container{{
						Name: "agent", Image: s.Image, Command: append([]string(nil), s.Command...), WorkingDir: "/workspace",
						ImagePullPolicy: corev1.PullIfNotPresent, TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, RunAsNonRoot: &yes, RunAsUser: &uid, RunAsGroup: &uid, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
						Resources:       corev1.ResourceRequirements{Requests: limits.DeepCopy(), Limits: limits.DeepCopy()},
						VolumeMounts:    []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}, {Name: "tmp", MountPath: "/tmp"}},
					}},
					Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &workspace}}}, {Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: &tmp}}}},
				},
			},
		},
	}
	if b.profile.Kind == "aws-nitro-isolated" {
		// AWS isolated mode permits metadata and Pod Identity by default. Remove
		// the NIC explicitly. Logs/control still use vsock; see AWS's documented
		// trusted-parent/vsock caveat before treating this as an isolation claim.
		job.Spec.Template.Annotations = runtimeAnnotations(s)
		job.Spec.Template.Spec.NodeSelector = map[string]string{NitroResource: "true"}
		// Nitro's guest-backed disk usage is invisible to kubelet accounting.
		// Use bounded tmpfs scratch charged to the workload's memory limit.
		for i := range job.Spec.Template.Spec.Volumes {
			job.Spec.Template.Spec.Volumes[i].EmptyDir.Medium = corev1.StorageMediumMemory
		}
	}
	return job, nil
}

// Submit stages a suspended Job and never activates it. The caller must persist
// activation intent before calling Activate; thereafter it must never call
// Submit for this run again, even if the Job disappears.
func (b *Backend) Submit(ctx context.Context, run model.Record) error {
	if b.client == nil {
		return errors.New("kubernetes client is not configured; only offline rendering is available")
	}
	want, err := b.Render(run)
	if err != nil {
		return err
	}
	// Recheck prerequisites through the shared three-second positive cache.
	// This is not atomic with creation and cannot verify CNI behavior.
	if err := b.Ready(ctx); err != nil {
		return fmt.Errorf("execution prerequisites: %w", err)
	}
	created, err := b.client.BatchV1().Jobs(b.namespace).Create(ctx, want, metav1.CreateOptions{})
	if err == nil {
		if !sameExecution(created, want) || created.Spec.Suspend == nil || !*created.Spec.Suspend {
			// A mutating admission component changed the declared boundary. Stop
			// this owned Job rather than accepting an unexpected execution shape.
			_ = b.deleteJob(ctx, created)
			return errors.New("admission changed the execution specification")
		}
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create execution: %w", err)
	}
	got, err := b.ownedJob(ctx, run.ID)
	if err != nil {
		return err
	}
	if got.DeletionTimestamp != nil {
		return ErrCancelPending
	}
	if got.Annotations[SpecHashAnnotation] != want.Annotations[SpecHashAnnotation] || !sameExecution(got, want) {
		return errors.New("existing execution does not match the resolved run specification")
	}
	return nil
}

// Activate never creates a Job. Persisted activation intent lets callers retry
// an uncertain activation without replaying a missing execution after crashes.
func (b *Backend) Activate(ctx context.Context, run model.Record) error {
	want, err := b.Render(run)
	if err != nil {
		return err
	}
	got, err := b.ownedJob(ctx, run.ID)
	if err != nil {
		return err
	}
	if got.DeletionTimestamp != nil {
		return ErrCancelPending
	}
	if got.Annotations[SpecHashAnnotation] != want.Annotations[SpecHashAnnotation] || !sameExecution(got, want) {
		return errors.New("existing execution does not match the resolved run specification")
	}
	for _, condition := range got.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
			return nil // Completed runs must never be restarted by reconciliation.
		}
	}
	if got.Spec.Suspend == nil || !*got.Spec.Suspend {
		return nil
	}
	if got.UID == "" || got.ResourceVersion == "" {
		return errors.New("cannot activate execution without server identity and version")
	}
	if err := b.Ready(ctx); err != nil {
		return fmt.Errorf("activation prerequisites: %w", err)
	}
	patch, err := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": string(got.UID)},
		{"op": "test", "path": "/metadata/resourceVersion", "value": got.ResourceVersion},
		{"op": "test", "path": "/spec/suspend", "value": true},
		{"op": "replace", "path": "/spec/suspend", "value": false},
	})
	if err != nil {
		return err
	}
	// PATCH cannot create a missing Job. UID/version tests prevent an old
	// acknowledgment from activating a replacement or concurrently changed Job.
	activated, err := b.client.BatchV1().Jobs(b.namespace).Patch(ctx, got.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return backend.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("activate execution: %w", err)
	}
	if !sameExecution(activated, want) || activated.Spec.Suspend == nil || *activated.Spec.Suspend {
		return errors.New("activation did not preserve the execution specification")
	}
	return nil
}

// Compare the fields defining execution and isolation, allowing API-server
// generated selectors/labels and harmless Pod scheduling defaults.
func sameExecution(got, want *batchv1.Job) bool {
	g, w := got.Spec, want.Spec
	for key, value := range want.Annotations {
		if got.Annotations[key] != value {
			return false
		}
	}
	if !reflect.DeepEqual(g.BackoffLimit, w.BackoffLimit) || !reflect.DeepEqual(g.ActiveDeadlineSeconds, w.ActiveDeadlineSeconds) || !reflect.DeepEqual(g.Parallelism, w.Parallelism) || !reflect.DeepEqual(g.Completions, w.Completions) || (g.ManualSelector != nil && *g.ManualSelector) || g.TTLSecondsAfterFinished != nil {
		return false
	}
	p, q := g.Template.Spec, w.Template.Spec
	if p.HostNetwork || p.HostPID || p.HostIPC || p.NodeName != "" || len(p.InitContainers) != 0 || len(p.EphemeralContainers) != 0 || len(p.ImagePullSecrets) != 0 || len(p.HostAliases) != 0 {
		return false
	}
	if p.ServiceAccountName != q.ServiceAccountName || p.RestartPolicy != q.RestartPolicy || !reflect.DeepEqual(p.RuntimeClassName, q.RuntimeClassName) || !reflect.DeepEqual(p.AutomountServiceAccountToken, q.AutomountServiceAccountToken) || !apiequality.Semantic.DeepEqual(p.SecurityContext, q.SecurityContext) || !apiequality.Semantic.DeepEqual(p.Containers, q.Containers) || !apiequality.Semantic.DeepEqual(p.Volumes, q.Volumes) {
		return false
	}
	for key, value := range q.NodeSelector {
		if p.NodeSelector[key] != value {
			return false
		}
	}
	for key, value := range w.Template.Annotations {
		if g.Template.Annotations[key] != value {
			return false
		}
	}
	// Do not allow provider-control annotations to be injected or changed by
	// admission. In particular, network-isolated must not become false.
	for key, value := range g.Template.Annotations {
		if strings.HasPrefix(key, "sandbox.amazonaws.com/") {
			if expected, ok := w.Template.Annotations[key]; !ok || expected != value {
				return false
			}
		}
	}
	// RuntimeClass admission normally mutates Pods, not Job templates. If an
	// admission webhook also copies Nitro overhead here, require safe values.
	if w.Template.Annotations[NitroNetworkAnnotation] == "true" && len(p.Overhead) != 0 && !validNitroOverhead(p.Overhead) {
		return false
	}
	for key, value := range want.Labels {
		if got.Labels[key] != value || g.Template.Labels[key] != value {
			return false
		}
	}
	return true
}

func (b *Backend) ownedJob(ctx context.Context, id string) (*batchv1.Job, error) {
	if b.client == nil {
		return nil, errors.New("kubernetes client is not configured; only offline rendering is available")
	}
	name, err := jobName(id)
	if err != nil {
		return nil, err
	}
	job, err := b.client.BatchV1().Jobs(b.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, backend.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get execution: %w", err)
	}
	if job.Name != name || job.Namespace != b.namespace || job.Labels[RunIDLabel] != id || job.Labels[ManagedByLabel] != "agent-os" || job.Annotations[SpecHashAnnotation] == "" {
		return nil, errors.New("refusing an execution not owned by Agent OS")
	}
	return job, nil
}

func (b *Backend) Observe(ctx context.Context, id string) (backend.Observation, error) {
	job, err := b.ownedJob(ctx, id)
	if err != nil {
		return backend.Observation{}, err
	}
	complete, failed, failureTarget, deadline := false, false, false, false
	for _, condition := range job.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Type {
		case batchv1.JobFailed:
			failed = true
			deadline = deadline || condition.Reason == "DeadlineExceeded"
		case batchv1.JobFailureTarget:
			failureTarget = true
			deadline = deadline || condition.Reason == "DeadlineExceeded"
		case batchv1.JobComplete:
			complete = true
		}
	}
	// Decide after inspecting the whole set: contradictory controller facts
	// cannot authorize either success or release of the active reservation.
	if complete && (failed || failureTarget) {
		return backend.Observation{}, errors.New("execution has contradictory Job outcome conditions")
	}
	if complete {
		return backend.Observation{State: model.Succeeded, Reason: "Kubernetes Job completed", ReasonCode: model.ReasonCompleted}, nil
	}
	if deadline {
		state := model.Cancelling
		if failed {
			state = model.Failed
		}
		return backend.Observation{State: state, Reason: "Kubernetes execution deadline exceeded", ReasonCode: model.ReasonDeadlineExceeded}, nil
	}
	if !failed && job.DeletionTimestamp != nil {
		return backend.Observation{State: model.Cancelling, Reason: "execution deletion requested", ReasonCode: model.ReasonInfraFailure}, nil
	}
	pods, err := b.runPods(ctx, id, job.UID)
	if err != nil {
		if failed {
			// The terminal Job remains authoritative if optional Pod evidence is
			// unavailable. Do not guess a specific cause or adopt foreign Pods.
			return backend.Observation{State: model.Failed, Reason: "Kubernetes Job failed", ReasonCode: model.ReasonUnknown}, nil
		}
		return backend.Observation{}, err
	}
	code, imagePullWaiting := podFailureCode(pods)
	if failed {
		if code == "" {
			code = model.ReasonUnknown
		}
		return backend.Observation{State: model.Failed, Reason: "Kubernetes Job failed", ReasonCode: code}, nil
	}
	if failureTarget || code != "" {
		if code == "" {
			code = model.ReasonUnknown
		}
		// Pod status or a failure target is not proof that all workloads have
		// stopped. Retain capacity until Cancel confirms Job/Pod disappearance.
		return backend.Observation{State: model.Cancelling, Reason: "Kubernetes execution requires cleanup", ReasonCode: code}, nil
	}
	if job.Spec.Suspend != nil && *job.Spec.Suspend {
		return backend.Observation{State: model.Starting, Reason: "execution awaits activation"}, nil
	}
	if imagePullWaiting {
		// Registry/network failures can recover. This hint is consumed only
		// after the controller's existing startup wait has already expired.
		return backend.Observation{State: model.Starting, Reason: "execution awaits image pull", ReasonCode: model.ReasonImagePullFailed}, nil
	}
	for _, pod := range pods {
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == "agent" && (container.State.Running != nil || container.State.Terminated != nil) {
				// Job.Active also counts Pending Pods. Only current execution
				// evidence establishes a start. A zero-exit process awaiting the
				// Job terminal condition has started too; do not time it out as
				// an unstarted workload.
				return backend.Observation{State: model.Running}, nil
			}
		}
	}
	return backend.Observation{State: model.Starting}, nil
}

// podFailureCode uses structured status from already ownership-checked Pods,
// never termination messages or logs. Missing or conflicting causes stay
// unknown. Only the declared agent container's current status is considered.
func podFailureCode(pods []corev1.Pod) (model.ReasonCode, bool) {
	var result model.ReasonCode
	imagePullWaiting := false
	for _, pod := range pods {
		var code model.ReasonCode
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name != "agent" {
				continue
			}
			if terminated := container.State.Terminated; terminated != nil && terminated.ExitCode != 0 {
				code = model.ReasonTaskError
				if terminated.Reason == "OOMKilled" {
					code = model.ReasonOOMKilled
				}
			}
			if waiting := container.State.Waiting; waiting != nil {
				switch waiting.Reason {
				case "InvalidImageName":
					code = model.ReasonImagePullFailed
				case "ErrImagePull", "ImagePullBackOff":
					imagePullWaiting = true
				}
			}
		}
		if platformDisrupted(pod) {
			if code == model.ReasonOOMKilled || code == model.ReasonImagePullFailed {
				// Two specific competing facts do not establish their ordering.
				code = model.ReasonUnknown
			} else {
				// A generic nonzero exit after platform termination is not a
				// task-error attribution. This classification never enables retry.
				code = model.ReasonInfraFailure
			}
		} else if pod.Status.Phase == corev1.PodFailed && pod.Status.Reason == "Evicted" && code != model.ReasonOOMKilled {
			// Both platform pressure and own-limit eviction use Evicted.
			// Missing DisruptionTarget is not proof of an own-limit violation;
			// v1.37 exposes that distinction here only in free-form messages.
			code = model.ReasonUnknown
		} else if code == "" && pod.Status.Phase == corev1.PodFailed {
			code = model.ReasonUnknown
		}
		if code != "" {
			if result != "" && result != code {
				result = model.ReasonUnknown
			} else if result == "" {
				result = code
			}
		}
	}
	return result, imagePullWaiting
}

func platformDisrupted(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodFailed && pod.DeletionTimestamp == nil {
		return false // A disruption marker can be withdrawn without a deletion.
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.DisruptionTarget || condition.Status != corev1.ConditionTrue {
			continue
		}
		switch condition.Reason {
		case corev1.PodReasonTerminationByKubelet, corev1.PodReasonPreemptionByScheduler,
			"DeletionByTaintManager", "EvictionByEvictionAPI", "DeletionByPodGC":
			return true
		}
	}
	return false
}

func (b *Backend) deleteJob(ctx context.Context, job *batchv1.Job) error {
	policy, uid := metav1.DeletePropagationForeground, job.UID
	err := b.client.BatchV1().Jobs(b.namespace).Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &policy, Preconditions: &metav1.Preconditions{UID: &uid}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (b *Backend) runPods(ctx context.Context, id string, jobUID types.UID) ([]corev1.Pod, error) {
	name, err := jobName(id)
	if err != nil {
		return nil, err
	}
	pods, err := b.client.CoreV1().Pods(b.namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{RunIDLabel: id}.AsSelector().String()})
	if err != nil {
		return nil, fmt.Errorf("list execution pods: %w", err)
	}
	for _, pod := range pods.Items {
		owned := false
		for _, owner := range pod.OwnerReferences {
			if owner.APIVersion == "batch/v1" && owner.Kind == "Job" && owner.Name == name && owner.Controller != nil && *owner.Controller && (jobUID == "" || owner.UID == jobUID) {
				owned = true
			}
		}
		if !owned || pod.Namespace != b.namespace || pod.Labels[ManagedByLabel] != "agent-os" {
			return nil, errors.New("refusing a run-labeled Pod not owned by this execution")
		}
	}
	return pods.Items, nil
}

func (b *Backend) Cancel(ctx context.Context, id string) error {
	return b.cancelExecution(ctx, id, nil)
}

// Cleanup verifies the actual persisted execution shape before using the same
// UID-fenced deletion protocol as cancellation. It never creates/adopts work.
func (b *Backend) Cleanup(ctx context.Context, run model.Record) error {
	want, err := b.Render(run)
	if err != nil {
		return err
	}
	return b.cancelExecution(ctx, run.ID, want)
}

func (b *Backend) cancelExecution(ctx context.Context, id string, want *batchv1.Job) error {
	if _, err := jobName(id); err != nil {
		return err
	}
	job, err := b.ownedJob(ctx, id)
	if err != nil && !errors.Is(err, backend.ErrNotFound) {
		return err
	}
	var uid types.UID
	if job != nil {
		if want != nil && (job.Annotations[SpecHashAnnotation] != want.Annotations[SpecHashAnnotation] || !sameExecution(job, want)) {
			return errors.New("refusing cleanup of an execution that differs from its durable record")
		}
		uid = job.UID
		// Validate associated objects before performing destructive operations.
		if _, err := b.runPods(ctx, id, uid); err != nil {
			return err
		}
		if err := b.deleteJob(ctx, job); err != nil {
			return fmt.Errorf("delete execution: %w", err)
		}
	}
	remaining, err := b.ownedJob(ctx, id)
	if err != nil && !errors.Is(err, backend.ErrNotFound) {
		return err
	}
	pods, err := b.runPods(ctx, id, uid)
	if err != nil {
		return err
	}
	if remaining != nil {
		return ErrCancelPending
	}
	if len(pods) > 0 {
		// Covers orphaned Pods too. Never force-delete: API disappearance alone
		// cannot prove physical termination on an unreachable Kubernetes node.
		for _, pod := range pods {
			podUID := pod.UID
			err := b.client.CoreV1().Pods(b.namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &podUID}})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete execution pod: %w", err)
			}
		}
		return ErrCancelPending
	}
	return nil
}

func (b *Backend) Logs(ctx context.Context, id string, limit int64) (string, error) {
	if limit <= 0 || limit > maxLogBytes {
		return "", fmt.Errorf("log limit must be between 1 and %d bytes", maxLogBytes)
	}
	job, err := b.ownedJob(ctx, id)
	if err != nil {
		return "", err
	}
	pods, err := b.runPods(ctx, id, job.UID)
	if err != nil {
		return "", err
	}
	if len(pods) == 0 {
		return "", backend.ErrNotFound
	}
	if len(pods) != 1 {
		return "", errors.New("execution has multiple Pods; refusing ambiguous logs")
	}
	stream, err := b.client.CoreV1().Pods(b.namespace).GetLogs(pods[0].Name, &corev1.PodLogOptions{Container: "agent", LimitBytes: &limit}).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("stream execution logs: %w", err)
	}
	defer stream.Close()
	data, err := io.ReadAll(io.LimitReader(stream, limit))
	if err != nil {
		return "", fmt.Errorf("read execution logs: %w", err)
	}
	return string(data), nil
}
