package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	resultPartBytes        = 16 * 1024
	captureScanBytes int64 = 16 * 1024 * 1024
)

var _ backend.ResultCapturer = (*Backend)(nil)
var _ backend.JobInventory = (*Backend)(nil)

// ListExecutions includes every managed-label Job, including malformed/unknown
// run IDs. The inventory itself neither adopts nor deletes any resource.
func (b *Backend) ListExecutions(ctx context.Context) ([]backend.Execution, error) {
	if b.client == nil {
		return nil, errors.New("kubernetes client is not configured")
	}
	result := []backend.Execution{}
	next := ""
	for {
		jobs, err := b.client.BatchV1().Jobs(b.namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{ManagedByLabel: "agent-os"}.AsSelector().String(), Limit: 100, Continue: next})
		if err != nil {
			return nil, fmt.Errorf("list managed executions: %w", err)
		}
		for _, job := range jobs.Items {
			entry := backend.Execution{ID: job.Labels[RunIDLabel], SpecHash: job.Annotations[SpecHashAnnotation]}
			name, err := jobName(entry.ID)
			if err != nil || job.Name != name || job.Namespace != b.namespace || job.Labels[ManagedByLabel] != "agent-os" || entry.SpecHash == "" {
				// Empty ID is deliberately unmatched by all durable run records.
				entry.ID = ""
			}
			for _, condition := range job.Status.Conditions {
				if condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) {
					entry.Terminal = true
				}
			}
			result = append(result, entry)
		}
		if jobs.Continue == "" {
			return result, nil
		}
		if jobs.Continue == next {
			return nil, errors.New("managed execution list did not advance")
		}
		next = jobs.Continue
	}
}

// CaptureResult observes one owned Pod's current agent status and combined
// stdout/stderr before cleanup. Scan work and retained memory are bounded.
// Complete means EOF of the currently readable log for a terminated process;
// kubelet log rotation or an earlier platform deletion can still remove data.
func (b *Backend) CaptureResult(ctx context.Context, id string) (model.ExecutionResult, error) {
	result := model.ExecutionResult{Status: "unavailable", CapturedAt: time.Now().UTC()}
	job, err := b.ownedJob(ctx, id)
	if errors.Is(err, backend.ErrNotFound) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	pods, err := b.runPods(ctx, id, job.UID)
	if err != nil {
		return result, err
	}
	if len(pods) == 0 {
		return result, nil
	}
	if len(pods) != 1 {
		return result, errors.New("execution has multiple Pods; refusing ambiguous result")
	}
	terminated := false
	for _, container := range pods[0].Status.ContainerStatuses {
		if container.Name == "agent" && container.State.Terminated != nil {
			exit := container.State.Terminated.ExitCode
			result.ExitCode, terminated = &exit, true
		}
	}
	// Request one extra byte so hitting the scan bound cannot be mislabeled as
	// a complete EOF. Never ask the kubelet to follow a still-running process.
	limit := captureScanBytes + 1
	stream, err := b.client.CoreV1().Pods(b.namespace).GetLogs(pods[0].Name, &corev1.PodLogOptions{Container: "agent", LimitBytes: &limit}).Stream(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return result, nil // Preserve the observed exit when logs are gone.
		}
		return result, fmt.Errorf("capture execution logs: %w", err)
	}
	defer stream.Close()
	head, tail, count, complete := boundedOutput(stream, captureScanBytes)
	result.Status = "captured"
	var clippedHead, clippedTail bool
	result.OutputHead, clippedHead = validOutput(head, false)
	result.OutputTail, clippedTail = validOutput(tail, true)
	result.OutputTruncated = count > 2*resultPartBytes || !complete || clippedHead || clippedTail
	result.OutputComplete = complete && terminated
	return result, nil
}

// boundedOutput keeps the first and last bytes of the scanned stream. For a
// short stream the parts do not overlap. EOF beyond the scan cap is unknown.
func boundedOutput(reader io.Reader, limit int64) (head, tail []byte, count int64, complete bool) {
	buffer := make([]byte, 32*1024)
	for count <= limit {
		size := int64(len(buffer))
		if size > limit+1-count {
			size = limit + 1 - count
		}
		n, err := reader.Read(buffer[:size])
		if n > 0 {
			part := buffer[:n]
			if count+int64(n) > limit {
				part = part[:limit-count]
			}
			if len(head) < resultPartBytes {
				head = append(head, part[:min(resultPartBytes-len(head), len(part))]...)
			}
			tail = append(tail, part...)
			if len(tail) > resultPartBytes {
				tail = append([]byte(nil), tail[len(tail)-resultPartBytes:]...)
			}
			count += int64(n)
		}
		if count > limit {
			break
		}
		if err != nil {
			complete = errors.Is(err, io.EOF)
			break
		}
		if n == 0 {
			break
		} // A broken reader must not spin forever.
	}
	if count <= resultPartBytes {
		tail = nil
	} else if count <= 2*resultPartBytes {
		tail = tail[len(tail)-int(count-resultPartBytes):]
	}
	return head, tail, count, complete
}

func validOutput(data []byte, tail bool) (string, bool) {
	value := strings.ToValidUTF8(string(data), "\uFFFD")
	if len(value) <= resultPartBytes {
		return value, false
	}
	if tail {
		value = value[len(value)-resultPartBytes:]
		for !utf8.ValidString(value) {
			value = value[1:]
		}
	} else {
		value = value[:resultPartBytes]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return value, true
}
