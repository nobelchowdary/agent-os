package kubernetes

import (
	"context"
	"errors"
	"time"

	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runtimeDeclaration shares a short positive declaration cache between Ready
// and Pod validation. It is not a cache of node execution/isolation evidence.
// Return independent copies so a caller cannot change another validation.
func (b *Backend) runtimeDeclaration(ctx context.Context) (*nodev1.RuntimeClass, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b.runtimeMu.Lock()
		if b.runtimeValue != nil && b.now().Before(b.runtimeUntil) {
			value := b.runtimeValue.DeepCopy()
			b.runtimeMu.Unlock()
			return value, nil
		}
		if pending := b.runtimeChecking; pending != nil {
			b.runtimeMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-pending:
				continue
			}
		}
		b.runtimeChecking = make(chan struct{})
		b.runtimeMu.Unlock()
		var value *nodev1.RuntimeClass
		var err error
		if b.client == nil {
			err = errors.New("kubernetes client is not configured")
		} else {
			value, err = b.client.NodeV1().RuntimeClasses().Get(ctx, b.runtimeClass, metav1.GetOptions{})
		}
		if err == nil && value == nil {
			err = errors.New("empty RuntimeClass declaration")
		}
		if err == nil {
			err = ctx.Err()
		}
		b.runtimeMu.Lock()
		b.runtimeValue, b.runtimeUntil = nil, time.Time{}
		if err == nil && b.cacheableRuntime(value) {
			b.runtimeValue = value.DeepCopy()
			b.runtimeUntil = b.now().Add(3 * time.Second)
		}
		close(b.runtimeChecking)
		b.runtimeChecking = nil
		b.runtimeMu.Unlock()
		if err != nil {
			return nil, err
		}
		// An invalid declaration is deliberately returned fresh, never cached.
		// Existing callers retain their own readiness/mutation error semantics.
		return value.DeepCopy(), nil
	}
}

func (b *Backend) cacheableRuntime(value *nodev1.RuntimeClass) bool {
	if value == nil || value.Name != b.runtimeClass || value.DeletionTimestamp != nil || value.Handler != b.profile.RequiredHandler {
		return false
	}
	if b.profile.Kind == "aws-nitro-isolated" {
		return value.Overhead != nil && validNitroOverhead(value.Overhead.PodFixed) && value.Scheduling != nil && value.Scheduling.NodeSelector[NitroResource] == "true"
	}
	return true
}
