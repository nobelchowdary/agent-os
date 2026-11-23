package config

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"time"

	"github.com/nobelchowdary/agent-os/internal/model"
)

type Tenant struct {
	ID       string `json:"id"`
	TokenEnv string `json:"token_env"`
}

type Principal struct {
	TenantID  string
	TokenHash [32]byte
}

type Template struct {
	Image                 string          `json:"image"`
	Command               []string        `json:"command"`
	Resources             model.Resources `json:"resources"`
	DefaultTimeoutSeconds int64           `json:"default_timeout_seconds"`
	MaxTimeoutSeconds     int64           `json:"max_timeout_seconds"`
}

type Config struct {
	Namespace              string                `json:"namespace"`
	RuntimeClass           string                `json:"runtime_class,omitempty"`
	RuntimeProfile         *model.RuntimeProfile `json:"runtime_profile,omitempty"`
	MaxActivePerTenant     int                   `json:"max_active_per_tenant"`
	MaxActiveTotal         int                   `json:"max_active_total"`
	StartupTimeoutSeconds  int64                 `json:"startup_timeout_seconds,omitempty"`
	ResultRetentionSeconds int64                 `json:"result_retention_seconds,omitempty"`
	KubernetesQPS          float32               `json:"kubernetes_qps,omitempty"`
	KubernetesBurst        int                   `json:"kubernetes_burst,omitempty"`
	Tenants                []Tenant              `json:"tenants"`
	Templates              map[string]Template   `json:"templates"`
}

var name = regexp.MustCompile(`^[a-z][a-z0-9-]{0,61}[a-z0-9]$|^[a-z]$`)
var digest = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}$`)
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 128*1024))
	d.DisallowUnknownFields()
	var c Config
	if err = d.Decode(&c); err != nil {
		return c, fmt.Errorf("invalid configuration: %w", err)
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return c, errors.New("configuration must contain one JSON object")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.StartupTimeoutSeconds < 0 || c.StartupTimeoutSeconds > 86400 || c.ResultRetentionSeconds < 0 || c.ResultRetentionSeconds > 30*86400 {
		return errors.New("startup timeout must be 1–86400 seconds and result retention 1–2592000 seconds (zero selects defaults)")
	}
	if math.IsNaN(float64(c.KubernetesQPS)) || math.IsInf(float64(c.KubernetesQPS), 0) || c.KubernetesQPS < 0 || c.KubernetesQPS > 1000 || c.KubernetesBurst < 0 || c.KubernetesBurst > 2000 {
		return errors.New("Kubernetes client QPS must be 1–1000 and burst 1–2000 (zero selects defaults)")
	}
	if c.KubernetesQPS > 0 && c.KubernetesQPS < 1 {
		return errors.New("Kubernetes client QPS must be at least 1")
	}
	if !name.MatchString(c.Namespace) {
		return errors.New("explicit workload namespace required")
	}
	profile, err := c.ResolvedRuntimeProfile()
	if err != nil {
		return err
	}
	if c.MaxActivePerTenant < 1 || c.MaxActiveTotal < c.MaxActivePerTenant || c.MaxActiveTotal > 64 {
		return errors.New("invalid admission limits (maximum total 64 in this alpha)")
	}
	if len(c.Tenants) == 0 || len(c.Tenants) > 100 || len(c.Templates) == 0 || len(c.Templates) > 100 {
		return errors.New("configure 1–100 tenants and templates")
	}
	seen := map[string]bool{}
	for _, t := range c.Tenants {
		if !name.MatchString(t.ID) || !envName.MatchString(t.TokenEnv) || seen[t.ID] {
			return errors.New("invalid or duplicate tenant configuration")
		}
		seen[t.ID] = true
	}
	for id, t := range c.Templates {
		if !name.MatchString(id) || !digest.MatchString(t.Image) {
			return fmt.Errorf("template %q must have a valid name and SHA256-pinned image", id)
		}
		if t.Resources.CPUMilli < 1 || t.Resources.CPUMilli > 64000 || t.Resources.MemoryMiB < 16 || t.Resources.MemoryMiB > 65536 || t.Resources.DiskMiB < 16 || t.Resources.DiskMiB > 65536 {
			return fmt.Errorf("template %q resources out of supported range", id)
		}
		if profile.Kind == "aws-nitro-isolated" && t.Resources.DiskMiB >= t.Resources.MemoryMiB {
			return fmt.Errorf("template %q Nitro memory-backed scratch must be smaller than its memory limit", id)
		}
		if t.DefaultTimeoutSeconds < 1 || t.MaxTimeoutSeconds < t.DefaultTimeoutSeconds || t.MaxTimeoutSeconds > 86400 {
			return fmt.Errorf("template %q has invalid time limits", id)
		}
		if err := validateCommand(t.Command); err != nil {
			return fmt.Errorf("template %q: %w", id, err)
		}
	}
	return nil
}

func (c Config) StartupTimeout() time.Duration {
	seconds := c.StartupTimeoutSeconds
	if seconds == 0 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func (c Config) ResultRetention() time.Duration {
	seconds := c.ResultRetentionSeconds
	if seconds == 0 {
		seconds = 86400
	}
	return time.Duration(seconds) * time.Second
}

func (c Config) ClientLimits() (float32, int) {
	qps, burst := c.KubernetesQPS, c.KubernetesBurst
	if qps == 0 {
		qps = 50
	}
	if burst == 0 {
		burst = 100
	}
	return qps, burst
}

// ResolvedRuntimeProfile validates the complete administrator-owned runtime
// selection. Legacy runtime_class retains the original gVisor/runsc semantics.
// No provider name or ordinary container handler can select another boundary.
func (c Config) ResolvedRuntimeProfile() (model.RuntimeProfile, error) {
	if c.RuntimeProfile != nil && c.RuntimeClass != "" {
		return model.RuntimeProfile{}, errors.New("configure runtime_profile or legacy runtime_class, not both")
	}
	p := model.RuntimeProfile{Kind: "gvisor", RuntimeClass: c.RuntimeClass, RequiredHandler: "runsc"}
	if c.RuntimeProfile != nil {
		p = *c.RuntimeProfile
	}
	if !name.MatchString(p.RuntimeClass) {
		return model.RuntimeProfile{}, errors.New("an explicit sandbox RuntimeClass is required")
	}
	switch p.Kind {
	case "gvisor":
		if p.RequiredHandler != "runsc" || p.RuntimeClass == "runc" || p.RuntimeClass == "runc-v2" || p.RuntimeClass == "default" || p.RuntimeClass == "crun" || p.RuntimeClass == "isolated-sandbox" || p.RuntimeClass == "confidential-sandbox" {
			return model.RuntimeProfile{}, errors.New("gvisor profile requires a dedicated RuntimeClass and runsc handler")
		}
	case "aws-nitro-isolated":
		if p.RuntimeClass != "isolated-sandbox" || p.RequiredHandler != "isolated-sandbox" {
			return model.RuntimeProfile{}, errors.New("aws-nitro-isolated profile requires isolated-sandbox RuntimeClass and handler")
		}
	default:
		return model.RuntimeProfile{}, errors.New("unsupported execution runtime profile")
	}
	return p, nil
}

func (c Config) Principals(lookup func(string) string) ([]Principal, error) {
	var out []Principal
	seen := map[[32]byte]bool{}
	for _, t := range c.Tenants {
		token := lookup(t.TokenEnv)
		if len(token) < 32 || len(token) > 512 {
			return nil, fmt.Errorf("%s must contain a 32–512 byte API token", t.TokenEnv)
		}
		for _, b := range []byte(token) {
			if b <= 32 || b >= 127 {
				return nil, fmt.Errorf("%s must contain printable non-whitespace ASCII", t.TokenEnv)
			}
		}
		h := sha256.Sum256([]byte(token))
		if seen[h] {
			return nil, errors.New("tenants must use different API tokens")
		}
		seen[h] = true
		out = append(out, Principal{TenantID: t.ID, TokenHash: h})
	}
	return out, nil
}

func (c Config) Resolve(req model.CreateRequest) (model.Spec, error) {
	profile, err := c.ResolvedRuntimeProfile()
	if err != nil {
		return model.Spec{}, err
	}
	t, ok := c.Templates[req.Template]
	if !ok {
		return model.Spec{}, errors.New("unknown environment template")
	}
	command := req.Command
	if len(command) == 0 {
		command = t.Command
	}
	if err := validateCommand(command); err != nil {
		return model.Spec{}, err
	}
	timeout := req.TimeoutSeconds
	if timeout == 0 {
		timeout = t.DefaultTimeoutSeconds
	}
	if timeout < 1 || timeout > t.MaxTimeoutSeconds {
		return model.Spec{}, errors.New("timeout exceeds environment policy")
	}
	s := model.Spec{Template: req.Template, Image: t.Image, RuntimeClass: profile.RuntimeClass, Command: append([]string(nil), command...), Resources: t.Resources, TimeoutSeconds: timeout}
	if c.RuntimeProfile != nil {
		// Copy rather than aliasing mutable administrator configuration. Preserve
		// the legacy nil representation so existing idempotency keys still match.
		s.RuntimeProfile = &profile
	}
	return s, nil
}

func validateCommand(command []string) error {
	if len(command) == 0 || len(command) > 64 || command[0] == "" {
		return errors.New("command requires 1–64 arguments and a nonempty executable")
	}
	total := 0
	for _, arg := range command {
		total += len(arg)
		if len(arg) > 8192 {
			return errors.New("command argument too large")
		}
		for _, b := range []byte(arg) {
			if b == 0 {
				return errors.New("command contains NUL")
			}
		}
	}
	if total > 16384 {
		return errors.New("command exceeds 16 KiB")
	}
	return nil
}
