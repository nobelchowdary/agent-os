package config

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"

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
	Namespace          string              `json:"namespace"`
	RuntimeClass       string              `json:"runtime_class"`
	MaxActivePerTenant int                 `json:"max_active_per_tenant"`
	MaxActiveTotal     int                 `json:"max_active_total"`
	Tenants            []Tenant            `json:"tenants"`
	Templates          map[string]Template `json:"templates"`
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
	if !name.MatchString(c.Namespace) || !name.MatchString(c.RuntimeClass) || c.RuntimeClass == "runc" || c.RuntimeClass == "default" {
		return errors.New("explicit namespace and sandbox RuntimeClass required")
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
		if t.DefaultTimeoutSeconds < 1 || t.MaxTimeoutSeconds < t.DefaultTimeoutSeconds || t.MaxTimeoutSeconds > 86400 {
			return fmt.Errorf("template %q has invalid time limits", id)
		}
		if err := validateCommand(t.Command); err != nil {
			return fmt.Errorf("template %q: %w", id, err)
		}
	}
	return nil
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
	return model.Spec{Template: req.Template, Image: t.Image, RuntimeClass: c.RuntimeClass, Command: append([]string(nil), command...), Resources: t.Resources, TimeoutSeconds: timeout}, nil
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
